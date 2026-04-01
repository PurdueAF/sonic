const express = require("express");
const path = require("path");
const dns = require("node:dns").promises;
const { execFile } = require("child_process");
const util = require("util");

const execFileAsync = util.promisify(execFile);

const app = express();
const PORT = process.env.PORT || 3000;

/** e.g. http://127.0.0.1:9901 — optional STRICT_DNS / cluster visibility from Envoy admin */
const ENVOY_ADMIN_URL = (process.env.ENVOY_ADMIN_URL || "").trim();
/** Substring to locate relevant cluster block in /clusters (e.g. interlink or outbound service name) */
const ENVOY_CLUSTER_FILTER = (process.env.ENVOY_CLUSTER_FILTER || "").trim();
/** Pod label selector for Envoy (e.g. app.kubernetes.io/component=envoy). Overrides default pod name when set. */
const ENVOY_K8S_SELECTOR = (process.env.ENVOY_K8S_SELECTOR || "").trim();
/** Exact Deployment-style name prefix (default sonic-interlink-envoy). Set ENVOY_K8S_POD= empty to disable. */
const ENVOY_K8S_POD_ENV = process.env.ENVOY_K8S_POD;
const ENVOY_K8S_POD =
  ENVOY_K8S_POD_ENV === undefined ? "sonic-interlink-envoy" : String(ENVOY_K8S_POD_ENV).trim();
const ENVOY_K8S_POD_DISABLED = ENVOY_K8S_POD_ENV === "";
/** Namespace for Envoy pod (defaults to the monitored service namespace) */
const ENVOY_K8S_NAMESPACE = (process.env.ENVOY_K8S_NAMESPACE || "").trim();
/** Multi-container pods: admin container name */
const ENVOY_K8S_CONTAINER = (process.env.ENVOY_K8S_CONTAINER || "").trim();
const ENVOY_ADMIN_PORT = Math.min(
  Math.max(parseInt(process.env.ENVOY_ADMIN_PORT || "9901", 10) || 9901, 1),
  65535
);

const KUBECTL_EXEC_MAX_BUF = 12 * 1024 * 1024;
/** Keep kubectl exec bounded so overlapping /state requests do not pile up long-lived processes. */
const KUBECTL_EXEC_TIMEOUT_MS = Math.min(
  Math.max(parseInt(process.env.KUBECTL_EXEC_TIMEOUT_MS || "7000", 10) || 7000, 3000),
  20000
);
const ENVOY_POD_CACHE_TTL_MS = 8000;

/** Serialize Envoy admin scraping so we never run multiple kubectl exec trains against the cluster at once. */
let envoySerialChain = Promise.resolve();

async function fetchEnvoySnapshotForState(requestNamespace) {
  const run = async () => {
    try {
      return await fetchEnvoySnapshot(requestNamespace);
    } catch (e) {
      console.error("fetchEnvoySnapshot failed:", e && e.message);
      const ns = ENVOY_K8S_NAMESPACE || requestNamespace;
      return {
        configured: true,
        source: ENVOY_ADMIN_URL ? "http" : "kubectl-exec",
        adminUrl: ENVOY_ADMIN_URL || null,
        adminNamespace: ns,
        adminPod: null,
        adminPort: ENVOY_ADMIN_PORT,
        adminContainer: ENVOY_K8S_CONTAINER || null,
        format: null,
        clusterCount: null,
        raw: null,
        text: "",
        error: e.message || String(e),
        serverInfo: null,
        readyHttpCode: null,
      };
    }
  };
  const task = envoySerialChain.then(run);
  envoySerialChain = task.catch((err) => {
    console.error("Envoy serial:", err && err.message);
  });
  return task;
}

/** Whether to scrape Envoy admin via kubectl exec inside the Envoy pod.
 * Default: off, because many Envoy images are distroless (no curl/wget/sh).
 * To enable, set ENVOY_K8S_EXEC_ENABLED=1 and either ENVOY_K8S_SELECTOR or ENVOY_K8S_POD.
 */
const ENVOY_K8S_EXEC_ENABLED = process.env.ENVOY_K8S_EXEC_ENABLED === "1";

/** Gateway name in the monitored namespace; empty = list all gateways in that namespace */
const EG_GATEWAY_NAME =
  process.env.EG_GATEWAY_NAME === undefined
    ? "sonic-gateway"
    : String(process.env.EG_GATEWAY_NAME).trim();
/** Envoy Gateway controller install namespace; empty = skip control-plane scrape */
const EG_SYSTEM_NAMESPACE = (process.env.EG_SYSTEM_NAMESPACE || "envoy-gateway-system").trim();

function fetchTimeoutMs(ms) {
  if (typeof AbortSignal !== "undefined" && typeof AbortSignal.timeout === "function") {
    return AbortSignal.timeout(ms);
  }
  const c = new AbortController();
  setTimeout(() => c.abort(), ms);
  return c.signal;
}

let envoyPodResolveCache = { key: "", pod: null, at: 0 };

function pickServerInfoSummary(raw) {
  if (!raw || typeof raw !== "object") return null;
  return {
    version: raw.version || "",
    state: raw.state || "",
    hot_restart_version: raw.hot_restart_version || "",
    uptime_current_epoch: raw.uptime_current_epoch || "",
  };
}

function kubectlExecBaseArgs(namespace, pod, container) {
  const a = ["exec", "-n", namespace, pod];
  if (container) a.push("-c", container);
  return a;
}

/** HTTP GET Envoy admin from inside the pod via one kubectl exec. Never throws (returns null on failure). */
async function kubectlExecAdminBody(namespace, pod, container, port, pathAndQuery) {
  const adminPath = pathAndQuery.startsWith("/") ? pathAndQuery : `/${pathAndQuery}`;
  const url = `http://127.0.0.1:${port}${adminPath}`;
  const base = kubectlExecBaseArgs(namespace, pod, container);
  const opts = { maxBuffer: KUBECTL_EXEC_MAX_BUF, timeout: KUBECTL_EXEC_TIMEOUT_MS };
  const safeUrl = url.replace(/'/g, "'\\''");

  const tryOnce = async (execArgs) => {
    try {
      const { stdout } = await execFileAsync("kubectl", [...base, "--", ...execArgs], opts);
      return stdout;
    } catch {
      return null;
    }
  };

  let out = await tryOnce([
    "curl",
    "-sfS",
    "--connect-timeout",
    "1",
    "--max-time",
    "4",
    url,
  ]);
  if (out != null && out !== "") return out;

  out = await tryOnce(["wget", "-qO-", "-T", "4", url]);
  if (out != null && out !== "") return out;

  out = await tryOnce([
    "sh",
    "-c",
    `curl -sfS --connect-timeout 1 --max-time 4 '${safeUrl}' || wget -qO- -T 4 '${safeUrl}'`,
  ]);
  if (out != null && out !== "") return out;

  return null;
}

async function kubectlExecReadyCode(namespace, pod, container, port) {
  const url = `http://127.0.0.1:${port}/ready`;
  const base = kubectlExecBaseArgs(namespace, pod, container);
  const opts = { maxBuffer: 64, timeout: KUBECTL_EXEC_TIMEOUT_MS };
  try {
    const { stdout } = await execFileAsync(
      "kubectl",
      [
        ...base,
        "--",
        "curl",
        "-s",
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        "--connect-timeout",
        "2",
        "--max-time",
        "4",
        url,
      ],
      opts
    );
    const n = parseInt(String(stdout).trim(), 10);
    return Number.isFinite(n) ? n : null;
  } catch {
    return null;
  }
}

async function resolveEnvoyPodForExec(requestNamespace) {
  const ns = ENVOY_K8S_NAMESPACE || requestNamespace;
  const cacheKey = `${ns}\0${ENVOY_K8S_SELECTOR}\0${ENVOY_K8S_POD}\0${ENVOY_K8S_POD_DISABLED}`;
  if (
    envoyPodResolveCache.key === cacheKey &&
    Date.now() - envoyPodResolveCache.at < ENVOY_POD_CACHE_TTL_MS
  ) {
    return envoyPodResolveCache.pod;
  }

  let pod = null;
  if (ENVOY_K8S_SELECTOR) {
    const list = await kubectlJson(["get", "pods", "-n", ns, "-l", ENVOY_K8S_SELECTOR]);
    const items = list.items || [];
    const running = items.find((p) => p.status?.phase === "Running");
    pod = running?.metadata?.name || items[0]?.metadata?.name || null;
  } else if (!ENVOY_K8S_POD_DISABLED && ENVOY_K8S_POD) {
    try {
      const one = await kubectlJson(["get", "pod", ENVOY_K8S_POD, "-n", ns]);
      if (one.kind === "Pod") pod = one.metadata?.name || null;
    } catch (_) {
      const list = await kubectlJson(["get", "pods", "-n", ns]);
      const items = list.items || [];
      const candidates = items.filter(
        (p) =>
          p.metadata?.name === ENVOY_K8S_POD ||
          p.metadata?.name?.startsWith(`${ENVOY_K8S_POD}-`)
      );
      const running = candidates.find((p) => p.status?.phase === "Running");
      pod = running?.metadata?.name || candidates[0]?.metadata?.name || null;
    }
  }

  envoyPodResolveCache = { key: cacheKey, pod, at: Date.now() };
  return pod;
}

function pickClusterStatsFromStats(statsJson, filterSubstr) {
  if (!statsJson || !Array.isArray(statsJson.stats) || !filterSubstr) return null;
  const f = filterSubstr.toLowerCase();
  const byCluster = new Map();
  for (const s of statsJson.stats) {
    const name = s.name || s.key || "";
    const m = /^cluster\.([^\.]+)\.(.+)$/.exec(String(name));
    if (!m) continue;
    const clusterName = m[1];
    const metricKey = m[2];
    if (!clusterName.toLowerCase().includes(f)) continue;
    let entry = byCluster.get(clusterName);
    if (!entry) {
      entry = {
        clusterName,
        rqTotal: 0,
        rq2xx: 0,
        rq4xx: 0,
        rq5xx: 0,
        rqTimeout: 0,
        rqRetry: 0,
        rqPendingActive: 0,
        rqPendingOverflow: 0,
        cxConnectFail: 0,
        cxConnectTimeout: 0,
        cxOverflow: 0,
      };
      byCluster.set(clusterName, entry);
    }
    const v = Number(s.value ?? s.gauge ?? s.counter ?? 0) || 0;
    switch (metricKey) {
      case "upstream_rq_total":
        entry.rqTotal += v;
        break;
      case "upstream_rq_2xx":
        entry.rq2xx += v;
        break;
      case "upstream_rq_4xx":
        entry.rq4xx += v;
        break;
      case "upstream_rq_5xx":
        entry.rq5xx += v;
        break;
      case "upstream_rq_timeout":
        entry.rqTimeout += v;
        break;
      case "upstream_rq_retry":
        entry.rqRetry += v;
        break;
      case "upstream_rq_pending_active":
        entry.rqPendingActive += v;
        break;
      case "upstream_rq_pending_overflow":
        entry.rqPendingOverflow += v;
        break;
      case "upstream_cx_connect_fail":
        entry.cxConnectFail += v;
        break;
      case "upstream_cx_connect_timeout":
        entry.cxConnectTimeout += v;
        break;
      case "upstream_cx_overflow":
        entry.cxOverflow += v;
        break;
      default:
        break;
    }
  }
  if (byCluster.size === 0) return null;
  // If multiple clusters match the filter substring, pick the one with the most total requests.
  let best = null;
  for (const entry of byCluster.values()) {
    if (!best || entry.rqTotal > best.rqTotal) best = entry;
  }
  return best;
}

async function fetchEnvoyHttpSnapshot(adminBase) {
  if (!adminBase) return { configured: false };
  const base = adminBase.replace(/\/$/, "");
  const tryJson = `${base}/clusters?format=json`;
  const tryText = `${base}/clusters`;
  const tryInfo = `${base}/server_info`;

  const [infoRes, readyRes, clustersOutcome, statsRes] = await Promise.all([
    fetch(tryInfo, { signal: fetchTimeoutMs(3500) })
      .then(async (r) => (r.ok ? r.json() : null))
      .catch(() => null),
    fetch(`${base}/ready`, { method: "GET", signal: fetchTimeoutMs(3500) })
      .then((r) => r.status)
      .catch(() => null),
    (async () => {
      try {
        const res = await fetch(tryJson, { signal: fetchTimeoutMs(4000) });
        if (res.ok) {
          const body = await res.json();
          return {
            format: "json",
            clusterCount: Array.isArray(body.cluster_statuses)
              ? body.cluster_statuses.length
              : null,
            raw: body,
            text: "",
            error: null,
          };
        }
      } catch (_) {
        /* fall through */
      }
      try {
        const res = await fetch(tryText, { signal: fetchTimeoutMs(4000) });
        if (!res.ok) {
          return { format: null, clusterCount: null, raw: null, text: "", error: `HTTP ${res.status}` };
        }
        const text = await res.text();
        return { format: "text", clusterCount: null, raw: null, text, error: null };
      } catch (e) {
        return { format: null, clusterCount: null, raw: null, text: "", error: e.message || String(e) };
      }
    })(),
    fetch(`${base}/stats?format=json`, { signal: fetchTimeoutMs(3500) })
      .then(async (r) => (r.ok ? r.json() : null))
      .catch(() => null),
  ]);

  const co = clustersOutcome;
  return {
    configured: true,
    source: "http",
    adminUrl: adminBase,
    adminNamespace: null,
    adminPod: null,
    adminPort: null,
    adminContainer: null,
    format: co.format,
    clusterCount: co.clusterCount,
    raw: co.raw,
    text: co.text,
    error: co.error,
    serverInfo: pickServerInfoSummary(infoRes),
    readyHttpCode: readyRes,
    statsRaw: statsRes || null,
  };
}

async function fetchEnvoyKubectlSnapshot(requestNamespace) {
  const useK8s =
    ENVOY_K8S_EXEC_ENABLED &&
    (Boolean(ENVOY_K8S_SELECTOR) || (!ENVOY_K8S_POD_DISABLED && Boolean(ENVOY_K8S_POD)));
  if (!useK8s) {
    return { configured: false };
  }

  const ns = ENVOY_K8S_NAMESPACE || requestNamespace;
  const pod = await resolveEnvoyPodForExec(requestNamespace);
  if (!pod) {
    return {
      configured: true,
      source: "kubectl-exec",
      adminUrl: null,
      adminNamespace: ns,
      adminPod: null,
      adminPort: ENVOY_ADMIN_PORT,
      adminContainer: ENVOY_K8S_CONTAINER || null,
      format: null,
      clusterCount: null,
      raw: null,
      text: "",
      error: `No Envoy pod in namespace ${ns} (adjust ENVOY_K8S_SELECTOR / ENVOY_K8S_POD / ENVOY_K8S_NAMESPACE).`,
      serverInfo: null,
      readyHttpCode: null,
    };
  }

  const container = ENVOY_K8S_CONTAINER || null;
  const port = ENVOY_ADMIN_PORT;

  let format = null;
  let raw = null;
  let text = "";
  let clusterCount = null;
  let fetchError = null;
  let statsRaw = null;

  // One kubectl exec at a time — easier on the cluster than parallel exec storms.
  const clustersJsonTxt = await kubectlExecAdminBody(ns, pod, container, port, "/clusters?format=json");
  if (clustersJsonTxt) {
    try {
      const body = JSON.parse(clustersJsonTxt);
      if (Array.isArray(body.cluster_statuses)) {
        format = "json";
        raw = body;
        clusterCount = body.cluster_statuses.length;
      }
    } catch {
      raw = null;
    }
  }

  if (!format) {
    const clustersTxt = await kubectlExecAdminBody(ns, pod, container, port, "/clusters");
    if (clustersTxt) {
      text = clustersTxt;
      format = "text";
    }
  }

  if (!format) {
    fetchError =
      "kubectl exec could not read Envoy /clusters (Envoy image may be distroless without curl/wget/sh). Prefer ENVOY_ADMIN_URL from outside the cluster.";
  }

  let serverInfo = null;
  const serverInfoTxt = await kubectlExecAdminBody(ns, pod, container, port, "/server_info");
  if (serverInfoTxt) {
    try {
      serverInfo = pickServerInfoSummary(JSON.parse(serverInfoTxt));
    } catch {
      serverInfo = null;
    }
  }

  const readyHttpCode = await kubectlExecReadyCode(ns, pod, container, port);
  const statsTxt = await kubectlExecAdminBody(ns, pod, container, port, "/stats?format=json");
  if (statsTxt) {
    try {
      statsRaw = JSON.parse(statsTxt);
    } catch {
      statsRaw = null;
    }
  }

  return {
    configured: true,
    source: "kubectl-exec",
    adminUrl: null,
    adminNamespace: ns,
    adminPod: pod,
    adminPort: port,
    adminContainer: container,
    format,
    clusterCount,
    raw,
    text,
    error: fetchError,
    serverInfo,
    readyHttpCode,
    statsRaw,
  };
}

async function fetchEnvoySnapshot(requestNamespace) {
  if (ENVOY_ADMIN_URL) {
    return fetchEnvoyHttpSnapshot(ENVOY_ADMIN_URL);
  }
  const k8s = await fetchEnvoyKubectlSnapshot(requestNamespace);
  if (k8s.configured) return k8s;
  return { configured: false };
}

function envoyTextAnomalies(text, filterSubstr) {
  const out = [];
  if (!text || !filterSubstr) return out;
  const lower = text.toLowerCase();
  const f = filterSubstr.toLowerCase();
  if (!lower.includes(f)) {
    out.push({
      key: `envoy:clusters:missing-filter:${filterSubstr}`,
      severity: "warn",
      endpointIP: "",
      pod: "envoy",
      msg: `Envoy /clusters does not mention "${filterSubstr}" — wrong ENVOY_CLUSTER_FILTER or cluster name.`,
    });
    return out;
  }
  const badHints = [
    "health_flags::failed",
    "failed_active_hc",
    "health_flags::failed_",
    "::degraded",
    "Drain in progress",
  ];
  const idx = lower.indexOf(f);
  const window = text.slice(Math.max(0, idx - 500), Math.min(text.length, idx + 8000));
  for (const h of badHints) {
    if (window.toLowerCase().includes(h.toLowerCase())) {
      out.push({
        key: `envoy:clusters:unhealthy:${filterSubstr}:${h}`,
        severity: "danger",
        endpointIP: "",
        pod: "envoy",
        msg: `Envoy cluster text suggests unhealthy/degraded hosts near "${filterSubstr}" (snippet hint: ${h}). Check STRICT_DNS / upstream health.`,
      });
      break;
    }
  }
  return out;
}

function envoyHostAddress(hostStatus) {
  const a = hostStatus?.address || hostStatus?.host || {};
  if (typeof a === "string") return a;
  const sa = a.socket_address || a.socketAddress;
  if (sa?.address) {
    const p = sa.port_value ?? sa.portValue;
    return p != null ? `${sa.address}:${p}` : String(sa.address);
  }
  return "";
}

function envoyHostUnhealthyReason(hostStatus) {
  const hs = hostStatus?.health_status || hostStatus?.healthStatus || {};
  if (hs.failed_active_health_check === true || hs.failed_active_hc === true) {
    return "failed_active_health_check";
  }
  if (hs.failed_outlier_check === true) return "failed_outlier_check";
  const state = (hs.state || hs.eden_health_failure || "").toString();
  if (/unhealthy|degraded|draining|FAILED/i.test(state)) return state;
  return null;
}

function envoyJsonAnomalies(raw, filterSubstr) {
  const out = [];
  if (!raw || !filterSubstr) return out;
  const statuses = raw.cluster_statuses;
  if (!Array.isArray(statuses)) return out;
  const f = filterSubstr.toLowerCase();
  const clusters = statuses.filter((c) => String(c.name || "").toLowerCase().includes(f));
  if (clusters.length === 0) {
    out.push({
      key: `envoy:clusters:missing-filter:${filterSubstr}`,
      severity: "warn",
      endpointIP: "",
      pod: "envoy",
      msg: `Envoy cluster JSON has no name containing "${filterSubstr}" (ENVOY_CLUSTER_FILTER).`,
    });
    return out;
  }
  for (const cl of clusters) {
    const hosts = cl.host_statuses || cl.hostStatuses || [];
    if (!Array.isArray(hosts) || hosts.length === 0) {
      out.push({
        key: `envoy:cluster:no-hosts:${cl.name}`,
        severity: "warn",
        endpointIP: "",
        pod: "envoy",
        msg: `Envoy cluster "${cl.name}" has no host_statuses (STRICT_DNS may not have resolved yet).`,
      });
      continue;
    }
    let badAddr = "";
    let badReason = "";
    for (const h of hosts) {
      const reason = envoyHostUnhealthyReason(h);
      if (reason) {
        badAddr = envoyHostAddress(h);
        badReason = reason;
        break;
      }
    }
    if (badReason) {
      const severity = /degraded|draining/i.test(badReason) ? "warn" : "danger";
      out.push({
        key: `envoy:cluster:unhealthy:${cl.name}:${badReason}`,
        severity,
        endpointIP: badAddr || "",
        pod: "envoy",
        msg: `Envoy cluster "${cl.name}" upstream ${badAddr || "?"} — ${badReason}.`,
      });
    }
  }
  return out;
}

async function kubectlJson(args) {
  const { stdout } = await execFileAsync("kubectl", [...args, "-o", "json"]);
  return JSON.parse(stdout);
}

async function kubectlJsonOptional(args) {
  try {
    return await kubectlJson(args);
  } catch {
    return null;
  }
}

function summarizeGateway(g) {
  if (!g || typeof g !== "object") return null;
  const msgCap = 180;
  return {
    name: g.metadata?.name || "",
    namespace: g.metadata?.namespace || "",
    className: g.spec?.gatewayClassName || "",
    listeners: (g.spec?.listeners || []).map((l) => ({
      name: l.name,
      port: l.port,
      protocol: l.protocol,
    })),
    conditions: (g.status?.conditions || []).map((c) => ({
      type: c.type,
      status: c.status,
      reason: c.reason || "",
      message: String(c.message || "").slice(0, msgCap),
    })),
    addresses: (g.status?.addresses || []).map((a) => a.value || "").filter(Boolean),
    listenerStatus: (g.status?.listeners || []).map((l) => ({
      name: l.name,
      attachedRoutes: l.attachedRoutes,
      conditions: (l.conditions || []).map((c) => ({
        type: c.type,
        status: c.status,
        reason: c.reason || "",
      })),
    })),
  };
}

function summarizeGrpcRoutes(listJson) {
  const items = listJson?.items || [];
  return items.map((r) => ({
    name: r.metadata?.name || "",
    parentGateways: (r.spec?.parentRefs || []).map((p) => p.name).filter(Boolean),
    backends: (r.spec?.rules || []).flatMap((rule) =>
      (rule.backendRefs || []).map((b) => `${b.name}:${b.port}`)
    ),
    parentConditions: (r.status?.parents || []).map((p) => ({
      refName: p.parentRef?.name || "",
      conditions: (p.conditions || []).map((c) => ({
        type: c.type,
        status: c.status,
        reason: c.reason || "",
      })),
    })),
  }));
}

function summarizeDeploymentsBrief(depListJson) {
  return (depListJson?.items || []).map((d) => ({
    name: d.metadata?.name || "",
    ready: d.status?.readyReplicas ?? 0,
    desired: d.status?.replicas ?? 0,
  }));
}

function countReadyPods(podListJson) {
  const items = podListJson?.items || [];
  let ready = 0;
  for (const p of items) {
    const phase = p?.status?.phase;
    const rc = (p?.status?.conditions || []).find((c) => c.type === "Ready");
    if (phase === "Running" && rc?.status === "True") ready += 1;
  }
  return { ready, total: items.length };
}

async function fetchEnvoyGatewaySnapshot(workNamespace) {
  const out = {
    error: null,
    hint: null,
    gateway: null,
    gateways: null,
    grpcRoutes: [],
    dataplane: [],
    system: null,
  };

  try {
    let primaryGateway = null;
    if (EG_GATEWAY_NAME) {
      const doc = await kubectlJsonOptional(["get", "gateway", EG_GATEWAY_NAME, "-n", workNamespace]);
      if (doc) primaryGateway = summarizeGateway(doc);
      else out.hint = `Gateway "${EG_GATEWAY_NAME}" not found in ${workNamespace}`;
    } else {
      const list = await kubectlJsonOptional(["get", "gateway", "-n", workNamespace]);
      if (list?.items?.length) {
        out.gateways = list.items.map(summarizeGateway).filter(Boolean);
        primaryGateway = out.gateways[0];
      }
    }

    if (primaryGateway) out.gateway = primaryGateway;

    const grpcList = await kubectlJsonOptional(["get", "grpcroute", "-n", workNamespace]);
    if (grpcList?.items?.length) out.grpcRoutes = summarizeGrpcRoutes(grpcList);

    const gwNameForLabel = out.gateway?.name || out.gateways?.[0]?.name || EG_GATEWAY_NAME || "";
    if (gwNameForLabel && workNamespace) {
      const dpBoth = await kubectlJsonOptional([
        "get",
        "deployment",
        "-n",
        workNamespace,
        "-l",
        `gateway.envoyproxy.io/owning-gateway-name=${gwNameForLabel},gateway.envoyproxy.io/owning-gateway-namespace=${workNamespace}`,
      ]);
      let dpItems = dpBoth?.items || [];
      if (dpItems.length === 0) {
        const dpNameOnly = await kubectlJsonOptional([
          "get",
          "deployment",
          "-n",
          workNamespace,
          "-l",
          `gateway.envoyproxy.io/owning-gateway-name=${gwNameForLabel}`,
        ]);
        dpItems = dpNameOnly?.items || [];
      }
      out.dataplane = summarizeDeploymentsBrief({ items: dpItems });
    }

    if (EG_SYSTEM_NAMESPACE) {
      const depList = await kubectlJsonOptional(["get", "deployment", "-n", EG_SYSTEM_NAMESPACE]);
      const deps = summarizeDeploymentsBrief(depList).slice(0, 24);
      let podsReady = null;
      const byLabel = await kubectlJsonOptional([
        "get",
        "pods",
        "-n",
        EG_SYSTEM_NAMESPACE,
        "-l",
        "control-plane=envoy-gateway",
      ]);
      if (byLabel?.items?.length) {
        podsReady = countReadyPods(byLabel);
      } else {
        const allPods = await kubectlJsonOptional(["get", "pods", "-n", EG_SYSTEM_NAMESPACE]);
        if (allPods?.items?.length) podsReady = countReadyPods(allPods);
      }
      out.system = { namespace: EG_SYSTEM_NAMESPACE, deployments: deps, podsReady };
    }

    const hasData =
      out.gateway ||
      (out.gateways && out.gateways.length) ||
      out.grpcRoutes.length ||
      out.dataplane.length ||
      (out.system && (out.system.deployments.length || (out.system.podsReady && out.system.podsReady.total)));

    if (!hasData && !out.hint) {
      out.hint = "No Gateway / GRPCRoute / dataplane in this namespace, and no controller data (check CRDs and EG_SYSTEM_NAMESPACE).";
    }
  } catch (e) {
    out.error = e.message || String(e);
  }
  return out;
}

function envoyGatewayAnomalies(eg) {
  const out = [];
  if (!eg || eg.error) return out;
  const gws = [];
  if (eg.gateway) gws.push(eg.gateway);
  if (Array.isArray(eg.gateways)) gws.push(...eg.gateways);
  for (const g of gws) {
    if (!g) continue;
    const gwLabel = `${g.namespace}/${g.name}`;
    for (const c of g.conditions || []) {
      if ((c.type === "Programmed" || c.type === "Ready") && c.status !== "True") {
        out.push({
          key: `egw:gw:${g.name}:${c.type}:${c.status}`,
          severity: c.type === "Ready" ? "danger" : "warn",
          endpointIP: "",
          pod: "envoy-gateway",
          msg: `Gateway ${gwLabel} ${c.type}=${c.status}${c.reason ? ` (${c.reason})` : ""}${c.message ? ` — ${c.message}` : ""}`,
        });
      }
    }
    for (const ls of g.listenerStatus || []) {
      const progBad = (ls.conditions || []).some(
        (c) => c.type === "Programmed" && c.status !== "True"
      );
      if (progBad) {
        out.push({
          key: `egw:listener:${g.name}:${ls.name}:programmed`,
          severity: "warn",
          endpointIP: "",
          pod: "envoy-gateway",
          msg: `Gateway ${gwLabel} listener "${ls.name}" Programmed is not True.`,
        });
      }
      if (ls.attachedRoutes === 0) {
        const gwProgrammed = (g.conditions || []).find((c) => c.type === "Programmed");
        if (gwProgrammed?.status === "True") {
          out.push({
            key: `egw:listener:${g.name}:${ls.name}:noroutes`,
            severity: "warn",
            endpointIP: "",
            pod: "envoy-gateway",
            msg: `Gateway ${gwLabel} listener "${ls.name}" has attachedRoutes=0.`,
          });
        }
      }
    }
  }
  for (const r of eg.grpcRoutes || []) {
    for (const pc of r.parentConditions || []) {
      for (const c of pc.conditions || []) {
        if (c.type === "Accepted" && c.status !== "True") {
          out.push({
            key: `egw:grpcroute:${r.name}:accepted:${pc.refName || "parent"}`,
            severity: "warn",
            endpointIP: "",
            pod: "envoy-gateway",
            msg: `GRPCRoute "${r.name}" Accepted=${c.status}${c.reason ? ` (${c.reason})` : ""}.`,
          });
        }
      }
    }
  }
  for (const d of eg.dataplane || []) {
    if (d.desired > 0 && d.ready < d.desired) {
      out.push({
        key: `egw:dataplane:${d.name}`,
        severity: "warn",
        endpointIP: "",
        pod: "envoy-gateway",
        msg: `Envoy Gateway dataplane deployment "${d.name}" ready ${d.ready}/${d.desired}.`,
      });
    }
  }
  if (eg.system?.deployments?.length) {
    for (const d of eg.system.deployments) {
      if (d.desired > 0 && d.ready < d.desired) {
        out.push({
          key: `egw:ctrl:${d.name}`,
          severity: "danger",
          endpointIP: "",
          pod: "envoy-gateway-system",
          msg: `Envoy Gateway controller "${d.name}" in ${eg.system.namespace} ready ${d.ready}/${d.desired}.`,
        });
      }
    }
  }
  return out;
}

function statusToEnum(kubePod) {
  const phase = kubePod?.status?.phase;
  const conditions = kubePod?.status?.conditions || [];
  const readyCond = conditions.find((c) => c.type === "Ready");
  const deletionTimestamp = kubePod?.metadata?.deletionTimestamp;
  const containerStatuses = kubePod?.status?.containerStatuses || [];
  const hasCrashLoop = containerStatuses.some(
    (cs) => cs.state?.waiting?.reason === "CrashLoopBackOff"
  );

  if (deletionTimestamp) return "terminating";
  if (hasCrashLoop) return "crashloop";
  if (phase === "Pending") return "pending";
  if (phase === "Running" && readyCond?.status === "True") return "running-ready";
  if (phase === "Running") return "running-notready";
  return "pending";
}

function statusToHistoryChar(statusEnum) {
  switch (statusEnum) {
    case "running-ready":
      return "r";
    case "running-notready":
      return "n";
    case "pending":
      return "p";
    case "terminating":
      return "t";
    case "crashloop":
      return "c";
    default:
      return "n";
  }
}

function buildInitialHistory(statusEnum) {
  const ch = statusToHistoryChar(statusEnum);
  return ch.repeat(60);
}

function parseBoolOrNull(value) {
  if (value === true) return true;
  if (value === false) return false;
  return null;
}

function endpointReceivesTraffic(endpointState) {
  // Derived monitor rule:
  // 1) explicit include: ready=true OR serving=true
  // 2) implicit include: ready/serving unset (null) and not terminating=true
  const ready = endpointState?.ready;
  const serving = endpointState?.serving;
  const terminating = endpointState?.terminating;
  if (ready === true || serving === true) return true;
  if (ready === null && serving === null && terminating !== true) return true;
  return false;
}

function aggregateEndpointStates(states) {
  if (!states || states.length === 0) {
    return {
      present: false,
      readyAny: false,
      servingAny: false,
      terminatingAny: false,
      receivesTrafficAny: false,
    };
  }
  return {
    present: true,
    readyAny: states.some((s) => s.ready === true),
    servingAny: states.some((s) => s.serving === true),
    terminatingAny: states.some((s) => s.terminating === true),
    receivesTrafficAny: states.some((s) => s.receivesTraffic === true),
  };
}

function humanAge(startTime) {
  if (!startTime) return "";
  const start = new Date(startTime);
  if (Number.isNaN(start.getTime())) return "";
  const now = new Date();
  const seconds = Math.max(0, Math.floor((now - start) / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m`;
}

function summarizeProbeSpec(probe) {
  if (!probe) return null;
  if (probe.httpGet) {
    return {
      kind: "httpGet",
      path: probe.httpGet.path || "",
      port: probe.httpGet.port != null ? String(probe.httpGet.port) : "",
      scheme: probe.httpGet.scheme || "HTTP",
    };
  }
  if (probe.tcpSocket) {
    return {
      kind: "tcpSocket",
      port: probe.tcpSocket.port != null ? String(probe.tcpSocket.port) : "",
    };
  }
  if (probe.exec && Array.isArray(probe.exec.command)) {
    return {
      kind: "exec",
      commandPreview: probe.exec.command.slice(0, 4).join(" "),
    };
  }
  if (probe.grpc) {
    return {
      kind: "grpc",
      port: probe.grpc.port != null ? String(probe.grpc.port) : "",
      service: probe.grpc.service || "",
    };
  }
  return { kind: "other" };
}

function containerRuntimeState(cs) {
  if (!cs || !cs.state) return { phase: "unknown", reason: "", message: "" };
  if (cs.state.running) {
    return {
      phase: "running",
      reason: "Running",
      message: cs.state.running.message || "",
      startedAt: cs.state.running.startedAt || "",
    };
  }
  if (cs.state.waiting) {
    return {
      phase: "waiting",
      reason: cs.state.waiting.reason || "Waiting",
      message: cs.state.waiting.message || "",
    };
  }
  if (cs.state.terminated) {
    return {
      phase: "terminated",
      reason: cs.state.terminated.reason || "Terminated",
      message: cs.state.terminated.message || "",
      exitCode: cs.state.terminated.exitCode,
    };
  }
  return { phase: "unknown", reason: "", message: "" };
}

function buildPodDetails(p) {
  const status = p?.status || {};
  const spec = p?.spec || {};
  const phase = status.phase || "Unknown";
  const qosClass = status.qosClass || "";
  const deletionTimestamp = p?.metadata?.deletionTimestamp || null;
  const podIP = status.podIP || null;

  const conditions = (status.conditions || []).map((c) => ({
    type: c.type || "",
    status: c.status || "",
    reason: c.reason || "",
    message: (c.message || "").slice(0, 200),
    lastTransitionTime: c.lastTransitionTime || "",
  }));

  const specContainers = spec.containers || [];
  const statusByName = new Map(
    (status.containerStatuses || []).map((cs) => [cs.name, cs])
  );

  const containers = specContainers.map((sc) => {
    const cs = statusByName.get(sc.name) || null;
    const rt = cs ? containerRuntimeState(cs) : { phase: "unknown", reason: "", message: "" };
    const readinessConfigured = Boolean(sc.readinessProbe);
    const livenessConfigured = Boolean(sc.livenessProbe);
    const startupConfigured = Boolean(sc.startupProbe);

    const readinessSpec = summarizeProbeSpec(sc.readinessProbe);
    const livenessSpec = summarizeProbeSpec(sc.livenessProbe);
    const startupSpec = summarizeProbeSpec(sc.startupProbe);

    let readinessStatus = "unknown";
    if (!readinessConfigured) {
      readinessStatus = "not_configured";
    } else if (!cs) {
      readinessStatus = "unknown";
    } else if (cs.ready === true) {
      readinessStatus = "ready_true";
    } else if (cs.ready === false) {
      readinessStatus = "ready_false";
    }

    let startupStatus = "not_configured";
    if (startupConfigured) {
      if (!cs) startupStatus = "unknown";
      else if (cs.started === true) startupStatus = "complete";
      else if (cs.started === false) startupStatus = "in_progress";
      else startupStatus = "unknown_legacy";
    }

    let livenessStatus = "not_configured";
    if (livenessConfigured) {
      if (rt.phase === "running") livenessStatus = "running";
      else if (rt.phase === "waiting" && cs?.state?.waiting?.reason === "CrashLoopBackOff")
        livenessStatus = "crashloop";
      else if (rt.phase === "waiting") livenessStatus = "waiting";
      else livenessStatus = rt.phase || "unknown";
    }

    return {
      name: sc.name,
      ready: cs?.ready ?? null,
      restartCount: cs?.restartCount ?? 0,
      started: cs?.started ?? null,
      image: sc.image || "",
      runtimePhase: rt.phase,
      runtimeReason: rt.reason,
      runtimeMessage: (rt.message || "").slice(0, 160),
      probes: {
        readiness: { configured: readinessConfigured, spec: readinessSpec, status: readinessStatus },
        liveness: { configured: livenessConfigured, spec: livenessSpec, status: livenessStatus },
        startup: { configured: startupConfigured, spec: startupSpec, status: startupStatus },
      },
    };
  });

  return {
    phase,
    qosClass,
    deletionTimestamp,
    conditions,
    containers,
  };
}

// Simple CORS to allow opening the HTML from file:// or another port.
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET");
  next();
});

// Serve the dashboard HTML from the project root so you can just open
// http://localhost:3000/ in the browser.
app.get("/", (req, res) => {
  const htmlPath = path.join(__dirname, "..", "service-monitor.html");
  res.sendFile(htmlPath);
});

app.get("/state", async (req, res) => {
  const namespace = req.query.namespace || "sonic";
  const serviceName = req.query.service || "sonic-interlink-triton";

  try {
    const svc = await kubectlJson(["get", "service", serviceName, "-n", namespace]);

    const selector = svc?.spec?.selector || {};
    const labelSelector = Object.entries(selector)
      .map(([k, v]) => `${k}=${v}`)
      .join(",");

    const podArgs = ["get", "pods", "-n", namespace];
    if (labelSelector) {
      podArgs.push("-l", labelSelector);
    }

    const [podsJson, endpointSlicesJson] = await Promise.all([
      kubectlJson(podArgs),
      kubectlJson([
        "get",
        "endpointslices",
        "-n",
        namespace,
        "-l",
        `kubernetes.io/service-name=${serviceName}`,
      ]).catch(() => ({ items: [] })),
    ]);

    const endpointByPodName = new Map();
    const endpointByIp = new Map();
    const endpointRecords = [];

    (endpointSlicesJson.items || []).forEach((slice) => {
      (slice.endpoints || []).forEach((ep) => {
        const cond = ep.conditions || {};
        const endpointState = {
          ready: parseBoolOrNull(cond.ready),
          serving: parseBoolOrNull(cond.serving),
          terminating: parseBoolOrNull(cond.terminating),
        };

        const podName = ep?.targetRef?.kind === "Pod" ? ep?.targetRef?.name : null;
        const stateWithTraffic = {
          ...endpointState,
          receivesTraffic: endpointReceivesTraffic(endpointState),
        };
        if (podName) {
          const existing = endpointByPodName.get(podName) || [];
          existing.push(stateWithTraffic);
          endpointByPodName.set(podName, existing);
        }

        (ep.addresses || []).forEach((ip) => {
          const existing = endpointByIp.get(ip) || [];
          existing.push(stateWithTraffic);
          endpointByIp.set(ip, existing);
          endpointRecords.push({
            slice: slice?.metadata?.name || "",
            ip,
            podName,
            targetRefKind: ep?.targetRef?.kind || "",
            ready: stateWithTraffic.ready,
            serving: stateWithTraffic.serving,
            terminating: stateWithTraffic.terminating,
            receivesTraffic: stateWithTraffic.receivesTraffic,
          });
        });
      });
    });

    const pods = (podsJson.items || []).map((p) => {
      const statusEnum = statusToEnum(p);
      const podIP = p?.status?.podIP || null;
      const containerStatuses = p?.status?.containerStatuses || [];
      const restarts = containerStatuses.reduce(
        (sum, cs) => sum + (cs.restartCount || 0),
        0
      );
      const k8s = buildPodDetails(p);

      const podNameStates = endpointByPodName.get(p?.metadata?.name || "") || [];
      const ipStates = podIP ? (endpointByIp.get(podIP) || []) : [];
      const endpointAgg = aggregateEndpointStates([...podNameStates, ...ipStates]);
      const endpointReady = endpointAgg.readyAny;
      const endpointServing = endpointAgg.servingAny;
      const endpointTerminating = endpointAgg.terminatingAny;
      const receivesTraffic = endpointAgg.receivesTrafficAny;

      return {
        id: p?.metadata?.name || "",
        status: statusEnum,
        exposed: endpointAgg.present,
        phase: k8s.phase,
        qosClass: k8s.qosClass || null,
        deletionTimestamp: k8s.deletionTimestamp,
        conditions: k8s.conditions,
        containers: k8s.containers,
        endpointReady,
        endpointServing,
        endpointTerminating,
        receivesTraffic,
        ip: podIP,
        restarts,
        age: humanAge(p?.status?.startTime),
        history: buildInitialHistory(statusEnum),
      };
    });

    const podByName = new Map();
    pods.forEach((p) => {
      podByName.set(p.id, p);
    });

    const anomalies = [];
    endpointRecords.forEach((ep) => {
      if (!ep.receivesTraffic) return;
      const pod = ep.podName ? podByName.get(ep.podName) : null;
      const podUnready = pod ? pod.status !== "running-ready" : true;
      if (podUnready) {
        const condText = `ready=${ep.ready} serving=${ep.serving} terminating=${ep.terminating}`;
        const explicit = ep.ready === true || ep.serving === true;
        anomalies.push({
          key: `${ep.slice}:${ep.ip}:${ep.podName || "unknown"}`,
          severity: "danger",
          endpointIP: ep.ip,
          pod: ep.podName || "unknown",
          msg: pod
            ? explicit
              ? `EndpointSlice explicitly routes to this pod (${condText}) while pod is not ready.`
              : `EndpointSlice has no ready/serving condition set (${condText}), so this address may still be used while pod is not ready.`
            : explicit
              ? `EndpointSlice explicitly routes endpoint (${condText}) but it does not map to a known selected pod.`
              : `EndpointSlice includes endpoint with implicit routing (${condText}) but it does not map to a known selected pod.`,
        });
      } else if (
        ep.terminating === true &&
        ep.receivesTraffic &&
        (ep.ready === true || ep.serving === true)
      ) {
        anomalies.push({
          key: `${ep.slice}:${ep.ip}:${ep.podName || "unknown"}:terminating-traffic`,
          severity: "danger",
          endpointIP: ep.ip,
          pod: ep.podName || "unknown",
          msg: "Endpoint is terminating=true but still has ready/serving=true, so EndpointSlice can keep routing to it.",
        });
      }
    });

    // Service-wide outage signals that are easy to miss pod-by-pod.
    const trafficEligibleEndpoints = endpointRecords.filter((ep) => ep.receivesTraffic);
    if (endpointRecords.length === 0) {
      anomalies.push({
        key: `svc:${serviceName}:no-endpoints`,
        severity: "danger",
        endpointIP: "",
        pod: serviceName,
        msg: "Service has zero EndpointSlice endpoints.",
      });
    } else if (trafficEligibleEndpoints.length === 0) {
      anomalies.push({
        key: `svc:${serviceName}:no-traffic-eligible`,
        severity: "danger",
        endpointIP: "",
        pod: serviceName,
        msg: "Service has endpoints, but none satisfy routing conditions (ready=true/serving=true, or implicit ready/serving unset while not terminating).",
      });
    }

    const clusterIPVal = svc?.spec?.clusterIP || "";
    const isHeadless =
      clusterIPVal === "None" || clusterIPVal === "";
    const allSliceIps = new Set(
      endpointRecords.map((e) => e.ip).filter(Boolean)
    );

    const envoyFilter = ENVOY_CLUSTER_FILTER || serviceName;
    const [envoy, envoyGateway] = await Promise.all([
      fetchEnvoySnapshotForState(namespace),
      fetchEnvoyGatewaySnapshot(namespace),
    ]);

    anomalies.push(...envoyGatewayAnomalies(envoyGateway));

    if (envoy.configured) {
      if (envoy.error) {
        anomalies.push({
          key: "envoy:admin:fetch-fail",
          severity: "warn",
          endpointIP: "",
          pod: "envoy",
          msg: `Envoy admin fetch failed: ${envoy.error}`,
        });
      } else {
        if (envoy.format === "text" && envoy.text) {
          anomalies.push(...envoyTextAnomalies(envoy.text, envoyFilter));
        }
        if (envoy.format === "json" && envoy.raw) {
          anomalies.push(...envoyJsonAnomalies(envoy.raw, envoyFilter));
        }
      }
      if (envoy.serverInfo?.state && envoy.serverInfo.state !== "LIVE") {
        anomalies.push({
          key: `envoy:server:state:${envoy.serverInfo.state}`,
          severity: "warn",
          endpointIP: "",
          pod: "envoy",
          msg: `Envoy process state is ${envoy.serverInfo.state} (expected LIVE).`,
        });
      }
      if (envoy.readyHttpCode != null && envoy.readyHttpCode !== 200) {
        anomalies.push({
          key: "envoy:ready:not-ok",
          severity: "warn",
          endpointIP: "",
          pod: "envoy",
          msg: `Envoy admin /ready returned HTTP ${envoy.readyHttpCode} (expected 200).`,
        });
      }
      if (envoy.statsRaw) {
        const cs = pickClusterStatsFromStats(envoy.statsRaw, envoyFilter);
        if (cs && cs.rqTotal > 0 && (cs.rq5xx > 0 || cs.rqTimeout > 0 || cs.rqPendingOverflow > 0 || cs.cxOverflow > 0)) {
          anomalies.push({
            key: `envoy:cluster:errors:${cs.clusterName}`,
            severity: "warn",
            endpointIP: "",
            pod: "envoy",
            msg: `Envoy cluster "${cs.clusterName}" has elevated errors/timeouts/overflows (total=${cs.rqTotal}, 5xx=${cs.rq5xx}, timeout=${cs.rqTimeout}, overflow=${cs.rqPendingOverflow}).`,
          });
        }
      }
    }

    const proxy = {
      note: "Envoy STRICT_DNS uses DNS; kube Endpoints come from the API. Envoy admin shows active upstreams and health.",
      serviceIsHeadless: isHeadless,
      envoyGateway: {
        error: envoyGateway.error,
        hint: envoyGateway.hint,
        gateway: envoyGateway.gateway,
        gateways: envoyGateway.gateways,
        grpcRoutes: envoyGateway.grpcRoutes,
        dataplane: envoyGateway.dataplane,
        system: envoyGateway.system,
      },
      envoy: envoy.configured
        ? {
            configured: true,
            source: envoy.source || null,
            adminUrl: envoy.adminUrl || null,
            adminNamespace: envoy.adminNamespace || null,
            adminPod: envoy.adminPod || null,
            adminPort: envoy.adminPort != null ? envoy.adminPort : null,
            adminContainer: envoy.adminContainer || null,
            clusterFilter: envoyFilter,
            format: envoy.format,
            clusterCount: envoy.clusterCount,
            textLength: envoy.text ? envoy.text.length : 0,
            error: envoy.error || null,
            serverInfo: envoy.serverInfo || null,
            readyHttpCode: envoy.readyHttpCode,
            statsSummary: envoy.statsRaw ? pickClusterStatsFromStats(envoy.statsRaw, envoyFilter) : null,
          }
        : {
            configured: false,
            hint:
              "Set ENVOY_ADMIN_URL, or rely on in-cluster ENVOY_K8S_POD (default sonic-interlink-envoy) / ENVOY_K8S_SELECTOR, plus ENVOY_CLUSTER_FILTER for your upstream cluster name.",
          },
    };

    res.json({
      service: {
        name: svc?.metadata?.name || serviceName,
        namespace: svc?.metadata?.namespace || namespace,
        clusterIP: svc?.spec?.clusterIP || "",
        ports: svc?.spec?.ports || [],
      },
      pods,
      endpoints: endpointRecords,
      anomalies,
      events: [],
      proxy,
    });
  } catch (err) {
    console.error("Error in /state:", err);
    res.status(500).json({
      error: "Failed to query Kubernetes via kubectl",
      message: err.message,
    });
  }
});

app.listen(PORT, () => {
  console.log(`service-monitor-backend listening on http://localhost:${PORT}`);
  console.log(
    "Example: curl 'http://localhost:%d/state?namespace=sonic&service=sonic-interlink-triton'",
    PORT
  );
  if (ENVOY_ADMIN_URL) {
    console.log(
      `Envoy admin (HTTP): ${ENVOY_ADMIN_URL} (ENVOY_CLUSTER_FILTER=${ENVOY_CLUSTER_FILTER || "<service name>"})`
    );
  } else if (ENVOY_K8S_EXEC_ENABLED && ENVOY_K8S_SELECTOR) {
    console.log(
      `Envoy admin via kubectl exec: selector "${ENVOY_K8S_SELECTOR}" · port ${ENVOY_ADMIN_PORT} · ns ${ENVOY_K8S_NAMESPACE || "<same as /state>"} (ENVOY_CLUSTER_FILTER=${ENVOY_CLUSTER_FILTER || "<service name>"})`
    );
  } else if (ENVOY_K8S_EXEC_ENABLED && !ENVOY_K8S_POD_DISABLED) {
    console.log(
      `Envoy admin via kubectl exec: pod prefix "${ENVOY_K8S_POD}" · port ${ENVOY_ADMIN_PORT} · ns ${ENVOY_K8S_NAMESPACE || "<same as /state>"} (set ENVOY_K8S_POD= empty to disable)`
    );
  } else {
    console.log(
      "Envoy admin scraping: kubectl-exec path is disabled by default (set ENVOY_K8S_EXEC_ENABLED=1 to enable) and ENVOY_ADMIN_URL is not set."
    );
  }
  if (EG_SYSTEM_NAMESPACE) {
    console.log(
      `Envoy Gateway API: gateway "${EG_GATEWAY_NAME || "<list all>"}" · controller ns ${EG_SYSTEM_NAMESPACE}`
    );
  } else {
    console.log("Envoy Gateway API: controller scrape disabled (EG_SYSTEM_NAMESPACE empty)");
  }
});

