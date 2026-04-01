const { spawn } = require("child_process");
const path = require("path");

function log(prefix, msg) {
  process.stdout.write(`[${prefix}] ${msg}\n`);
}

function start() {
  const ns = process.env.SONIC_NAMESPACE || "sonic";
  const svc = process.env.SONIC_ENVOY_SERVICE || "sonic-interlink";
  const localPort = process.env.SONIC_ENVOY_ADMIN_PORT || "9901";

  const kubectlArgs = [
    "port-forward",
    "-n",
    ns,
    `svc/${svc}`,
    `${localPort}:9901`,
  ];

  log("dev", `starting kubectl ${kubectlArgs.join(" ")}`);

  const kubectl = spawn("kubectl", kubectlArgs, {
    stdio: ["ignore", "pipe", "pipe"],
  });

  let server;
  let serverStarted = false;

  kubectl.stdout.on("data", (data) => {
    const text = data.toString();
    process.stdout.write(`[kubectl] ${text}`);
    if (!serverStarted && /Forwarding from 127\.0\.0\.1:?\d+ -> 9901/.test(text)) {
      serverStarted = true;
      const env = {
        ...process.env,
        ENVOY_ADMIN_URL: process.env.ENVOY_ADMIN_URL || `http://127.0.0.1:${localPort}`,
        // Default to the real Envoy cluster name that fronts Triton traffic so
        // /clusters and /stats look at the right upstream without extra env.
        ENVOY_CLUSTER_FILTER: process.env.ENVOY_CLUSTER_FILTER || "triton_grpc_service",
      };
      const serverPath = path.join(__dirname, "server.js");
      log("dev", `starting backend with ENVOY_ADMIN_URL=${env.ENVOY_ADMIN_URL}`);
      server = spawn("node", [serverPath], {
        stdio: "inherit",
        env,
      });

      server.on("exit", (code, signal) => {
        log("dev", `backend exited (code=${code}, signal=${signal || "none"})`);
        kubectl.kill();
        process.exit(code != null ? code : 0);
      });
    }
  });

  kubectl.stderr.on("data", (data) => {
    const text = data.toString();
    process.stderr.write(`[kubectl] ${text}`);
  });

  kubectl.on("exit", (code, signal) => {
    log("dev", `kubectl port-forward exited (code=${code}, signal=${signal || "none"})`);
    if (server) {
      server.kill();
    }
    process.exit(code != null ? code : 0);
  });

  function shutdown() {
    log("dev", "received shutdown signal, cleaning up...");
    if (server) server.kill();
    kubectl.kill();
  }

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

start();

