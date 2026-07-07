from WMCore.Configuration import Configuration
config = Configuration()

config.section_("General")
config.General.requestName = 'SONIC_MiniAOD_Purdue_1file'
config.General.workArea = 'crabsubmit'
config.General.transferLogs = True
config.General.transferOutputs = True

config.section_("JobType")
config.JobType.pluginName = 'Analysis'
config.JobType.psetName = 'run.py'
config.JobType.allowUndistributedCMSSW = True
# NOTE: When changing 'numCores' also change '--threads' below !
config.JobType.numCores = 2
config.JobType.maxMemoryMB = 5000
config.JobType.maxJobRuntimeMin = 230

### Move here all CRAB-related parameters from run.py
### NOTE: has to be a single line!
config.JobType.pyCfgParams=['--threads', '2', '--tempDir', '.', '--address', 'cms-run3-miniaod.sonic.geddes.rcac.purdue.edu', '--port', '8001', '--verboseDiscovery', '--tries', '10']

config.section_("Data")
config.Data.inputDataset = '/TTtoLNu2Q_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer23DRPremix-130X_mcRun3_2023_realistic_v14-v2/AODSIM'
# config.Data.inputDataset = '/TTto4Q_TuneCP5_13p6TeV_powheg-pythia8/Run3Summer23DRPremix-130X_mcRun3_2023_realistic_v14-v2/AODSIM'
config.Data.inputDBS = 'global'
config.Data.ignoreLocality = True
config.Data.allowNonValidInputDataset = True
config.Data.splitting = 'FileBased'
#config.Data.splitting = 'EventBased'
config.Data.unitsPerJob = 1
NJOBS = 1  # This is not a configuration parameter, but an auxiliary variable that we use in the next line.
config.Data.totalUnits = config.Data.unitsPerJob * NJOBS
config.Data.publication = False
config.Data.outputDatasetTag = 'SONIC_MiniAOD_Purdue_1file'
config.Data.outLFNDirBase = '/store/user/dkondrat'

config.section_("Site")
# only-Purdue
config.Site.blacklist = ['T2_US_Caltech','T2_US_UCSD','T2_US_Florida','T2_US_MIT','T2_US_Nebraska','T2_US_Vanderbilt','T2_US_Wisconsin']
config.Site.whitelist = ['T2_US_Purdue']
# non-Purdue
# config.Site.blacklist = ['T2_US_Purdue']
# config.Site.whitelist = ['T2_US_Caltech','T2_US_Florida','T2_US_UCSD','T2_US_MIT','T2_US_Nebraska','T2_US_Vanderbilt','T2_US_Wisconsin']
config.Site.storageSite = 'T2_US_Purdue'
# this is needed in order to prevent jobs overflowing to blacklisted sites
config.section_("Debug")
config.Debug.extraJDL = ['+CMS_ALLOW_OVERFLOW=False']
