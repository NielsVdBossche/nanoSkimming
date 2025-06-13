# This file is formally needed by CRAB,
# but it is just a dummy; do not modify.
# Also do not change the file name.

# Exception 1: the numberOfThreads parameter must be consistent
# with the numCores in the CRAB configuration file,
# (there will be errors when trying to submit if they aren't)
# so this value can be modified as needed.

# Exception 2: you can change the process.source.fileNames
# to hold a test file of your liking for local testing,
# it is ignored and replaced by a correct input file within a CRAB job.
# Note: the same does not seem to hold for maxEvents
# it seems to be ignored in local tests as well as in CRAB jobs.

# Note: special care should be taken with the OutputModule fileName.
# This parameter specifies the name of the file that will be looked for
# by the CRAB processor upon finishing crabrun.py;
# hence it must correspond to the output file name set in crabrun.py
# (and also with the one in crabconfig.py, if specified there),
# else the job will crash even though the actual code ran without an issue.

import FWCore.ParameterSet.Config as cms
process = cms.Process('NANO')
process.source = cms.Source("PoolSource",
  fileNames = cms.untracked.vstring(),
)
process.source.fileNames = [
    #'root://xrootd-cms.infn.it///store/mc/RunIISummer20UL18NanoAODv9/TTWJetsToLNu_TuneCP5_13TeV-amcatnloFXFX-madspin-pythia8/NANOAODSIM/106X_upgrade2018_realistic_v16_L1v1-v1/270000/0BF1CAC0-066F-6C41-89EF-6F7B67A8E1D7.root'
    # 'file:///pnfs/iihe/cms/store/user/llambrec/nanoaod/TTWJetsToLNu-RunIISummer20UL18-nanoAOD-fullfile.root'
    'root://xrootd-cms.infn.it///store/data/Run2017B/SingleMuon/NANOAOD/UL2017_MiniAODv2_NanoAODv9_GT36-v1/2810000/16C69B97-668D-394A-A91B-4E2189CB3512.root'
]
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(10))
# process.options.numberOfThreads=cms.untracked.uint32(1)
# process.options.numberOfStreams=cms.untracked.uint32(1)
process.options   = cms.untracked.PSet(numberOfStreams = cms.untracked.uint32(1), numberOfThreads = cms.untracked.uint32(1))

process.output = cms.OutputModule("PoolOutputModule",
  fileName = cms.untracked.string('skimmed.root'))
process.out = cms.EndPath(process.output)
