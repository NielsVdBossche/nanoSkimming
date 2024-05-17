# Mostly copied from crabrun and testdummy

# imports
import os, sys
import argparse
import ROOT
ROOT.PyConfig.IgnoreCommandLineOptions = True # (?)

# import tools from NanoAODTools
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor import PostProcessor
import PhysicsTools.NanoAODTools.postprocessing.modules.jme.jetmetHelperRun2 as jme
import PhysicsTools.NanoAODTools.postprocessing.modules.common.muonScaleResProducer as muoncorr

# import local tools
from PhysicsTools.nanoSkimming.skimselection.multilightleptonskimmer import MultiLightLeptonSkimmer
from PhysicsTools.nanoSkimming.skimselection.nlightleptonskimmer import nLightLeptonSkimmer
from PhysicsTools.nanoSkimming.processing.leptonvariables import LeptonVariablesModule
from PhysicsTools.nanoSkimming.processing.topleptonmva import TopLeptonMvaModule
from PhysicsTools.nanoSkimming.processing.leptongenvariables import LeptonGenVariablesModule
from PhysicsTools.nanoSkimming.processing.triggervariables import TriggerVariablesModule
from PhysicsTools.nanoSkimming.tools.sampletools import getsampleparams

# read command line arguments
parser = argparse.ArgumentParser(description='Submission through HTCondor')
parser.add_argument('-i', '--inputfile', required=True)
# parser.add_argument('-o', '--outputdir', required=True, type=os.path.abspath)
parser.add_argument('-n', '--nentries', type=int, default=-1)
# parser.add_argument('-d', '--dropbranches', default=None)
# parser.add_argument('-j', '--json', default=None)
args = parser.parse_args()

# print arguments
print('Running with following configuration:')
for arg in vars(args):
    print('  - {}: {}'.format(arg,getattr(args,arg)))

# set input files and output directory
inputfile = args.inputfile
inputfiles = [args.inputfile]
outputdir = os.getenv('TMPDIR')

# get sample parameters
# (note: no check is done on consistency between samples,
#  only first sample is used)
sampleparams = getsampleparams(inputfile)
year = sampleparams['year']
dtype = sampleparams['dtype']
print('Sample is found to be {} {}.'.format(year,dtype))

# else the jobs seem to fail with error codes pointing to missing job reports)
haddname = 'skimmed.root'
# (must be specified if jobreport is True,
# else there are warnings and unconvenient default values;
# note that it must correspond to the value in PSet.py and crabconfig.py.)
provenance = True
# (seems to take care of copying the MetaData and ParameterSets trees)

# define json preskim
jsonfile = None
if dtype=='data':
    jsonfile = '../data/lumijsons/lumijson_{}.json'.format(year)
    if not os.path.exists(jsonfile):
        # for CRAB submission, the data directory is copied to the working directory
        jsonfile = 'data/lumijsons/lumijson_{}.json'.format(year)
    if not os.path.exists(jsonfile):
        raise Exception('ERROR: json file not found.')

# define branches to drop and keep
dropbranches = '../data/dropbranches/fourtops.txt'
if not os.path.exists(dropbranches):
    # for CRAB submission, the data directory is copied to the working directory
    dropbranches = 'data/dropbranches/fourtops.txt'
if not os.path.exists(dropbranches):
    raise Exception('ERROR: dropbranches file not found.')

# set up JetMET module
yeardict = {
    '2016PreVFP': 'UL2016_preVFP',
    '2016PostVFP': 'UL2016',
    '2017': 'UL2017',
    '2018': 'UL2018'
}
JetMetCorrector = jme.createJMECorrector(
    isMC=(dtype=='sim'),
    dataYear=yeardict[year],
    jesUncert="Merged",
    splitJER=False
)

# set up Muon Rochester corrections module:
roccor_file = {
    '2016PreVFP': 'RoccoR2016aUL.txt',
    '2016PostVFP': 'RoccoR2016bUL.txt',
    '2017': 'RoccoR2017UL.txt',
    '2018': 'RoccoR2018UL.txt'
}
muonCorrector = muoncorr.muonScaleResProducer(
    rc_dir="roccor.Run2.v5",
    rc_corrections=roccor_file[year],
    dataYear=year
)

# Skimmer modules
leptonmodule = None
if dtype=='data':
    leptonmodule = nLightLeptonSkimmer(2,
        electron_selection_id='run2ul_loose',
        muon_selection_id='run2ul_loose')
else:
    leptonmodule = MultiLightLeptonSkimmer(
        electron_selection_id='run2ul_loose',
        muon_selection_id='run2ul_loose')

# Output modules
modules = ([
    leptonmodule,
    LeptonVariablesModule(),
    TopLeptonMvaModule(year, 'ULv1'),
    TriggerVariablesModule(year),
    JetMetCorrector(),
    muonCorrector()
])
if dtype!='data': modules.append(LeptonGenVariablesModule())

# set other arguments -> what does this do? -< prob postfix filename. Don't want this, remove
postfix = ''

# define a PostProcessor
p = PostProcessor(
    outputdir,
    inputfiles,
    modules = modules,
    maxEntries = None if args.nentries<=0 else args.nentries,
    postfix = postfix,
    branchsel = dropbranches,
    jsonInput = jsonfile
)

# run the PostProcessor
p.run()
