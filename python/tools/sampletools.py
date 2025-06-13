#############################
# Tools for sample handling #
#############################

def getsampleparams(sample):
    ### get year and data type for a given sample name
    # not guaranteed to be complete,
    # perhaps to extend when encountering more 'exotic' sample names...
    years = []
    dtypes = []
    if all([tag.lower() in sample.lower() for tag in ['HIPM_UL2016', 'Run2016']]):
        years.append('2016PreVFP')
        dtypes.append('data')
    if all([tag.lower() in sample.lower() for tag in ['UL2016', 'Run2016']]) and not "HIPM_UL2016".lower() in sample.lower():
        years.append('2016PostVFP')
        dtypes.append('data')
    if all([tag.lower() in sample.lower() for tag in ['UL2017', 'Run2017']]):
        years.append('2017')
        dtypes.append('data')
    if all([tag.lower() in sample.lower() for tag in ['UL2018', 'Run2018']]):
        years.append('2018')
        dtypes.append('data')
    if any([tag.lower() in sample.lower() for tag in ['RunIISummer20UL16APV', 'Run2SIM_UL2016PreVFP', 'PreVFP', 'preVFP', 'RunIISummer20UL16NanoAODAPV']]):
        years.append('2016PreVFP')
        dtypes.append('sim')
    if any([tag.lower() in sample.lower() for tag in ['RunIISummer20UL16NanoAODv', 'Run2SIM_UL2016PostVFP', 'Run2SIM_UL2016Mini', 'PostVFP']]):
        years.append('2016PostVFP')
        dtypes.append('sim')
    if any([tag.lower() in sample.lower() for tag in ['RunIISummer20UL17', 'Run2SIM_UL2017']]):
        years.append('2017')
        dtypes.append('sim')
    if any([tag.lower() in sample.lower() for tag in ['RunIISummer20UL18', 'Run2SIM_UL2018']]):
        years.append('2018')
        dtypes.append('sim')
    if len(set(years))!=1:
        msg = 'ERROR: could not determine year'
        msg += ' for sample {}, found candidates {}'.format(sample, years)
        raise Exception(msg)
    if len(set(dtypes))!=1:
        msg = 'ERROR: could not determine data type'
        msg += ' for sample {}, found candidates {}'.format(sample, dtypes)
        raise Exception(msg)
    
    ret = {'year': years[0], 'dtype': dtypes[0]}
    if ret['dtype'] == 'data':
        # split on "Run" and take last part
        runperiod = sample.split("Run")[-1][4]
        # assert runperiod is a capitalized letter:
        if not runperiod.isupper():
            msg = 'ERROR: could not determine run period'
            msg += ' for sample {}'.format(sample)
            msg += " Make sure sample name for data follows Run[YEAR][PERIOD] "
            msg += "format with period a single capital letter for the era."
            raise Exception(msg)
        ret['runperiod'] = runperiod
    else:
        ret['runperiod'] = "B"

    return ret
