def main():
    
    import pandas as pd
    import numpy as np
    import pickle
    import argparse
    
    parser = argparse.ArgumentParser(description='''\
    This script shifts bb data up one timestamp where applicable within a given underway file.''')
    
    parser.add_argument('--uwfile', nargs=1, type=str, required=True, help='''\
    Full path, name, and extension of underway file containing bbtot, bbacid, timestamp, and cruise name data.''')
    
    parser.add_argument('--bbcycleParametersFile', nargs=1, type=str, required=True, help='''\
    Full path, name, and extension of the python dictionary pickle file containing acceptable bb cycling durations on a per cruise basis.''')
    
    parser.add_argument('--cruiseNameColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing the cruise name.''')
    
    parser.add_argument('--datetimeColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing timestamps. Must be formatted yyyy-dd-mm hh:mm:ss.''')
    
    parser.add_argument('--numSamplesColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing the number of samples set for the bb cycling.''')
    
    parser.add_argument('--ofileShiftedData', nargs=1, type=str, required=True, help='''\
    Full filepath, name, and extension of the output file in which to save the bb-shifted dataframe. Must be csv.''')

    
    args = parser.parse_args()
    dict_args = vars(args)

    ### Define Dictionary Variables: ########################################
    
    uw_fp = dict_args['uwfile'][0]
    cycle_fp = dict_args['bbcycleParametersFile'][0]
    cruisename = dict_args['cruiseNameColumn'][0]
    datetime = dict_args['datetimeColumn'][0]
    numsamples = dict_args['numSamplesColumn'][0]
    ofile_shifted = dict_args['ofileShiftedData'][0]
    
    ### Read in Data ###
    
    uw = pd.read_csv(uw_fp)
    with open(cycle_fp, 'rb') as handle:
        cycleDict = pickle.load(handle) 
    
    
    ### Insert a Cycle Duration Column. ###
    ## Note that the cycle duration associated with a row is the difference in time between the current row and the next row.

    crz_list = uw[cruisename].unique()
    uw[datetime] = pd.to_datetime(uw[datetime])
    insert_idx = uw.columns.get_loc(numsamples) + 1
    
    # Perform operation per each individual cruise
    crz_dfs = []
    for crz in crz_list:
        crz_df = uw.loc[uw[cruisename]==crz]
        crz_df = crz_df.sort_values(datetime)
        
        # Calculate and record the time in-between successive rows
        cycle = []
        for i in range(len(crz_df) - 1):
            cycle.append((crz_df[datetime].iloc[i+1] - crz_df[datetime].iloc[i]).total_seconds())
        # For the last row, append a nan-time as there is no subsequent row with which to calculate a delta t
        cycle.append(pd.NaT)
        
        # Append the delta t's between rows as a new column:
        crz_df.insert(insert_idx, 'cycle_duration[s]', cycle)
        crz_dfs.append(crz_df)
        
    cycle_df = pd.concat(crz_dfs)
    
    ### Separate GNATS/EN616 From AMT System Cruises: ###
    gnats_df = cycle_df.loc[(cycle_df[cruisename].str[0]=='s')|(cycle_df[cruisename]=='en616')]
    gnats_crz = gnats_df[cruisename].unique()
    
    amt_df = cycle_df.loc[(cycle_df[cruisename].str[0]!='s')&(cycle_df[cruisename]!='en616')]
    amt_crz = amt_df[cruisename].unique()
    
    ### Create a Cycle Duration Flag Column ###
    # 0: No flag
    # 1: shorter than minimum acceptable cycle
    # 2: longer than maximum acceptable cycle
    # 3: cycle duration is null --> last timestamp for cruise
    # 4: amt cruise with nSamples set to odd/inconsistent value.
    ## Note that cycle min/max limits were set based on the numSamples setting.  Histograms/distributions were plotted for all cruises with the same numSamples, and limits set by these distributions. These limits were stored in the bbCycleParameters Pickle File. 
    
    cycle_flag_idx = cycle_df.columns.get_loc('cycle_duration[s]') + 1
    
    # All gnats cruises as well as en616 had nSamples = 30, so we only have 1 minimum and 1 maximum cycle limit for all gnats. 
    if len(gnats_df) > 0:
        gmin_cycle = cycleDict['gnats'][0]
        gmax_cycle = cycleDict['gnats'][1]

        if 'cycle_duration_flag' not in gnats_df.columns:
            gnats_df.insert(cycle_flag_idx, 'cycle_duration_flag', 0)
        gnats_df.loc[gnats_df['cycle_duration[s]'] < gmin_cycle, 'cycle_duration_flag'] = 1
        gnats_df.loc[gnats_df['cycle_duration[s]'] > gmax_cycle, 'cycle_duration_flag'] = 2
        gnats_df.loc[gnats_df['cycle_duration[s]'].isnull()==True, 'cycle_duration_flag'] = 3
    
    flagged_gnats_df = gnats_df.copy()
    
    
     # For AMT cruises, we have limits set based on the nSamples per cruise.
    # There were a few odd entries with a few rows having nSamples set to 10, 25, or 200. This was most likely interactively set for testing parameters/calibrations. Lets section out these rows/cruises, and flag the data with flag=4.
    if len(amt_df) > 0:
    
        amt_nsample_dfs = []
        amt_nsamples = [20, 30, 40, 45, 50, 60, 65, 70, 75, 80, 85, 90, 100]
        for ns,key in zip(amt_nsamples, cycleDict.keys()):
            nsample_df = amt_df.loc[amt_df[numsamples]==ns]
            nsmin_cycle = cycleDict[key][0]
            nsmax_cycle = cycleDict[key][1]

            if 'cycle_duration_flag' not in nsample_df.columns:
                nsample_df.insert(cycle_flag_idx, 'cycle_duration_flag', 0)
                nsample_df.loc[nsample_df['cycle_duration[s]'] < nsmin_cycle, 'cycle_duration_flag'] = 1
                nsample_df.loc[nsample_df['cycle_duration[s]'] > nsmax_cycle, 'cycle_duration_flag'] = 2
                nsample_df.loc[nsample_df['cycle_duration[s]'].isnull()==True, 'cycle_duration_flag'] = 3

            amt_nsample_dfs.append(nsample_df)

        for ns in [10, 25, 120]:
            nsample_df = amt_df.loc[amt_df[numsamples]==ns]
            nsample_df.loc[:, 'cycle_duration_flag'] = 4
            amt_nsample_dfs.append(nsample_df)

        flagged_amt_df = pd.concat(amt_nsample_dfs)
    else:
        flagged_amt_df = amt_df.copy()
    
    ### SHIFT THE BB DATA UP ONE TIMESTAMP ##########################################
    
    flagged_uw_df = pd.concat([flagged_gnats_df, flagged_amt_df])
    bbcols = [col for col in flagged_uw_df.columns if ('bb' in col)&('470' not in col)&('676' not in col)]
    
    shifted_bb_dfs = []
    for crz in crz_list:
        crz_df = flagged_uw_df.loc[flagged_uw_df[cruisename]==crz]
        crz_df = crz_df.sort_values(datetime)
        crz_df[bbcols] = crz_df[bbcols].shift(periods=-1)
        shifted_bb_dfs.append(crz_df)
        
    shifted_uw_df = pd.concat(shifted_bb_dfs)
    
    # Nullify data that was flagged and therefore should not have been shifted:
    shifted_uw_df.loc[shifted_uw_df['cycle_duration_flag']!=0, bbcols] = np.nan
    
    # Save out Shifted Dataframe:
    shifted_uw_df.to_csv(ofile_shifted, index=False)
    
    
if __name__ == "__main__": main() 
    
    