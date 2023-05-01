def main():
    
    import pandas as pd
    import numpy as np
    import pickle
    import argparse
    
    parser = argparse.ArgumentParser(description='''\
    This script calculates bbprime from bbtot and the average of the two surrounding bbacids.''')
    
    parser.add_argument('--uwfile', nargs=1, type=str, required=True, help='''\
    Full path, name, and extension of underway file containing bbtot, bbacid, timestamp, and cruise name data.''')
    
    parser.add_argument('--cruiseNameColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing the cruise name.''')
    
    parser.add_argument('--datetimeColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing timestamps. Must be formatted yyyy-dd-mm hh:mm:ss.''')
    
    parser.add_argument('--numSamplesColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing the designated number of samples for the bb pH cycling.''')
    
    parser.add_argument('--bbtotColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing the bbtot data.''')
    
    parser.add_argument('--bbtotStdColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing the bbtot error data.''')
    
    parser.add_argument('--bbacidColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing the bbacid data.''')
    
    parser.add_argument('--bbacidStdColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing the bbacid error data.''')
    
    parser.add_argument('--bbprimeColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing the bbprime data.''')
    
    parser.add_argument('--bbprimeStdColumn', nargs=1, type=str, required=True, help='''\
    Name of column containing the bbprime error data.''')
    
    parser.add_argument('--ofileAveragedBB', nargs=1, type=str, required=True, help='''\
    Full filepath, name, and extension of the output file in which to save the bb-shifted dataframe. Must be csv.''')

    
    args = parser.parse_args()
    dict_args = vars(args)

    ### Define Dictionary Variables: ########################################
    
    uw_fp = dict_args['uwfile'][0]
    cruisename = dict_args['cruiseNameColumn'][0]
    datetime = dict_args['datetimeColumn'][0]
    numsamples = dict_args['numSamplesColumn'][0]
    bbtot = dict_args['bbtotColumn'][0]
    bbtot_std = dict_args['bbtotStdColumn'][0]
    bbacid = dict_args['bbacidColumn'][0]
    bbacid_std= dict_args['bbacidStdColumn'][0]
    bbprime = dict_args['bbprimeColumn'][0]
    bbprime_std = dict_args['bbprimeStdColumn'][0]
    ofile_avg_bb = dict_args['ofileAveragedBB'][0]
    
    ### Read in Data ########################################################
    
    uw = pd.read_csv(uw_fp)
    
    # We don't want to accidentally average two bbacids spanning two different cruises. Therefore, we will apply this script per cruise.
    # Generate a list of unique cruises:
    crz_list = uw[cruisename].unique()
    
    # Convert datetime column into pandas datetime objects:
    uw[datetime] = pd.to_datetime(uw[datetime])
    
    # If bb standard error columns do not exist, create them:
    if 'bbtot532StErr' not in uw.columns:
        idx = uw.columns.get_loc(bbtot) + 1
        uw.insert(idx, 'bbtot532StErr', uw[bbtot_std]/np.sqrt(uw[numsamples]))
    if 'bbacidStErr' not in uw.columns:
        idx = uw.columns.get_loc(bbacid) + 1
        uw.insert(idx, 'bbacidStErr', uw[bbacid_std]/np.sqrt(uw[numsamples]))
    if 'bbprimeStErr' not in uw.columns:
        idx = uw.columns.get_loc(bbprime) + 1
        uw.insert(idx, 'bbprimeStErr', uw[bbprime_std]/np.sqrt(uw[numsamples]))
        
                  
    
    ### DESIGNATE BBACID[i] AND BBACID[i-1]: #####################################
    # 1) Rename bbacid to bbacid[i].
    # 2) Create a new column named bbacid[i-1].
    # 3) Copy bbacid[i] into bbacid[i-1].
    # 4) Shift bbacid[i-1] down one row.
    
    bbcols = [col for col in uw.columns if 'bb' in col]
    insert_idx = uw.columns.get_loc(bbcols[-1]) + 1
    
    uw.rename(columns={bbacid:'bbacid[i]', 'bbacidStErr':'bbacidStErr[i]'}, inplace=True)
    previous_acid_cols = ['bbacid[i-1]', 'bbacidStErr[i-1]']
    
    shifted_acid_dfs = []
    for crz in crz_list:
        crz_df = uw.loc[uw[cruisename]==crz]
        crz_df.insert(insert_idx, 'bbacid[i-1]', crz_df['bbacid[i]'])
        crz_df.insert(insert_idx + 1, 'bbacidStErr[i-1]', crz_df['bbacidStErr[i]'])
        crz_df[previous_acid_cols] = crz_df[previous_acid_cols].shift(periods=1)
        shifted_acid_dfs.append(crz_df)
    
    acid_shifted_df = pd.concat(shifted_acid_dfs)
    
    ### PROPAGATE ERROR ON BBACID AVERAGE AND BBPRIME AVERAGE ##########################
    # 1) Create a bbacid average column and propagate the error: sqrt(acid[i]^2 + acid[i-1]^2)/2
    # 2) Calculate a bbprime average column: bbtot[i] - bbacid_average.
    # 3) Propagate error on bbprime average: sqrt(bbtot_err^2 + bbacid_average_err^2)
    
    if 'bbacidAvg' not in acid_shifted_df.columns:
        
        insert_idx = acid_shifted_df.columns.get_loc('bbacidStErr[i-1]') + 1
        
        bba_avg = acid_shifted_df[['bbacid[i]', 'bbacid[i-1]']].mean(axis=1, skipna=False)
        bba_avg_err = bbacidAvgError(acid_shifted_df['bbacidStErr[i]'], acid_shifted_df['bbacidStErr[i-1]'])
        
        acid_shifted_df.insert(insert_idx, 'bbacidAvg', bba_avg)
        acid_shifted_df.insert(insert_idx + 1, 'bbacidAvgStErr', bba_avg_err)
        
    if 'bbprimeAvg' not in acid_shifted_df.columns:
        
        insert_idx = acid_shifted_df.columns.get_loc('bbprimeStErr') + 1
        
        bbp_avg = acid_shifted_df[bbtot] - acid_shifted_df['bbacidAvg']
        bbp_avg_err = bbprimeError(acid_shifted_df['bbtot532StErr'], acid_shifted_df['bbacidAvgStErr'])
        
        acid_shifted_df.insert(insert_idx, 'bbprimeAvg', bbp_avg)
        acid_shifted_df.insert(insert_idx + 1, 'bbprimeAvgStErr', bbp_avg_err)
        
    ### SAVE OUT AVERAGE BB DATAFRAME ################################################
    acid_shifted_df.to_csv(ofile_avg_bb, index=False)
    
#########################################################################  
    
def bbacidAvgError(err1, err2):
    import numpy as np

    avgErr = np.sqrt(err1**2 + err2**2)/2
    return avgErr

def bbprimeError(tot_err, acid_err):
    import numpy as np

    bbpErr = np.sqrt(tot_err**2 + acid_err**2)
    return bbpErr    
    
if __name__ == "__main__": main()  