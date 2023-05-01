def main():
    
    import pandas as pd
    import numpy as np
    import pickle
    import argparse
    
    parser = argparse.ArgumentParser(description='''\
    This takes in an underway file and a discrete file and merges them together by nearest timestamp within 5 minutes. Note that the merge occurs differently for GNATS cruises than for AMT style cruises. Therefore, as an input argument, we need to specify whether or not our data is solely Gnats, solely AMT, or both. Additionally, in the merged dataframe, we create an overall ID, which is: "cruisename_uwid" or if not uwid exists: "cruisename_discreteid". ''')
    
    parser.add_argument('--uwFile', nargs=1, type=str, required=True, help='''\
    Full path, name, and extension of underway file.''')
    
    parser.add_argument('--discreteFile', nargs=1, type=str, required=True, help='''\
    Full path, name, and extension of discrete file.''')
    
    parser.add_argument('--uwIdCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains underway ids.''')
    
    parser.add_argument('--discreteIdCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains discrete ids.''')
    
    parser.add_argument('--uwCruiseNameCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains underway cruisenames.''')
    
    parser.add_argument('--discreteCruiseNameCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains discrete cruisenames.''')
    
    parser.add_argument('--uwTimeCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains underway timestamps.''')
    
    parser.add_argument('--discreteTimeCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains discrete timestamps.''')
    
    parser.add_argument('--uwLongitudeCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains underway longitudes.''')
    
    parser.add_argument('--discreteLongitudeCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains discrete longitudes.''')
    
    parser.add_argument('--uwLatitudeCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains underway latitudes.''')
    
    parser.add_argument('--discreteLatitudeCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains discrete latitudes.''')
    
    parser.add_argument('--discreteShallowestCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains discrete station shallowest sample info.''')
    
    parser.add_argument('--discreteDepthCol', nargs=1, type=str, required=True, help='''\
    Name of column that contains discrete depths.''')
   
    parser.add_argument('--ofileFieldData', nargs=1, type=str, required=True, help='''\
    Full path, name, and extension of where to save merged field csv.''')  

    
    args = parser.parse_args()
    dict_args = vars(args)

    ### Define Dictionary Variables: ########################################
    
    uw_fp = dict_args['uwFile'][0]
    d_fp = dict_args['discreteFile'][0]
    uw_id = dict_args['uwIdCol'][0]
    d_id = dict_args['discreteIdCol'][0]
    uw_cruisename = dict_args['uwCruiseNameCol'][0]
    d_cruisename = dict_args['discreteCruiseNameCol'][0]
    uw_time = dict_args['uwTimeCol'][0]
    d_time = dict_args['discreteTimeCol'][0]
    uw_longitude = dict_args['uwLongitudeCol'][0]
    d_longitude = dict_args['discreteLongitudeCol'][0]
    uw_latitude = dict_args['uwLatitudeCol'][0]
    d_latitude = dict_args['discreteLatitudeCol'][0]
    d_shallowest = dict_args['discreteShallowestCol'][0]
    d_depth = dict_args['discreteDepthCol'][0]
    ofile = dict_args['ofileFieldData'][0]
    
    # Read in Data:
    uw = pd.read_csv(uw_fp)
    d = pd.read_csv(d_fp)
    
    # Turn datetime strings into pandas timestamps:
    uw.loc[:, uw_time] = pd.to_datetime(uw[uw_time])
    d.loc[:, d_time] = pd.to_datetime(d[d_time])
    
    # Generate a list of all cruises to iterate through:
    uw_cruises = [crz for crz in uw[uw_cruisename].unique()]
    d_cruises = [crz for crz in d[d_cruisename].unique()]
    crz_list = np.unique(uw_cruises + d_cruises)
    
    # Generate a list of GNATS cruises, and a list of AMT cruises:
    gnats_crz = [crz for crz in crz_list if crz[0]=='s']
    amt_crz = [crz for crz in crz_list if crz not in gnats_crz]
    
    # Merge Underway and field data for gnats cruises, and for amt cruises.
    # Merge data cruise by cruise:
    merged_cruise_dfs = []    
    for crz in gnats_crz:
        uw_crz_df = uw.loc[uw[uw_cruisename]==crz]
        d_crz_df = d.loc[d[d_cruisename]==crz]
        merged_cruise_dfs.append(underwayDiscreteMergeGnats(uw_crz_df, d_crz_df, uw_time, d_time, uw_cruisename, d_cruisename))
    
    for crz in amt_crz:
        merged_cruise_dfs.append(underwayDiscreteMergeAMT(uw.loc[uw[uw_cruisename]==crz], d.loc[d[d_cruisename]==crz], uw_id, d_id, uw_time, d_time, uw_cruisename, d_cruisename, uw_longitude, d_longitude, uw_latitude, d_latitude, d_shallowest, d_depth))
    
        
    field = pd.concat(merged_cruise_dfs)
    field.sort_values(by=['yyyy-mm-ddThh:mm:ss', uw_time], inplace=True, ignore_index=True)
    
    # Combine Underway lat/lons with discrete lat/lons. If a discrete lat/lon exists, report it. Otherwise, fill with underway coordinates.
    field[d_latitude] = field[d_latitude].fillna(field[uw_latitude])
    field[d_longitude] = field[d_longitude].fillna(field[uw_longitude])
    
    # Create a universal id which will be used in the matchup process:
    data_ids = [crz + '_' + str(uwid).split(sep='.')[0] if np.isnan(uwid)==False else crz + '_' + str(did).split(sep='.')[0] for crz,uwid,did in zip(field['CruiseName'],field[uw_id],field[d_id])]
    field.insert(0,'ID',data_ids)
        
    # Save out merged field data file:
    field.to_csv(ofile, index=False)
        
def underwayDiscreteMergeGnats(uw_crz_df, d_crz_df, uw_time_col, d_time_col, uw_cruisename_col, d_cruisename_col):
    
    import pandas as pd
    import numpy as np
    
    crz = uw_crz_df[uw_cruisename_col].unique()[0]
    
    # Drop Cruisename column:
    uw_crz_df = uw_crz_df.drop(columns = uw_cruisename_col)
    d_crz_df = d_crz_df.drop(columns = d_cruisename_col)
    
    # Ensure Datetime Columns are pandas timestamps, not strings:
    uw_crz_df.loc[:, uw_time_col] = pd.to_datetime(uw_crz_df[uw_time_col])
    d_crz_df.loc[:, d_time_col] = pd.to_datetime(d_crz_df[d_time_col])
    
    # Create Matching Datetime column in underway and discrete dataframes, so that when we merge, this column will populate with both underway and discrete datetimes:
    uw_crz_df['yyyy-mm-ddThh:mm:ss'] = uw_crz_df[uw_time_col]
    d_crz_df.insert(1, 'yyyy-mm-ddThh:mm:ss', d_crz_df[d_time_col])
    
    uw_nearest_idx = []
    d_nearest_idx = []
    
    for idx in d_crz_df.index:
        
        # Calculate the delta time for current discrete data point to all underway datapoints:
        delta_dts = abs(uw_crz_df[uw_time_col] - d_crz_df[d_time_col].loc[idx])
        
        # If there are some underway data points are within 5 minutes of the current discrete datapoint:
        # Save the index of the underway datapoint with the smallest time delta.
        if len(delta_dts.loc[delta_dts <= pd.Timedelta('5min')]) > 0:
            uw_idx = delta_dts.loc[delta_dts <= pd.Timedelta('5min')].idxmin()
            uw_nearest_idx.append(uw_idx)
            d_nearest_idx.append(idx)
        
    # Partition the underway and discrete data in nearest vs. non-nearest data:
    uw_nearest = uw_crz_df.loc[uw_crz_df.index.isin(uw_nearest_idx)]
    d_nearest = d_crz_df.loc[d_crz_df.index.isin(d_nearest_idx)]

    uw_unmatched = uw_crz_df.loc[~uw_crz_df.index.isin(uw_nearest_idx)]
    d_unmatched = d_crz_df.loc[~d_crz_df.index.isin(d_nearest_idx)]

    # Apply merge as of with a 5 minute time tolerance to the nearest data subsets:
    nearest_merge = pd.merge_asof(d_nearest, uw_nearest, on='yyyy-mm-ddThh:mm:ss', direction='nearest', allow_exact_matches=True)

    # Apply a regular merge to the rest of the data:
    unmatched_merge = pd.merge(d_unmatched, uw_unmatched, how='outer', on='yyyy-mm-ddThh:mm:ss')

    # Concat the nearest and unmatched merged dataframes into a single merged dataframe.
    # Sort by merged datetime column (which contains both underway and discrete datetimes).
    gnats_field = pd.concat([nearest_merge, unmatched_merge])
    gnats_field.sort_values(['yyyy-mm-ddThh:mm:ss'], inplace=True, ignore_index=True)
    
    gnats_field.insert(0, 'CruiseName', crz)

    return gnats_field        
        
    
def underwayDiscreteMergeAMT(uw_crz_df, d_crz_df, uw_id_col, d_id_col, uw_time_col, d_time_col, uw_cruisename_col, d_cruisename_col, uw_longitude_col, d_longitude_col, uw_latitude_col, d_latitude_col, d_shallowest_col, d_depth_col):
    
    crz = uw_crz_df[uw_cruisename_col].unique()[0]
    
    # Drop Cruisename column:
    uw_crz_df = uw_crz_df.drop(columns = uw_cruisename_col)
    d_crz_df = d_crz_df.drop(columns = d_cruisename_col)
    
    # Ensure Datetime Columns are pandas timestamps, not strings:
    uw_crz_df[uw_time_col] = pd.to_datetime(uw_crz_df[uw_time_col])
    d_crz_df[d_time_col] = pd.to_datetime(d_crz_df[d_time_col])
    
    # Create Matching Datetime column in underway and discrete dataframes, so that when we merge, this column will populate with both underway and discrete datetimes:
    uw_crz_df['yyyy-mm-ddThh:mm:ss'] = uw_crz_df[uw_time_col]
    d_crz_df.insert(1, 'yyyy-mm-ddThh:mm:ss', d_crz_df[d_time_col])
    
    # Partition discrete data into surface vs at depth based on Shallowest flag, and depth<=10.
    surf = d_crz_df.loc[(d_crz_df[d_shallowest_col]==1)&(d_crz_df[d_depth_col]<=10)]
    depth = d_crz_df.loc[~d_crz_df.index.isin(surf.index)]
    
    # Merge surface data with merge_asof and a time tolerance of 6 hours:
    surf_merge = pd.merge_asof(surf, uw_crz_df, on='yyyy-mm-ddThh:mm:ss', direction='nearest', allow_exact_matches=True)
    
    # Only Keep merged data if the underway and discrete gps coordinates are within 0.01 degrees of each other:
    surf_merge = surf_merge.loc[((abs(surf_merge[d_longitude_col] - surf_merge[uw_longitude_col])<=0.01)&(abs(surf_merge[d_latitude_col] - surf_merge[uw_latitude_col])<=0.01))]
    
    # Collect all the discrete and underway data that did not get merged in the surface merge:
    uw_unmatched = uw_crz_df.loc[~uw_crz_df[uw_id_col].isin(surf_merge[uw_id_col])]
    d_unmatched = d_crz_df.loc[~d_crz_df[d_id_col].isin(surf_merge[d_id_col])]
    
    # Merge the unmatched underway and discrete data:
    unmatched_merge = pd.merge(d_unmatched, uw_unmatched, how='outer', on='yyyy-mm-ddThh:mm:ss')
    
    # Concat the surface and unmatched merged dataframes into a single merged dataframe:
    amt_field = pd.concat([surf_merge, unmatched_merge])
    amt_field.sort_values('yyyy-mm-ddThh:mm:ss', inplace=True, ignore_index=True)
    
    amt_field.insert(0, 'CruiseName', crz)
    
    return amt_field

if __name__ == "__main__": main()  