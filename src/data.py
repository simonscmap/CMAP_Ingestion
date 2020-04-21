import sys
import os
import pandas as pd
import numpy as np
import glob
sys.path.append('../conf/')
import vault_structure as vs
import DB
import common
##############   Helper Funcs   ############

def removeMissings(cols, df):
    for col in cols:
        df[col].replace('', np.nan, inplace=True)
        df.dropna(subset=[col], inplace=True)
    return df

##############   Data Import    ############


def import_single_data(make_tableName, process_level = 'REP'):
    flist = glob.glob(make_tableName + '/' + process_level.lower() + '/' +'*.csv*')
    df = pd.read_csv(flist[0],sep=',',parse_dates=['time'])
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df
# df = import_single_data(vs.cruise + 'tblSeaglider_148_Mission_15')


##############   Data Insert    ############


def data_df_to_db(df, tableName,server = 'Rainier'):
    """
    input dataset dataframe - makes some checks/sorting, then inserts using bcp


    """
    df = common.nanToNA(df)
    df['time'] = pd.to_datetime(df['time'].astype(str), format='%Y-%m-%d %H:%M:%S',errors='coerce')
    if 'depth' in list(df):
        df = removeMissings(['time','lat', 'lon','depth'], df)
        df.sort_values(['time', 'lat', 'lon','depth'], ascending=[True, True, True, True])
    else:
        df = ip.removeMissings(['time','lat', 'lon'], df)
        df = df.sort_values(['time', 'lat', 'lon',], ascending=[True, True, True])

    temp_file_path = vs.BCP + tableName + '.csv'
    df.to_csv(temp_file_path, index=False)
    DB.toSQLbcp(temp_file_path, tableName,server)
    os.remove(temp_file_path)
