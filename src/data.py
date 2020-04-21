import sys
import os
import pandas as pd
import glob
sys.path.append('../conf/')
import vault_structure as vs



def import_single_data(make_tableName, process_level = 'REP'):
    flist = glob.glob(make_tableName + '/' + process_level.lower() + '/' +'*.csv*')
    df = pd.read_csv(flist[0],sep=',',parse_dates=['time'])
    df.rename(columns=lambda x: x.strip(), inplace=True)
    return df
df = import_single_data(vs.cruise + 'tblSeaglider_148_Mission_15')
