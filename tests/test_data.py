import pandas as pd
from pandas._testing import assert_frame_equal
import sys
import numpy as np
sys.path.append('../src')
import data


def test_removeMissings():
    test_df =  pd.DataFrame({'lat': [-90,90,'',-12.34],'lon': [-180,180,134.2,''], 'var': [1,2,3,4]})
    expected_df = pd.DataFrame({'lat': [-90,90],'lon': [-180,180], 'var': [1,2]})
    func_df = data.removeMissings(test_df, ['lat','lon'])
    assert_frame_equal(func_df, expected_df,check_dtype=False,obj = "removeMissings test failed")

def test_format_time_col():
    test_df =  pd.DataFrame({'time': ['2018-03-15T05:41:00','2018-03-15 05:41:00','2018-03-15','2018/03/15']})
    expected_df = pd.DataFrame({'time': ['2018-03-15T05:41:00.000000000','2018-03-15T05:41:00.000000000','2018-03-15T00:00:00.000000000','2018-03-15T00:00:00.000000000']})
    func_df = data.format_time_col(test_df,'time')
    assert_frame_equal(func_df,expected_df,check_datetimelike_compat=True,check_dtype=False,obj = "test_format_time_col test failed")

def test_sort_values():
    test_df = pd.DataFrame({'time': ['2015-01-01','2000-01-11'],'lon': [-180,-170], 'lat': [-90,-80]})
    expected_df = pd.DataFrame({'time': ['2000-01-11','2015-01-01'],'lon': [-170,-180], 'lat': [-80,-90]})
    func_df = data.sort_values(test_df, ['time','lon','lat'])
    assert func_df.reset_index(drop=True, inplace=True) == expected_df.reset_index(drop=True, inplace=True), "test_sort_values test failed"

def sort_values(df,cols):
    """Sorts dataframe cols

    Parameters
    ----------
    df : Pandas DataFrame
        The dataframe to be modified
    cols : list
        List of column name strings

    Returns
    -------
    df
        Pandas DataFrame with input cols sorts in ASC order.
    """
    df = df.sort_values(cols, ascending=[True] * len(cols))
    return df

def ST_columns(df):
    """Returns SpaceTime related columns in a dataset"""
    df_cols = cmn.lowercase_List(list(df))
    ST_vars = [st for st in df_cols if "time" in st or "lat" in st or "lon" in st or "depth" in st]
    return ST_vars

##############   Data Import    ############

def read_csv(path_and_filename,delim = ','):
    """Imports csv into pandas DataFrame"""
    df = pd.read_csv(path_and_filename,sep=delim,parse_dates=['time'])
    return df


def fetch_single_datafile(branch,tableName, file_ext = '.csv',process_level = 'REP'):
    """Finds first file in glob with input path to vault structure. Returns path_filename """
    vault_path = cmn.vault_struct_retrieval(branch)
    flist = glob.glob(vault_path + tableName+'/' + process_level.lower() + '/' +'*' + file_ext)
    return flist
