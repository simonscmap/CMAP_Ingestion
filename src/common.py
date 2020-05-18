import sys
sys.path.append('../conf/')
import vault_structure as vs
import pandas as pd
import numpy as np
import DB
import os


def normalize(vals, min_max=False):
    """Takes an array and either normalize to min/max, standardize it (remove the mean and divide by standard deviation)."""
    if min_max:
        normalized_vals=(vals-np.nanmin(vals))/(np.nanmax(vals)-np.nanmin(vals))
    else:
        normalized_vals=(vals-np.nanmean(vals))/np.nanstd(vals)
    return normalized_vals


def strip_whitespace_headers(df):
    """Strips any whitespace from dataframe headers"""
    df.columns = df.columns.str.strip()
    return df

def nanToNA(df):
    """Replaces and numpy nans with '' """
    df = df.replace(np.nan, '', regex=True)
    return df

def lowercase_List(list):
    """Converts every string in a list to lowercase string"""
    lower_list = [x.lower() for x in list]
    return lower_list

def getColBounds(df,col,list_multiplier='0'):
    """Gets the min and max bounds of a dataframe column

    Parameters
    ----------
    df : Pandas DataFrame
        Input DataFrame
    col : str
        Name of column
    list_multiplier: int, optional, default = 0
        Output is a a list of returned values with length of length list_multiplier integer


    Returns
    -------

    min_col_list, max_col_list
        returns two lists of column mins and maxes

    """
    min_col = [int(pd.to_numeric(df[col]).min())]
    max_col = [int(pd.to_numeric(df[col]).max())]
    if list_multiplier != '0':
        min_col = min_col * int(list_multiplier)
        max_col = max_col * int(list_multiplier)

    return min_col, max_col

def vault_struct_retrieval(branch):
    """Returns vault structure path for input branch"""
    if branch.lower() == 'cruise':
        vs_struct = vs.cruise
    elif branch.lower() == 'float':
        vs_struct = vs.float
    elif branch.lower() == 'station':
        vs_struct = vs.station
    elif branch.lower() == 'satellite':
        vs_struct = vs.satellite
    elif branch.lower() == 'model':
        vs_struct = vs.model
    elif branch.lower() == 'assimilation':
        vs_struct = vs.assimilation
    else:
        print('Vault branch structure not found in vault_structure.py. Please modify that script.')
    return vs_struct

def getDatasetID_DS_Name(datasetName):
    """Get DatasetID from input dataset name """
    cur_str = """select [ID] FROM [Opedia].[dbo].[tblDatasets] WHERE [Dataset_Name] = '""" + datasetName + """'"""
    query_return = DB.DB_query(cur_str)
    dsID = query_return.iloc[0][0]
    return dsID

def getDatasetID_Tbl_Name(tableName):
    """Get DatasetID from input table name """
    cur_str = """select distinct [Dataset_ID] FROM [Opedia].[dbo].[tblVariables] WHERE [Table_Name] = '""" + tableName + """'"""
    query_return = DB.DB_query(cur_str)
    dsID = query_return.iloc[0][0]
    return dsID

def getKeywordIDsTableNameVarName(tableName,var_short_name_list):
    """Get list of keyword ID's from input dataset ID"""
    cur_str = """select [ID] from tblVariables where Table_Name = '{tableName}' AND [Short_Name] in {vsnp}""".format(tableName = tableName,vsnp = tuple(var_short_name_list))
    query_return = str(tuple(DB.DB_query(cur_str)['ID'].to_list()))
    return query_return

def getKeywordsIDDataset(dataset_ID):
    """Get list of keyword ID's from input dataset ID"""
    cur_str = """select [ID] from tblVariables where Dataset_ID = '{dataset_ID}'""".format(dataset_ID = str(dataset_ID))
    query_return = DB.DB_query(cur_str)['ID'].to_list()
    return query_return

def getTableName_Dtypes(tableName):
    """Get data types from input table name """
    query = """ select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '""" + tableName + """'"""
    query_return = DB.DB_query(query)
    return query_return

def getCruiseDetails(cruiseName):
    """Get cruise details from cruise name using uspCruiseByName"""
    query = """EXEC uspCruiseByName '""" + cruiseName + """'"""
    query_return = DB.DB_query(query)
    return query_return

def getListCruises():
    """Get list of available cruises using uspCruises"""
    query = """EXEC uspCruises"""
    query_return = DB.DB_query(query)
    return query_return

def findVarID(datasetID, Short_Name,  server='Rainier'):
    """Get ID value from tblVariables for specific variable"""
    cur_str = """select [ID] FROM [Opedia].[dbo].[tblVariables] WHERE [Dataset_ID] = '""" + str(datasetID) + """' AND [Short_Name] = '"""+ Short_Name + """'"""
    query = DB.DB_query(cur_str)
    VarID = query.iloc[0][0]
    return VarID



def find_File_Path_guess_tree(name):
    """Attempts to return vault structure path for input filename"""
    for root, dirs, files in os.walk(vs.vault):
        if name in files:
            fpath=  os.path.join(root, name)
            if 'cruise' in fpath:
                struct = vs.cruise
            elif 'float' in fpath:
                struct = vs.float
            elif 'station' in fpath:
                struct = vs.station
            elif 'satellite' in fpath:
                struct = vs.satellite
            elif 'model' in fpath:
                struct = vs.model
            elif 'assimilation' in fpath:
                struct = vs.assimilation
            else:
                struct = 'File path not found'
            return struct

def verify_cruise_lists(dataset_metadata_df):
    """Returns matching and non matching cruises"""
    cruise_series = dataset_metadata_df['official_cruise_name(s)']
    """ check that every cruise_name in column exists in the database. map those that don't exist into return"""
    cruise_set = set(lowercase_List(cruise_series.to_list()))
    db_cruise_set = set(lowercase_List(getListCruises()['Name'].to_list()))
    matched = list(cruise_set.intersection(db_cruise_set))
    unmatched = list(cruise_set.difference(db_cruise_set))
    return matched, unmatched

def get_cruise_IDS(cruise_name_list):
    """Returns IDs of input cruise names"""
    cruise_db_df = getListCruises()
    cruise_name_list = lowercase_List(cruise_name_list)
    cruise_ID_list = cruise_db_df['ID'][cruise_db_df['Name'].str.lower().isin(cruise_name_list)].to_list()
    return cruise_ID_list
