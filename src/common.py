import sys
sys.path.append('../conf/')
import vault_structure as vs
import pandas as pd
import numpy as np
import DB
import os

def strip_whitespace_headers(df):
    df.rename(columns=lambda x: x.strip())
    return df

def nanToNA(df):
    df = df.replace(np.nan, '', regex=True)
    return df

def lowercase_List(list):
    lower_list = [x.lower() for x in list]
    return lower_list

def getColBounds(df,col,list_multiplier= None):
    min_col = df[col].min()
    max_col = df[col].max()
    if list_multiplier != None:
        min_col = [min_col] * int(list_multiplier)
        max_col = [max_col] * int(list_multiplier)

    return min_col, max_col

def vault_struct_retrival(branch):
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
    cur_str = """select [ID] FROM [Opedia].[dbo].[tblDatasets] WHERE [Dataset_Name] = '""" + datasetName + """'"""
    query_return = DB.DB_query(cur_str)
    dsID = query_return.iloc[0][0]
    return dsID

def getDatasetID_Tbl_Name(tableName):
    cur_str = """select distinct [Dataset_ID] FROM [Opedia].[dbo].[tblVariables] WHERE [Table_Name] = '""" + tableName + """'"""
    query_return = DB.DB_query(cur_str)
    dsID = query_return.iloc[0][0]
    return dsID

def getKeywordsDataset(dataset_ID):
    cur_str = """select [ID] from tblVariables where Dataset_ID = '{dataset_ID}'""".format(dataset_ID = dataset_ID)
    query_return = DB.DB_query(cur_str)['ID'].to_list()
    return query_return

def getTableName_Dtypes(tableName):
    query = """ select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '""" + tableName + """'"""
    query_return = DB.DB_query(query)
    return query_return

def getCruiseDetails(cruiseName):
    query = """EXEC uspCruiseByName '""" + cruiseName + """'"""
    query_return = DB.DB_query(query)
    return query_return

def getListCruises():
    query = """EXEC uspCruises"""
    query_return = DB.DB_query(query)
    return query_return

def findVarID(datasetID, Short_Name,  server):
    """ this function pulls the ID value from the [tblVariables] for the tblKeywords to use """
    cur_str = """select [ID] FROM [Opedia].[dbo].[tblVariables] WHERE [Dataset_ID] = '""" + str(datasetID) + """' AND [Short_Name] = '"""+ Short_Name + """'"""
    query = DB.DB_query(cur_str)
    VarID = query.iloc[0][0]
    return VarID

def dBtoDF(tableName,server):
    query = """SELECT * FROM tblDataset_Stats WHERE Dataset_Name =  '%s'""" % (tableName)
    print(query)
    df = DB.dbRead(query,server)
    df = pd.read_json(df['JSON_stats'][0])
    return df

def deletefromStatsTable(tableName,server):
    conn = DB.dbConnect(server)
    cursor = conn.cursor()
    insertQuery = """DELETE FROM tblDataset_Stats where Dataset_Name = '%s'""" % (tableName)
    cursor.execute(insertQuery)
    conn.commit()


def find(name, path):
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)

def find_File_Path_guess_tree(name):
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
            elif 'satellite' in fpath:
                struct = vs.satellite
            elif 'satellite' in fpath:
                struct = vs.satellite
            elif 'model' in fpath:
                struct = vs.model
            elif 'assimilation' in fpath:
                struct = vs.assimilation
            else:
                struct = 'File path not found'
            return struct
