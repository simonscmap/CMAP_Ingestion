import pandas as pd
import numpy as np
import DB

def strip_whitespace_headers(df):
    df.rename(columns=lambda x: x.strip())
    return df

def nanToNA(df):
    df = df.replace(np.nan, '', regex=True)
    return df

def getColBounds(df,col,list_multiplier= None):
    min_col = df[col].min()
    max_col = df[col].max()
    if list_multiplier != None:
        min_col = [min_col] * int(list_multiplier)
        max_col = [max_col] * int(list_multiplier)

    return min_col, max_col

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

def DB_query(query):
    api = pycmap.API()
    query_result = api.query(query)
    return query_result


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
