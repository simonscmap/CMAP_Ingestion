import common as cmn
import pandas as pd
import numpy as np
import DB
import pycmap
api = pycmap.API()

def updateStatsTable(ID, json_str, server):
    conn, cursor  = DB.dbConnect(server)
    print(conn,cursor)
    deleteQuery = """DELETE FROM tblDataset_Stats WHERE Dataset_ID = '{}'""".format(ID)
    insertQuery = """INSERT INTO tblDataset_Stats (Dataset_ID, JSON_stats) VALUES('{}','{}')""".format(ID, json_str)
    try:
        DB.DB_modify(deleteQuery)
        DB.DB_modify(insertQuery)

    except Exception as e: print(e)

def updateStats_Small(tableName, data_df=None,server='Rainier'):
            if data_df is not None:
                data_df = data_df
            else:
                query = 'SELECT * FROM {tableName}'.format(tableName=tableName)
                data_df = DB.dbRead(query,server)
            Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName)
            stats_df = data_df.describe()
            min_max_df = pd.DataFrame({'time':[data_df['time'].min(),data_df['time'].max()]},index=['min','max'])
            df = pd.concat([stats_df,min_max_df],axis=1, sort=True)
            json_str  = df.to_json(date_format = 'iso')
            sql_df = pd.DataFrame({'Dataset_ID': [Dataset_ID], 'JSON': [json_str]})
            updateStatsTable(Dataset_ID, json_str,server)
            print('Updated stats for ' + tableName)
