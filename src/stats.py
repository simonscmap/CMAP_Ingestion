import common as cmn
import pandas as pd
import numpy as np
import DB


def updateStatsTable(ID, json_str, server):
    conn, cursor  = DB.dbConnect(server)
    insertQuery = """INSERT INTO tblDataset_Stats_ID (Dataset_ID, JSON_stats) VALUES('{}','{}')""".format(ID, json_str)
    try:
        cursor.execute(insertQuery)
        conn.commit()
    except Exception as e: print(e)


def updateStats_Small(tableName, data_df,dataset_metadata_df,server='Rainier'): #Builds stats table entry for small tables (ex cruise)
            Dataset_ID = cmn.getDatasetID_DS_Name(dataset_metadata_df['dataset_short_name'].iloc[0])
            stats_df = data_df.describe()
            min_max_df = pd.DataFrame({'time':[data_df['time'].min(),data_df['time'].min()]},index=['min','max'])
            df = pd.concat([stats_df,min_max_df],axis=1, sort=True)
            json_str  = df.to_json(date_format = 'iso')
            sql_df = pd.DataFrame({'Dataset_ID': [Dataset_ID], 'JSON': [json_str]})
            updateStatsTable(Dataset_ID, json_str,server)
            print('Updated stats for ' + tableName)
