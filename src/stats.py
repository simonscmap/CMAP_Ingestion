import common as cmn
import pandas as pd
import numpy as np
import DB
import pycmap
api = pycmap.API()

def updateStatsTable(ID, json_str, server):
    conn, cursor  = DB.dbConnect(server)
    deleteQuery = """DELETE FROM tblDataset_Stats WHERE Dataset_ID = '{}'""".format(ID)
    insertQuery = """INSERT INTO tblDataset_Stats (Dataset_ID, JSON_stats) VALUES('{}','{}')""".format(ID, json_str)
    try:
        cursor.execute(deleteQuery)
        cursor.execute(insertQuery)
        conn.commit()
    except Exception as e: print(e)

def updateStats_Small(tableName, data_df='',server='Rainier'):
            print(tableName)
            if data_df == '':
                query = 'SELECT * FROM {tableName}'.format(tableName=tableName)

                data_df = DB.dbRead(query,server)
            else:
                data_df = data_df
            Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName)
            stats_df = data_df.describe()
            min_max_df = pd.DataFrame({'time':[data_df['time'].min(),data_df['time'].max()]},index=['min','max'])
            df = pd.concat([stats_df,min_max_df],axis=1, sort=True)
            json_str  = df.to_json(date_format = 'iso')
            sql_df = pd.DataFrame({'Dataset_ID': [Dataset_ID], 'JSON': [json_str]})
            updateStatsTable(Dataset_ID, json_str,server)
            # print('Updated stats for ' + tableName)

qry = """SELECT DISTINCT([Table_Name]) FROM [Opedia].[dbo].[tblVariables] where Sensor_ID != '3' AND Sensor_ID != '1' and Table_Name not in (
'tblKM1314_Cobalmins',
'tblKM1906_Gradients3',
'tblBottle_Chisholm',
'tblKM1709_mesoscope_CTD',
'tblBATS_Pigment_Validation',
'tblFalkor_2018',
'tblKOK1606_Gradients1_Cobalamins',
'tblKM1513_HOE_legacy_2A_Dyhrman_Omics',
'tblOceans_Melting_Greenland_CTD',
'tblSCOPE_HOT_metagenomics',
'tblFlombaum',
'tblGLODAP',
'tblBATS_CTD',
'tblBATS_Zooplankton_Biomass',
'tblGlobalDrifterProgram',
'tblKOK1507_HOE_Legacy_2B',
'tblHOE_Legacy_2B',
'tblWOA_Climatology',
'tblBATS_Pigment',
'tblMGL1704_Gradients2_Cobalamins',
'tblAloha_Deep_Trap_Omics',
'tblCTD_Chisholm',
'tblSeaFlow',
'tblBATS_Bottle',
'tblArgoMerge_REP')"""
table_list = api.query(qry)



stats_df = updateStats_Small('tblBATS_Pigment')
for tblName in table_list['Table_Name']:
    try:
        updateStats_Small(tblName)
    except:
        print('failed on table: ' + tblName)
