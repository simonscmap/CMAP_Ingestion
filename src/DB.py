import sys
import os
sys.path.append('../conf/login/')
import credentials as cr
import pyodbc
import platform
import pandas as pd
import pycmap

######## DB Specific ############

def DB_query(query):
    api = pycmap.API()
    query_result = api.query(query)
    return query_result




def server_select_credentials(server):
    if server == 'Rainier':
        usr=cr.usr_rainier
        psw=cr.psw_rainier
        ip=cr.ip_rainier
        port = cr.port_rainier
    else:
        usr=cr.usr_beast
        psw=cr.psw_beast
        ip=cr.ip_beast
        port = cr.port_beast

    return usr,psw,ip,port


def dbConnect(server,db='Opedia', TDS_Version='7.3'):
    usr,psw,ip,port = server_select_credentials(server)

    server = ip + ',' + port

    if platform.system().lower().find('windows') != -1:
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Uid=' + usr + ';Pwd='+ psw )
    elif platform.system().lower().find('darwin') != -1:
        conn = pyodbc.connect('DRIVER=/usr/local/lib/libtdsodbc.so;SERVER=' + server + ';DATABASE=' + db + ';Uid=' + usr + ';Pwd='+ psw )
    elif platform.system().lower().find('linux') != -1:
        conn = pyodbc.connect(DRIVER='/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so', TDS_Version =  TDS_Version , server =  ip , port =  port, uid = usr, pwd = psw)
    cursor = conn.cursor()
    return conn, cursor



def lineInsert(server,tableName, columnList ,query):
    conn,cursor = dbConnect(server)
    insertQuery = """INSERT INTO {} {} VALUES {} """.format(tableName, columnList, query)
    cursor.execute(insertQuery)
    conn.commit()




def toSQLbcp(export_path, tableName,  server):

    usr,psw,ip,port = server_select_credentials(server)
    str = """bcp Opedia.dbo.""" + tableName + """ in """ +"""'""" + export_path + """'""" + """ -e error -c -t, -U  """ + usr + """ -P """ + psw + """ -S """ + ip + """,""" + port
    print(str)
    os.system(str)

######## Python Specific ############

def build_SQL_suggestion_df(df):
    # sug_df = pd.DataFrame(columns = ['column_name','dtype','max_count'])
    sug_df = pd.DataFrame(columns = ['column_name','dtype'])
    for cn in list(df):
        col_dtype = str(df[cn][df[cn] != ' '].dtype)
        # max_count = len(max(df[cn].astype(str)))
        # sug_list = [cn, col_dtype, max_count]
        sug_list = [cn, col_dtype]
        sug_df.loc[len(sug_df)] = sug_list
    return sug_df
# sdf = build_SQL_suggestion_df(df)


def SQL_suggestion_formatter(sdf, tableName, FG = 'Primary', DB = 'Opedia', server = 'Rainer'):

    sdf['null_status'] = 'NULL,'
    sdf.loc[sdf['column_name'] == 'time','dtype'] = '[datetime]'
    sdf.loc[sdf['column_name'] == 'time','null_status'] = 'NOT NULL,'
    sdf.loc[sdf['column_name'] == 'lat','null_status'] = 'NOT NULL,'
    sdf.loc[sdf['column_name'] == 'lon','null_status'] = 'NOT NULL,'
    if 'depth' in list(sdf['column_name']):
        sdf.loc[sdf['column_name'] == 'depth','null_status'] = 'NOT NULL,'
    sdf['null_status'].iloc[-1] = sdf['null_status'].iloc[-1].replace(',','')
    sdf['column_name'] = '[' + sdf['column_name'].astype(str) + ']'
    sdf['dtype'] = sdf['dtype'].replace('object','[float]')
    sdf['dtype'] = sdf['dtype'].replace('float64','[float]')
    sdf['dtype'] = sdf['dtype'].replace('int64','[int]')

    var_string = sdf.to_string(header=False,index=False)


    """ Print table as SQL format """
    SQL_str = """
    USE [{}]

    SET ANSI_NULLS ON
    GO

    SET QUOTED_IDENTIFIER ON
    GO

    CREATE TABLE [dbo].[{}](

    {}


    ) ON [{}]

    GO""".format(
    DB
    ,tableName
    ,var_string
    ,FG
    )
    print(SQL_str)
    input("Please check and modify SQL table if needed, then press to continue validator...")

    return SQL_str

# asdf = SQL_suggestion_formatter(suggested_df=sdf, tableName='tblSeaglider_148_Mission_15')
