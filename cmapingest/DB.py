import sys
import os

from cmapingest import credentials as cr
import pyodbc
import sqlalchemy
import urllib
import pyodbc
import pandas.io.sql as sql
import platform
import pandas as pd
import pycmap

pycmap.API(cr.api_key)
######## DB Specific ############


def DB_query(query):
    api = pycmap.API()
    query_result = api.query(query)
    return query_result


def DB_modify(cmnd, server):

    try:
        conn, cursor = dbConnect(server)
        conn.autocommit = True
        cursor.execute(cmnd)
        conn.close()
    except Exception as e:
        print(e)
        conn.close()


def dbRead(query, server="Rainier"):
    conn, cursor = dbConnect(server)
    df = sql.read_sql(query, conn)
    conn.close()
    return df


def server_select_credentials(server):
    db_name = "opedia"
    TDS_Version = "7.3"
    if server.lower() == "rainier":
        usr = cr.usr_rainier
        psw = cr.psw_rainier
        ip = cr.ip_rainier
        port = cr.port_rainier
    elif server.lower() == "mariana":
        usr = cr.usr_mariana
        psw = cr.psw_mariana
        ip = cr.ip_mariana
        port = cr.port_mariana
    elif server.lower() == "beast":
        usr = cr.usr_beast
        psw = cr.psw_beast
        ip = cr.ip_beast
        port = cr.port_beast
    else:
        print("invalid server selected. exiting...")
        sys.exit()

    return usr, psw, ip, port, db_name, TDS_Version


def pyodbc_connection_string(server):
    usr, psw, ip, port, db_name, TDS_Version = server_select_credentials(server)
    server = ip + "," + port

    if platform.system().lower().find("windows") != -1:
        driver_str = "{SQL Server}"
    elif platform.system().lower().find("darwin") != -1:
        driver_str = "/usr/local/lib/libtdsodbc.so"
    elif platform.system().lower().find("linux") != -1:
        driver_str = "/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so"

    conn_str = """DRIVER={driver_str};TDS_Version={TDS_Version};SERVER={ip};PORT={port};UID={usr};PWD={psw}""".format(
        driver_str=driver_str,
        TDS_Version=TDS_Version,
        ip=ip,
        port=port,
        usr=usr,
        psw=psw,
    )
    return conn_str


def dbConnect(server):
    conn_str = pyodbc_connection_string(server)
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # conn = pyodbc.connect(
    #     "DRIVER={SQL Server};SERVER="
    #     + server
    #     + ";DATABASE="
    #     + db
    #     + ";Uid="
    #     + usr
    #     + ";Pwd="
    #     + psw
    # )
    # elif platform.system().lower().find("darwin") != -1:
    # conn = pyodbc.connect(
    #     "DRIVER=/usr/local/lib/libtdsodbc.so;SERVER="
    #     + server
    #     + ";DATABASE="
    #     + db
    #     + ";Uid="
    #     + usr
    #     + ";Pwd="
    #     + psw
    # )
    # elif platform.system().lower().find("linux") != -1:
    # conn = pyodbc.connect(
    #     DRIVER="/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so",
    #     TDS_Version=TDS_Version,
    #     server=ip,
    #     port=port,
    #     uid=usr,
    #     pwd=psw,
    #     )

    return conn, cursor


def lineInsert(server, tableName, columnList, query):
    insertQuery = """INSERT INTO {} {} VALUES {} """.format(
        tableName, columnList, query
    )
    # print(insertQuery)
    conn, cursor = dbConnect(server)
    cursor.execute(insertQuery)
    conn.commit()


def urllib_pyodbc_format(conn_str):
    quoted_conn_str = urllib.parse.quote_plus(conn_str)
    return quoted_conn_str


def toSQLpandas(df, tableName, server):
    conn_str = pyodbc_connection_string(server)
    quoted_conn_str = urllib_pyodbc_format(conn_str)
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect={}".format(quoted_conn_str)
    )
    df.to_sql(tableName, con=engine, if_exists="append", index=False, method="multi")


def toSQLbcp(export_path, tableName, server):

    usr, psw, ip, port, db_name = server_select_credentials(server)
    bcp_str = (
        """bcp Opedia.dbo."""
        + tableName
        + """ in """
        + """'"""
        + export_path
        + """'"""
        + """ -e error -c -t, -U  """
        + usr
        + """ -P """
        + psw
        + """ -S """
        + ip
        + ""","""
        + port
    )
    os.system(bcp_str)
