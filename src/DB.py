import sys
import os

sys.path.append("../conf/login/")
import credentials as cr
import pyodbc
import pandas.io.sql as sql
import platform
import pandas as pd
import pycmap

######## DB Specific ############


def DB_query(query):
    api = pycmap.API()
    query_result = api.query(query)
    return query_result


def DB_modify(cmnd, server="Rainier"):
    conn, cursor = dbConnect(server)
    conn.autocommit = True

    cursor.execute(cmnd)
    # conn.commit()


def dbRead(query, server="Rainier"):
    conn, cursor = dbConnect(server)
    df = sql.read_sql(query, conn)
    conn.close()
    return df


def server_select_credentials(server):

    if server == "Rainier":
        usr = cr.usr_rainier
        psw = cr.psw_rainier
        ip = cr.ip_rainier
        port = cr.port_rainier
    else:
        usr = cr.usr_beast
        psw = cr.psw_beast
        ip = cr.ip_beast
        port = cr.port_beast

    return usr, psw, ip, port


def dbConnect(server, db="Opedia", TDS_Version="7.3"):
    usr, psw, ip, port = server_select_credentials(server)

    server = ip + "," + port

    if platform.system().lower().find("windows") != -1:
        conn = pyodbc.connect(
            "DRIVER={SQL Server};SERVER="
            + server
            + ";DATABASE="
            + db
            + ";Uid="
            + usr
            + ";Pwd="
            + psw
        )
    elif platform.system().lower().find("darwin") != -1:
        conn = pyodbc.connect(
            "DRIVER=/usr/local/lib/libtdsodbc.so;SERVER="
            + server
            + ";DATABASE="
            + db
            + ";Uid="
            + usr
            + ";Pwd="
            + psw
        )
    elif platform.system().lower().find("linux") != -1:
        conn = pyodbc.connect(
            DRIVER="/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so",
            TDS_Version=TDS_Version,
            server=ip,
            port=port,
            uid=usr,
            pwd=psw,
        )
    cursor = conn.cursor()

    return conn, cursor


def lineInsert(server, tableName, columnList, query):
    insertQuery = """INSERT INTO {} {} VALUES {} """.format(
        tableName, columnList, query
    )
    conn, cursor = dbConnect(server)
    cursor.execute(insertQuery)
    conn.commit()


def toSQLbcp(export_path, tableName, server):

    usr, psw, ip, port = server_select_credentials(server)
    str = (
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
    os.system(str)
