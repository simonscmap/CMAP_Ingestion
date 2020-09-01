import sys
import os
from cmapingest import credentials as cr

import pandas as pd
import numpy as np
import pycmap

from cmapingest import common as cmn
from cmapingest import DB
from cmapingest import transfer


pycmap.API(cr.api_key)

api = pycmap.API()


def updateStatsTable(ID, json_str, server):
    conn, cursor = DB.dbConnect(server)
    deleteQuery = """DELETE FROM tblDataset_Stats WHERE Dataset_ID = '{}'""".format(ID)
    insertQuery = """INSERT INTO tblDataset_Stats (Dataset_ID, JSON_stats) VALUES('{}','{}')""".format(
        ID, json_str
    )
    try:
        DB.DB_modify(deleteQuery, server)
        DB.DB_modify(insertQuery, server)

    except Exception as e:
        print(e)


def updateStats_Small(tableName, server, data_df=None):
    if data_df is not None:
        data_df = data_df
    else:
        query = "SELECT * FROM {tableName}".format(tableName=tableName)
        data_df = DB.dbRead(query, server)
    Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName)
    stats_df = data_df.describe()
    min_max_df = pd.DataFrame(
        {"time": [data_df["time"].min(), data_df["time"].max()]}, index=["min", "max"]
    )
    df = pd.concat([stats_df, min_max_df], axis=1, sort=True)
    json_str = df.to_json(date_format="iso")
    sql_df = pd.DataFrame({"Dataset_ID": [Dataset_ID], "JSON": [json_str]})
    updateStatsTable(Dataset_ID, json_str, server)
    print("Updated stats for " + tableName)


def buildLarge_Stats(df, datetime_slice, tableName, branch, transfer_flag="dropbox"):
    """Input is dataframe slice (daily, 8 day, monthly etc.) of a dataset that is split into multiple files"""
    df_stats = df.describe()
    df_stats.insert(loc=0, column="time", value="")
    df_stats.at["count", "time"] = len(df["time"])
    df_stats.at["min", "time"] = min(df["time"])
    df_stats.at["max", "time"] = max(df["time"])
    branch_path = cmn.vault_struct_retrieval(branch)

    if transfer_flag == "dropbox":
        df_stats.to_csv("stats_temp.csv")
        transfer.dropbox_file_transfer(
            "stats_temp.csv",
            branch_path.split("Vault")[1]
            + tableName
            + "/stats/"
            + datetime_slice
            + "_summary_stats.csv",
        )
        os.remove("stats_temp.csv")
    else:
        df_stats.to_csv(
            branch_path + tableName + "/stats/" + datetime_slice + "_summary_stats.csv"
        )
    print(
        "stats built for :"
        + tableName
        + "  in branch: "
        + branch
        + " for date: "
        + datetime_slice
    )
