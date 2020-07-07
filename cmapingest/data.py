import sys
import os
import pandas as pd
import numpy as np
import glob
import vault_structure as vs
import DB
import common as cmn
import metadata


def removeMissings(df, cols):
    """Removes missing rows for all columns provided

    Parameters
    ----------
    df : Pandas DataFrame
        The dataframe to be modified
    cols :  list
        List of column names

    Returns
    -------
    df
        Pandas DataFrame with missing rows removed
    """
    for col in cols:
        df[col].replace("", np.nan, inplace=True)
        df.dropna(subset=[col], inplace=True)
    return df


def format_time_col(df, time_col, format="%Y-%m-%d %H:%M:%S"):
    """Formats dataframe timecolumn

    Parameters
    ----------
    df : Pandas DataFrame
        The dataframe to be modified
    time_col : str
        Name of the time column. ex: 'time'
    format : str, optional, default = %Y-%m-%d %H:%M:%S

    Returns
    -------
    df
        Pandas DataFrame with time col formatted
    """
    df["time"] = pd.to_datetime(df[time_col].astype(str), errors="coerce")
    df["time"].dt.strftime(format)
    return df


def sort_values(df, cols):
    """Sorts dataframe cols

    Parameters
    ----------
    df : Pandas DataFrame
        The dataframe to be modified
    cols : list
        List of column name strings

    Returns
    -------
    df
        Pandas DataFrame with input cols sorts in ASC order.
    """
    df = df.sort_values(cols, ascending=[True] * len(cols))
    return df


def ST_columns(df):
    """Returns SpaceTime related columns in a dataset as a list"""
    df_cols = cmn.lowercase_List(list(df))
    ST_vars = [i for i in df_cols if i in ["time", "lat", "lon", "depth"]]
    return ST_vars


##############   Data Import    ############


def clean_data_df(df):
    df = cmn.strip_whitespace_headers(df)
    df = cmn.nanToNA(df)
    df = format_time_col(df, "time")
    df = removeMissings(df, ST_columns(df))
    df = sort_values(df, ST_columns(df))
    return df


def read_csv(path_and_filename, delim=","):
    """Imports csv into pandas DataFrame"""
    df = pd.read_csv(path_and_filename, sep=delim, parse_dates=["time"])
    return df


def fetch_single_datafile(branch, tableName, file_ext=".csv", process_level="REP"):
    """Finds first file in glob with input path to vault structure. Returns path_filename """
    vault_path = cmn.vault_struct_retrieval(branch)
    flist = glob.glob(
        vault_path + tableName + "/" + process_level.lower() + "/" + "*" + file_ext
    )[0]
    return flist


def importDataMemory(branch, tableName):
    data_file_name = fetch_single_datafile(branch, tableName)
    data_df = read_csv(data_file_name)
    data_df.rename(columns={"latitude": "lat", "longitude": "lon"}, inplace=True)
    dataset_metadata_df, variable_metadata_df = metadata.import_metadata(
        branch, tableName
    )
    data_dict = {
        "data_df": data_df,
        "dataset_metadata_df": dataset_metadata_df,
        "variable_metadata_df": variable_metadata_df,
    }
    return data_dict


##############   Data Insert    ############


def data_df_to_db(df, tableName, clean_data_df=True, server="Rainier"):
    """Inserts dataframe into SQL tbl"""
    if clean_data_df == True:
        clean_data_df(df)
    temp_file_path = vs.BCP + tableName + ".csv"
    df.to_csv(temp_file_path, index=False, header=False)
    DB.toSQLbcp(temp_file_path, tableName, server)
    print(temp_file_path)
    # os.remove(temp_file_path)