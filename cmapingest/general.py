import sys
import os
import glob
import pandas as pd

sys.path.append("../login/")
import credentials as cr

import pycmap

pycmap.API(cr.api_key)
import argparse


from cmapingest import vault_structure as vs
from cmapingest import transfer
from cmapingest import data
from cmapingest import DB
from cmapingest import metadata
from cmapingest import SQL
from cmapingest import mapping
from cmapingest import stats
from cmapingest import common as cmn


def getBranch_Path(args):
    branch_path = cmn.vault_struct_retrieval(args.branch)
    return branch_path


def splitExcel(staging_filename, metadata_filename):
    transfer.single_file_split(staging_filename, metadata_filename)


def staging_to_vault(staging_filename, branch, tableName, remove_file_flag=False):
    transfer.staging_to_vault(staging_filename, branch, tableName, remove_file_flag)


def importDataMemory(branch, tableName, process_level):
    data_file_name = data.fetch_single_datafile(branch, tableName, process_level)
    data_df = data.read_csv(data_file_name)
    dataset_metadata_df, variable_metadata_df = metadata.import_metadata(
        branch, tableName
    )
    data_dict = {
        "data_df": data_df,
        "dataset_metadata_df": dataset_metadata_df,
        "variable_metadata_df": variable_metadata_df,
    }
    return data_dict


def SQL_suggestion(data_dict, tableName, branch, server):
    if branch != "model" or branch != "satellite":
        make = "observation"
    else:
        make = branch
    cdt = SQL.build_SQL_suggestion_df(data_dict["data_df"])
    sql_tbl = SQL.SQL_tbl_suggestion_formatter(cdt, tableName)
    sql_index = SQL.SQL_index_suggestion_formatter(data_dict["data_df"], tableName)
    sql_combined_str = sql_tbl["sql_tbl"] + sql_index["sql_index"]
    print(sql_combined_str)
    contYN = input("Do you want to build this table in SQL? " + " ?  [yes/no]: ")
    if contYN.lower() == "yes":
        DB.DB_modify(sql_tbl["sql_tbl"], server)
        DB.DB_modify(sql_index["sql_index"], server)

    else:
        sys.exit()
    SQL.write_SQL_file(sql_combined_str, tableName, make)


def insertData(data_dict, tableName, server):
    data.data_df_to_db(data_dict["data_df"], tableName, server)


def insertMetadata(data_dict, tableName, DOI_link_append, server):
    # metadata.tblDatasets_Insert(data_dict["dataset_metadata_df"], tableName, server)
    # metadata.tblDataset_References_Insert(
    #     data_dict["dataset_metadata_df"], DOI_link_append, server
    # )
    # metadata.tblVariables_Insert(
    #     data_dict["data_df"],
    #     data_dict["dataset_metadata_df"],
    #     data_dict["variable_metadata_df"],
    #     tableName,
    #     process_level="REP",
    #     CRS="CRS",
    #     server=server,
    # )
    metadata.tblKeywords_Insert(
        data_dict["variable_metadata_df"],
        data_dict["dataset_metadata_df"],
        tableName,
        server,
    )
    # metadata.ocean_region_classification(
    #     data_dict["data_df"],
    #     data_dict["dataset_metadata_df"]["dataset_short_name"].iloc[0],
    #     server,
    # )
    # if data_dict["dataset_metadata_df"]["cruise_names"].dropna().empty == False:
    #     metadata.tblDataset_Cruises_Insert(
    #         data_dict["data_df"], data_dict["dataset_metadata_df"], server
    #     )


###   TESTING SUITE   ###
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️ #
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️ #
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️ #
#########################


def insert_small_stats(data_dict, tableName, server):
    stats.updateStats_Small(tableName, server, data_dict["data_df"])


def insert_large_stats(tableName, branch, server):
    stats_df = stats.aggregate_large_stats(branch, tableName)
    stats.update_stats_large(tableName, stats_df, server)


def createIcon(data_dict, tableName):
    mapping.folium_map(data_dict["data_df"], tableName)


def push_icon():
    os.chdir(vs.static)
    os.system('git add . && git commit -m "add mission icons to git repo" && git push')


def full_ingestion(args):
    print("Full Ingestion")
    # splitExcel(args.staging_filename, args.metadata_filename)
    # staging_to_vault(
    #     args.staging_filename,
    #     getBranch_Path(args),
    #     args.tableName,
    #     remove_file_flag=True,
    # )
    data_dict = data.importDataMemory(args.branch, args.tableName, args.process_level)
    # SQL_suggestion(data_dict, args.tableName, args.branch, args.Server)
    # insertData(data_dict, args.tableName, args.Server)
    insertMetadata(data_dict, args.tableName, args.DOI_link_append, args.Server)
    # insert_small_stats(data_dict, args.tableName, args.Server)
    # if args.Server == "Rainier":
    #     createIcon(data_dict, args.tableName)
    #     push_icon()


def append_ingestion(args):
    """The Append Ingestion function is for appending data onto a table. Example: Extending satellite or model datasets in time"""
    print("append Ingestion")
    # start_date = input("please input start_date in YYYY-mm-dd")
    # end_date = input("please input end_date in YYYY-mm-dd")
    # dir_path = vs.satellite + 'tblModis_PAR/rep/'
    base_path = (
        cmn.vault_struct_retrieval(args.branch)
        + args.tableName
        + "/"
        + args.process_level
        + "/"
    )
    flist = glob.glob(base_path + "*.parquet")
    # startdate = '2010002'
    # enddate = '201030'
    startdate = "2010336"
    enddate = "2010366"
    flist_base = [os.path.basename(filename) for filename in flist]
    files_in_range = []
    for i in flist_base:
        strpted_time = i.replace(".L3m_DAY_PAR_par_9km.parquet", "").replace("A", "")
        zfill_time = strpted_time[:4] + strpted_time[4:].zfill(3)
        fdate = pd.to_datetime(zfill_time, format="%Y%j")
        if (
            pd.to_datetime(startdate, format="%Y%j")
            <= fdate
            <= pd.to_datetime(enddate, format="%Y%j")
        ):
            files_in_range.append(i)
    for selfile in files_in_range:
        df = pd.read_parquet(base_path + selfile)
        data.data_df_to_db(df, args.tableName, args.Server, clean_data_df_flag=False)
        print("processed: " + selfile)

    # print(flist[0], args.tableName, args.Server)
    # wait_test = input("""Do you want to continue, [y/n]: """)
    # if wait_test == "y":
    #     adll_flist = flist[1:]
    #     for adlfile in adll_flist:
    #         print(adlfile)
    #         # data.data_df_to_db(adlfile, args.tableName, args.server)

    # #     pass
    # else:
    #     print("exiting..")
    #     sys.exit()

    # """
    # 1. input start date and end date as input()
    # 2. gather all files in range of start_end
    # 3. user continue - > does this range look right?
    # 4. BCP data ingestion
    # 5. testing suite for that day
    # 6.if OK, user input, Finish bcp all files in range
    # 7. LARGE STATS TABLE ID: once tables are written to hdf5 or parquet, read all(in range) in vaex, df.describe().to_csv("stats_table_range_dates)
    # 8. update stats
    # 9. final test on random date/spatial range in bounds

    # """
    # pass


def partial_ingestion(args):
    print("Partial Ingestion")
    pass


def main():
    parser = argparse.ArgumentParser(description="Ingestion datasets into CMAP")

    parser.add_argument(
        "tableName", type=str, help="Desired SQL and Vault Table Name. Ex: tblSeaFlow"
    )
    parser.add_argument(
        "branch",
        type=str,
        help="Branch where dataset should be placed in Vault. Ex's: cruise, float, station, satellite, model, assimilation.",
    )
    parser.add_argument(
        "staging_filename",
        type=str,
        help="Filename from staging area. Ex: 'SeaFlow_ScientificData_2019-09-18.csv'",
    )
    parser.add_argument("-p", "--process_level", nargs="?", default="rep")
    parser.add_argument(
        "-m", "--metadata_filename", nargs="?",
    )
    parser.add_argument(
        "-d",
        "--DOI_link_append",
        help="DOI string to append to reference_list",
        nargs="?",
    )
    parser.add_argument("-P", "--Partial_Ingestion", nargs="?", const=True)

    parser.add_argument("-A", "--Append_Ingestion", nargs="?", const=True)

    parser.add_argument(
        "-S",
        "--Server",
        help="Server choice: Rainier, Mariana, Beast",
        nargs="?",
        default="Rainier",
    )

    args = parser.parse_args()

    if args.Partial_Ingestion:
        partial_ingestion(args)

    elif args.Append_Ingestion:
        append_ingestion(args)
    else:
        full_ingestion(args)


if __name__ == main():
    main()
