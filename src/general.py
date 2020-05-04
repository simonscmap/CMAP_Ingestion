import sys
import os
sys.path.append('../conf/')
import vault_structure as vs
import transfer
import data
import DB
import metadata
import SQL
import mapping
import stats
import common as cmn
import pandas as pd
import pycmap
import argparse





"""

Start function -- help will display all args and optional args
{functions and use:
1. Split file in staging
2. Transfer from staging to vault
3. Import data, suggest SQL, pause. continue.
3. tblDatasets -- flag for LARGE partial ingest
4. tblDataset_References
5. tblVariables
6. tblKeywords
7. tblDatasets_Stats
8. tblDatasets_Cruises (optional and if data is cruise -- flag)
---Testing Suite ----
9. If not pass, rollback
10. If --flag LARGE partial, continue ingestion. Rebuild stats. Update tblVars ST bounds
11. Create Index (auto suggest?)


What inputs
- filename - (in staging)
    -filename in vault should reflect staging
- tableName
- vs branch
-------------
optional_args:
-cruise name

#todo:
-mapping zoom feature
-SQL index suggestion print, post testing OK?
-large dataset ingestion flag, ingestion etc.
-Command line args

"""


parser = argparse.ArgumentParser(description = 'Ingestion datasets into CMAP')
parser.add_argument("staging filename",type=str,help = 'filename from staging area')
parser.add_argument("tableName",type=str)
parser.parse_args()

#inputs for argparse
staging_filename = sys.argv[1]
tableName = sys.argv[2]

# staging_filename = 'sg148m15d001-762_cmap.xlsx'
# tableName = 'tblSeaglider_148_Mission_15'

# struct = cmn.find_File_Path_guess_tree(filename)

###(opt) transfer ###
def splitExcel(staging_filename):
    transfer.single_file_split(staging_filename)

def staging_to_vault(staging_filename,path=vs.cruise + tableName, remove_file_flag=False):
    transfer.staging_to_vault(staging_filename,path, remove_file_flag)


def importDataMemory(branch, tableName):
    data_df = data.import_single_data(vs.cruise + tableName)
    dataset_metadata_df,variable_metadata_df = metadata.import_metadata(vs.cruise + tableName)
    data_dict = {'data_df':data_df, 'dataset_metadata_df':dataset_metadata_df,'variable_metadata_df':variable_metadata_df}
    return data_dict


def SQL_suggestion(data_dict,tableName,make ='observation'):
    cdt = SQL.build_SQL_suggestion_df(data_df)
    sql_tbl = SQL.SQL_tbl_suggestion_formatter(cdt, tableName)
    sql_index = SQL.SQL_index_suggestion_formatter(data_df, tableName)
    sql_combined_str = sql_tbl['sql_tbl'] + sql_index['sql_index']
    SQL.write_SQL_file(sql_combined_str,tableName,make)

def insertData(data_dict,tableName,server = 'Rainier'):
    data.data_df_to_db(data_dict['data_df'], tableName,server)


def insertMetadata(data_dict,tableName,server = 'Rainier',cruiseName=''):
    metadata.tblDatasets_Insert(data_dict['dataset_metadata_df']) # invalid col name Acknowledgement only in python?
    metadata.tblDataset_References_Insert(data_dict['dataset_metadata_df'])
    metadata.tblVariables_Insert(data_dict['data_df'], data_dict['dataset_metadata_df'],data_dict['variable_metadata_df'], tableName,process_level = 'REP',CRS='',server='Rainier')
    metadata.tblKeywords_Insert(data_dict['variable_metadata_df'],data_dict['dataset_metadata_df'],tableName)
    if cruiseName != '':
        metadata.tblDataset_Cruises_Insert(data_dict['dataset_metadata_df'], cruiseName)


### TESTING SUITE  ###
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️
######################

def insertStats(data_dict,tableName):
    stats.updateStats_Small(tableName, data_dict['data_df'],data_dict['dataset_metadata_df'])

def createIcon(data_dict,tableName):
    mapping.cartopy_sparse_map(data_dict['data_df'],tableName)
