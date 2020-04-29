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
import pandas as pd
import pycmap

#index sql suggestion - write sql suggestion to .sql file in ...
#drop table and metadata functionallity
#argparse


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

###next:
sql table output to DB,
dataset icon output
"""
# vs.leafStruc('tableName')

#inputs for argparse
filename = 'sg148m15d001-762_cmap.xlsx'
tableName = 'tblSeaglider_148_Mission_15'
cruise_name = ''

###(opt) transfer ###
# transfer.single_file_split(filename)
transfer.staging_to_vault(filename,vs.cruise + 'tblSeaglider_148_Mission_15', remove_file_flag=True)

# ### Data mem import ###
#
# data_df = data.import_single_data(vs.cruise + tableName)
# dataset_metadata_df,variable_metadata_df = metadata.import_metadata(vs.cruise + tableName)
#
#
# ### SQL Inserts ###
#
# cdt = SQL.build_SQL_suggestion_df(data_df)
# sql_tbl = SQL.SQL_tbl_suggestion_formatter(cdt, tableName)
# sql_index = SQL.SQL_index_suggestion_formatter(data_df, tableName)
# sql_combined_str = sql_tbl['sql_tbl'] + sql_index['sql_index']
# SQL.write_SQL_file(sql_combined_str,tableName,make='observation')
#
# ### Data Insert###
# data.data_df_to_db(df, tableName,server = 'Rainier')
#
# ### Metadata Inserts ###
#
#
# metadata.tblDatasets_Insert(dataset_metadata_df)
# metadata.tblDataset_References_Insert(dataset_metadata_df)
# metadata.tblVariables_Insert(data_df, dataset_metadata_df,variable_metadata_df, tableName,process_level = 'REP',CRS='',server='Rainier')
# metadata.tblKeywords_Insert(variable_metadata_df,dataset_metadata_df,tableName)
# metadata.tblDataset_Cruises_Insert(dataset_metadata_df, cruiseName)
# ## TESTING SUITE  ##
# #stats
# stats.updateStats_Small(tableName, data_df,dataset_metadata_df)
# #mapping
# mapping.cartopy_sparse_map(data_df,tableName)
