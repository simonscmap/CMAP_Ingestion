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







# staging_filename = 'g1_fluoro_test.xlsx'
# tableName = 'tblFluoro_Test'


###(opt) transfer ###

def getBranch_Path(args):
    branch_path = cmn.vault_struct_retrieval(args.branch)
    return branch_path


def splitExcel(staging_filename):
    transfer.single_file_split(staging_filename)


def staging_to_vault(staging_filename, branch, tableName, remove_file_flag=False):
    transfer.staging_to_vault(staging_filename,branch, tableName, remove_file_flag)

def importDataMemory(branch, tableName):
    data_file_name = data.fetch_single_datafile(branch,tableName)
    data_df = data.read_csv(data_file_name)
    dataset_metadata_df,variable_metadata_df = metadata.import_metadata(branch, tableName)
    data_dict = {'data_df':data_df, 'dataset_metadata_df':dataset_metadata_df,'variable_metadata_df':variable_metadata_df}
    return data_dict


def SQL_suggestion(data_dict,tableName,branch):
    if branch != 'model' or branch != 'satellite':
        make = 'observation'
    else:
        make = branch
    cdt = SQL.build_SQL_suggestion_df(data_dict['data_df'])
    sql_tbl = SQL.SQL_tbl_suggestion_formatter(cdt, tableName)
    sql_index = SQL.SQL_index_suggestion_formatter(data_dict['data_df'], tableName)
    sql_combined_str = sql_tbl['sql_tbl'] + sql_index['sql_index']
    SQL.write_SQL_file(sql_combined_str,tableName,make)

def insertData(data_dict,tableName,server = 'Rainier'):
    data.data_df_to_db(data_dict['data_df'], tableName,server)


def insertMetadata(data_dict,tableName,server = 'Rainier'):
    metadata.tblDatasets_Insert(data_dict['dataset_metadata_df'],tableName)
    metadata.tblDataset_References_Insert(data_dict['dataset_metadata_df'])
    metadata.tblVariables_Insert(data_dict['data_df'], data_dict['dataset_metadata_df'],data_dict['variable_metadata_df'], tableName,process_level = 'REP',CRS='',server='Rainier')
    metadata.tblKeywords_Insert(data_dict['variable_metadata_df'],data_dict['dataset_metadata_df'],tableName)
    metadata.tblDataset_Cruises_Insert(data_dict['dataset_metadata_df'])


###   TESTING SUITE   ###
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️ #
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️ #
# ⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️⚙️ #
#########################

def insertStats(data_dict,tableName):
    stats.updateStats_Small(tableName, data_dict['data_df'])

def createIcon(data_dict,tableName):
    mapping.cartopy_sparse_map(data_dict['data_df'],tableName,zoom_level='high')



def full_ingestion(args,server):
    print('Full Ingestion')
    #
    # splitExcel(args.staging_filename)
    # single_file_split(filename,metadata_only_split=False)
    # staging_to_vault(args.staging_filename, getBranch_Path(args), args.tableName, remove_file_flag=True)
    data_dict = importDataMemory(args.branch, args.tableName)
    # SQL_suggestion(data_dict,args.tableName,args.branch)
    # insertData(data_dict,args.tableName,server = server)
    # insertMetadata(data_dict,args.tableName,server =server)
    # insertStats(data_dict,args.tableName)
    createIcon(data_dict,args.tableName)

def partial_ingestion():
    print('Partial Ingestion')
    pass


def main():
    parser = argparse.ArgumentParser(description = "Ingestion datasets into CMAP")
    parser.add_argument("staging_filename",type=str,help = "Filename from staging area. Ex: 'SeaFlow_ScientificData_2019-09-18.csv'")
    parser.add_argument("tableName",type=str, help = "Desired SQL and Vault Table Name. Ex: tblSeaFlow")
    parser.add_argument("branch",type=str, help = "Branch where dataset should be placed in Vault. Ex's: cruise, float, station, satellite, model, assimilation.")
    parser.add_argument('-P','--Partial_Ingestion',nargs='?', const=True)
    # parser.add_argument('-C','--cruiseName',nargs='?')

    args = parser.parse_args()

    if args.Partial_Ingestion:
        partial_ingestion()

    else:
        full_ingestion(args,server ='Rainier')



if __name__=="__main__":
    main()




# staging_filename = 'sg148m15d001-762_cmap.xlsx'
# tableName = 'tblSeaglider_148_Mission_15'
