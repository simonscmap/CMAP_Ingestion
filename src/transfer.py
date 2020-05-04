import os
import sys
sys.path.append("../conf/")
import vault_structure as vs
import shutil
import pandas as pd
import requests


requests_Download(DOI,filename,table, process_level = 'REP'):
    r = requests.get(DOI, stream=True)
    with open(vs.staging + 'combined/' + filename,'wb') as f:
        f.write(r.content)
def Zenodo_DOI_to_vault(DOI, tableName, process_level = 'REP'):
    if filename.astype(str).contains('.csv'):
        filename = DOI.astype(str).split('.csv')[0].rsplit('/',1)[1] + '.csv'
    elif filename.astype(str).contains('.xlsx'):
        filename = DOI.astype(str).split('.xlsx')[0].rsplit('/',1)[1] + '.xlsx'

def staging_to_vault(filename,make_tableName, process_level = 'REP', remove_file_flag=True):
    """
    Transfers a file from staging to vault rep or nrt.
    removes file from staging on successful transfer

    Parameters
    ----------
    filename : string
        Filename and extension to be transfered.
    make_tableName : string
        Vault organization and tableName: ex: vs.cruise + 'tblCruise_CruiseNN_ctd'
    process_level : str, default REP, optional
        Place the data in the REP or the NRT
    remove_file_flag : bool, default True, optional
        Flag option for removing input file from staging

    """
    nrt_tree, rep_tree, metadata_tree, stats_tree, doc_tree, code_tree  = vs.leafStruc(make_tableName)
    base_filename = os.path.splitext(os.path.basename(filename))[0]

    data_fname = vs.staging + 'data/' + base_filename + '_data.csv'
    dataset_metadata_fname = vs.staging + 'metadata/' + base_filename + '_dataset_metadata.csv'
    vars_metadata_fname = vs.staging + 'metadata/' + base_filename + '_vars_metadata.csv'

    if process_level.lower() == 'nrt':
        shutil.copyfile(data_fname, nrt_tree + base_filename + '_data.csv')
    else:
        shutil.copyfile(data_fname, rep_tree + base_filename + '_data.csv')

    shutil.copyfile(dataset_metadata_fname, metadata_tree + base_filename + '_dataset_metadata.csv')
    shutil.copyfile(vars_metadata_fname, metadata_tree + base_filename + '_vars_metadata.csv')

    if remove_file_flag == True:
        os.remove(data_fname)
        os.remove(dataset_metadata_fname)
        os.remove(vars_metadata_fname)

def single_file_split(filename):
    """
    Splits an excel file containing data, dataset_metadata and vars_metadata sheets
    into three seperate files in the staging file strucutre.

    Parameters
    ----------
    filename : string
        Filename and extension to be split.

    """

    base_filename = os.path.splitext(os.path.basename(filename))[0]
    data_df = pd.read_excel(vs.combined + filename, sheet_name = 0)
    data_df.to_csv(vs.data + base_filename + '_data.csv', sep =',', index=False)
    dataset_metadata_df = pd.read_excel(vs.combined + filename, sheet_name = 1)
    dataset_metadata_df.to_csv(vs.metadata + base_filename + '_dataset_metadata.csv', sep =',', index=False)
    vars_metadata_df = pd.read_excel(vs.combined + filename, sheet_name = 2)
    vars_metadata_df.to_csv(vs.metadata + base_filename + '_vars_metadata.csv', sep =',', index=False)
