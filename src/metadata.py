import common as cmn
import DB
import glob
import pandas as pd
"""
Tables to insert into:
tblDatasets:





"""

def import_metadata(make_tableName):
    ds_meta_list = glob.glob(make_tableName +'/metadata/'+'*dataset_metadata*')
    vars_meta_list = glob.glob(make_tableName +'/metadata/'+'*vars_metadata*')

    dataset_metadata_df = pd.read_csv(ds_meta_list[0],sep=',')
    vars_metadata_df = pd.read_csv(vars_meta_list[0],sep=',')

    dataset_metadata_df = cmn.strip_whitespace_headers(dataset_metadata_df)
    vars_metadata_df = cmn.strip_whitespace_headers(vars_metadata_df)
    return dataset_metadata_df, vars_metadata_df

def tblDatasets_Insert(dataset_metadata_df,server='Rainier'):
        dataset_metadata_df = cmn.nanToNA(dataset_metadata_df)

        Dataset_Name = dataset_metadata_df['dataset_short_name'].iloc[0]
        Dataset_Long_Name = dataset_metadata_df['dataset_long_name'].iloc[0]
        Dataset_Version = dataset_metadata_df['dataset_version'].iloc[0]
        Dataset_Release_Date = dataset_metadata_df['dataset_release_date'].iloc[0]
        Dataset_Make = dataset_metadata_df['dataset_make'].iloc[0]
        Data_Source = dataset_metadata_df['dataset_source'].iloc[0]
        Distributor = dataset_metadata_df['dataset_distributor'].iloc[0]
        Dataset_Acknowledgement = dataset_metadata_df['dataset_acknowledgement'].iloc[0]
        Contact_Email = dataset_metadata_df['contact_email'].iloc[0]
        Dataset_History = dataset_metadata_df['dataset_history'].iloc[0]
        Description = dataset_metadata_df['dataset_description'].iloc[0]
        Climatology = dataset_metadata_df['climatology'].iloc[0]
        Db = 'Opedia'
        #Temps
        Variables = ''
        Doc_URL = ''
        Icon_URL = ''

        query = (Db,Dataset_Name,Dataset_Long_Name,Variables,Data_Source,Distributor,Description,Climatology,Dataset_Acknowledgement,Doc_URL,Icon_URL,Contact_Email,Dataset_Version,Dataset_Release_Date,Dataset_History)
        columnList = '(DB,Dataset_Name,Dataset_Long_Name,Variables,Data_Source,Distributor,Description,Climatology,Acknowledgement,Doc_URL,Icon_URL,Contact_Email,Dataset_Version,Dataset_Release_Date,Dataset_History)'
        DB.lineInsert(server,'[opedia].[dbo].[tblDatasets]', columnList, query)
        print('Metadata inserted into tblDatasets.')

def tblDataset_References_Insert(dataset_metadata_df,server='Rainier'):
    Dataset_Name = dataset_metadata_df['dataset_short_name'].iloc[0]
    IDvar = DB.findDatasetID(Dataset_Name, server)
    columnList = '(Dataset_ID, Reference)'
    reference_list = dataset_metadata_df['dataset_references'].to_list()
    for ref in reference_list:
        query = (IDvar, ref)
        print(query)
    #     cI.lineInsert(server,'[opedia].[dbo].[tblDataset_References]', columnList, query)
    # print('Inserting data into tblDataset_References.')


# cF.tblDatasets(DB, Dataset_Name, Dataset_Long_Name, Variables, Data_Source, Distributor, Description, Climatology,server)
# cF.tblDataset_References(Dataset_Name, reference_list,server)
# cF.tblVariables(DB_list, Dataset_Name_list, Table_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list, Make_ID_list,Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, comment_list,server)
# cF.tblKeywords(vars_metadata, Dataset_Name,keyword_col,tableName,server)

#
""" new ssf function updates"""
# ssf.buildVarDFSmallTables(tableName,server)
""" add to Datasets_Cruises table """
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(177,589)'
