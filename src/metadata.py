import common as cmn
import DB
import glob
import pandas as pd
import pycmap
"""
Tables to insert into:
tblDatasets:





"""
#
# convert temporal_res string to temporal_res ID :
# input col of temporal res, could differ
# Get temporal res ID and String - use as dict to map ID to temporal res input column from metadata
#
# list_temporal_res_cmap = list(api.query('''SELECT [Temporal_Resolution] FROM tblTemporal_Resolutions''').iloc[:,0])
# list_temporal_res_lowercase = [i.lower() for i in list_temporal_res_cmap]
# print(list_temporal_res_lowercase)
# non_matching_vals = list(df[col][~df[col].str.lower().isin(list_temporal_res_lowercase)])

def DB_query(query):
    api = pycmap.API()
    query_result = api.query(query)
    return query_result


def ID_Var_Map(series_to_map,res_col, tableName):
    api = pycmap.API()
    query = '''SELECT * FROM ''' + tableName
    call = DB.DB_query(query)
    series = series_to_map.str.lower()
    sdict = dict(zip(call[res_col].str.lower(), call.ID))
    mapped_series = series.map(sdict)
    mapped_series = cmn.nanToNA(mapped_series)
    return mapped_series

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
        cI.lineInsert(server,'[opedia].[dbo].[tblDataset_References]', columnList, query)
    print('Inserting data into tblDataset_References.')

def tblVariables_Insert(dataset_metadata_df,variable_metadata_df, Table_Name, CRS='',server='Rainier'):
    IDvar = DB.findDatasetID(dataset_metadata_df['dataset_short_name'].iloc[0], server)
    Short_Name = variable_metadata_df['var_short_name'].as_list()
    Long_Name = variable_metadata_df['var_long_name'].as_list()
    Unit = variable_metadata_df['var_unit'].as_list()
    Temporal_Res_ID = ID_Var_Map(variable_metadata_df['var_temporal_res'],'Temporal_Resolution', 'tblTemporal_Resolutions')
    Spatial_Res_ID = ID_Var_Map(variable_metadata_df['var_spatial_res'],'Spatial_Resolution', 'tblSpatial_Resolutions')

    Temporal_Coverage_Begin = 'FILL'
    Temporal_Coverage_End = 'FILL'
    Lat_Coverage_Begin = 'FILL'
    Lat_Coverage_End = 'FILL'
    Lon_Coverage_Begin = 'FILL'
    Lon_Coverage_End = 'FILL'
    Grid_Mapping = [CRS] * len(variable_metadata_df)
    Sensor_ID = ID_Var_Map(variable_metadata_df['var_sensor'],'Sensor', 'tblSensors')
    Process_ID = 'Pull from vault structure lookup? REP/NRT/FOR'
    Study_Domain_ID = ID_Var_Map(variable_metadata_df['var_discipline'],'Study_Domain', 'tblStudy_Domains')
    Comment = variable_metadata_df['var_comment'].as_list()
    Visualize = variable_metadata_df['visualize'].as_list()


"""
[DB],[Dataset_ID], [Table_Name],[Short_Name],[Long_Name],[Unit],[Make_ID],[Sensor_ID]
"""
"""

[Process_ID],[Study_Domain_ID],[Comment],[Visualize],[Data_Type]
"""


def tblVariables(DB_list, Dataset_Name_list, Table_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list, Make_ID_list,Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, comment_list,server):
    Dataset_ID_raw = cI.findID(Dataset_Name_list[0], 'tblDatasets', server)
    dataset_ID_list = [Dataset_ID_raw] * len(DB_list)
    columnList = '(DB, Dataset_ID, Table_Name, Short_Name, Long_Name, Unit, Temporal_Res_ID, Spatial_Res_ID, Temporal_Coverage_Begin, Temporal_Coverage_End, Lat_Coverage_Begin, Lat_Coverage_End, Lon_Coverage_Begin, Lon_Coverage_End, Grid_Mapping, Make_ID, Sensor_ID, Process_ID, Study_Domain_ID, Comment)'
    for DB, dataset_ID, Table_Name, short_name, long_name, unit, temporal_res, spatial_res, Temporal_Coverage_Begin, Temporal_Coverage_End, Lat_Coverage_Begin, Lat_Coverage_End, Lon_Coverage_Begin, Lon_Coverage_End, Grid_Mapping, Make_ID, Sensor_ID, Process_ID, Study_Domain_ID,  comment in zip(DB_list, dataset_ID_list, Table_Name_list, short_name_list, long_name_list, unit_list, temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list, Make_ID_list, Sensor_ID_list, Process_ID_list, Study_Domain_ID_list,  comment_list):
        query = (DB, dataset_ID, Table_Name, short_name, long_name, unit, temporal_res, spatial_res, Temporal_Coverage_Begin, Temporal_Coverage_End, Lat_Coverage_Begin, Lat_Coverage_End, Lon_Coverage_Begin, Lon_Coverage_End, Grid_Mapping, Make_ID, Sensor_ID, Process_ID, Study_Domain_ID,  comment)
        cI.lineInsert(server,'[opedia].[dbo].[tblVariables]', columnList, query)
    print('Inserting data into tblVariables')


# cF.tblDatasets(DB, Dataset_Name, Dataset_Long_Name, Variables, Data_Source, Distributor, Description, Climatology,server)
# cF.tblDataset_References(Dataset_Name, reference_list,server)
# cF.tblVariables(DB_list, Dataset_Name_list, Table_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list, Make_ID_list,Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, comment_list,server)
# cF.tblKeywords(vars_metadata, Dataset_Name,keyword_col,tableName,server)

#
""" new ssf function updates"""
# ssf.buildVarDFSmallTables(tableName,server)
""" add to Datasets_Cruises table """
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(177,589)'
