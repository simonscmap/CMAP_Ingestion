import common as cmn
import DB
import glob
import pandas as pd
import pycmap
"""
Tables to insert into:
tblDatasets:





"""

def DB_query(query):
    api = pycmap.API()
    query_result = api.query(query)
    return query_result


def ID_Var_Map(series_to_map,res_col, tableName):
    api = pycmap.API()
    query = '''SELECT * FROM ''' + tableName
    call = DB.DB_query(query)
    series = series_to_map.astype(str).str.lower()
    sdict = dict(zip(call[res_col].str.lower(), call.ID))
    mapped_series = series.map(sdict)
    mapped_series = list(cmn.nanToNA(mapped_series))
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
    IDvar = cmn.findDatasetID(Dataset_Name, server)
    columnList = '(Dataset_ID, Reference)'
    reference_list = dataset_metadata_df['dataset_references'].to_list()
    for ref in reference_list:
        query = (IDvar, ref)
        DB.lineInsert(server,'[opedia].[dbo].[tblDataset_References]', columnList, query)
    print('Inserting data into tblDataset_References.')

def tblVariables_Insert(data_df, dataset_metadata_df,variable_metadata_df, Table_Name, process_level = 'REP',CRS='',server='Rainier'):
    Db_list = len(variable_metadata_df) * ['Opedia']
    IDvar_list = len(variable_metadata_df) * [DB.findDatasetID(dataset_metadata_df['dataset_short_name'].iloc[0], server)]
    Table_Name_list = len(variable_metadata_df) * [Table_Name]
    Short_Name_list = variable_metadata_df['var_short_name'].tolist()
    Long_Name_list = variable_metadata_df['var_long_name'].tolist()
    Unit_list = variable_metadata_df['var_unit'].tolist()
    Temporal_Res_ID_list = ID_Var_Map(variable_metadata_df['var_temporal_res'],'Temporal_Resolution', 'tblTemporal_Resolutions')
    Spatial_Res_ID_list = ID_Var_Map(variable_metadata_df['var_spatial_res'],'Spatial_Resolution', 'tblSpatial_Resolutions')
    Temporal_Coverage_Begin_list, Temporal_Coverage_End_list = cmn.getColBounds(data_df,'time',list_multiplier = len(variable_metadata_df))
    Lat_Coverage_Begin_list, Lat_Coverage_End_list = cmn.getColBounds(data_df,'lat',list_multiplier = len(variable_metadata_df))
    Lon_Coverage_Begin_list, Lon_Coverage_End_list = cmn.getColBounds(data_df,'lon',list_multiplier = len(variable_metadata_df))
    Grid_Mapping_list = [CRS] * len(variable_metadata_df)
    Sensor_ID_list = ID_Var_Map(variable_metadata_df['var_sensor'],'Sensor', 'tblSensors')
    Make_ID_list = len(variable_metadata_df) * list(ID_Var_Map(dataset_metadata_df['dataset_make'],'Make', 'tblMakes'))
    Process_ID_list = ID_Var_Map(pd.Series(len(variable_metadata_df) * [process_level]),'Process_Stage', 'tblProcess_Stages')
    Study_Domain_ID_list = ID_Var_Map(variable_metadata_df['var_discipline'],'Study_Domain', 'tblStudy_Domains')
    Comment_list = cmn.nanToNA(variable_metadata_df['var_comment']).to_list()
    Visualize_list = cmn.nanToNA(variable_metadata_df['visualize']).tolist()

    columnList = '(DB, Dataset_ID, Table_Name, Short_Name, Long_Name, Unit, Temporal_Res_ID, Spatial_Res_ID, Temporal_Coverage_Begin, Temporal_Coverage_End, Lat_Coverage_Begin, Lat_Coverage_End, Lon_Coverage_Begin, Lon_Coverage_End, Grid_Mapping, Make_ID, Sensor_ID, Process_ID, Study_Domain_ID, Comment, Visualize)'


    for Db, IDvar, Table_Name, Short_Name, Long_Name, Unit, Temporal_Res_ID,Spatial_Res_ID, Temporal_Coverage_Begin,Temporal_Coverage_End,Lat_Coverage_Begin,Lat_Coverage_End,Lon_Coverage_Begin,Lon_Coverage_End,Grid_Mapping,Make_ID,Sensor_ID,Process_ID,Study_Domain_ID,Comment, Visualize in zip(
    Db_list,IDvar_list, Table_Name_list, Short_Name_list,Long_Name_list,Unit_list,Temporal_Res_ID_list,
    Spatial_Res_ID_list,Temporal_Coverage_Begin_list,Temporal_Coverage_End_list,Lat_Coverage_Begin_list,Lat_Coverage_End_list,
    Lon_Coverage_Begin_list, Lon_Coverage_End_list,Grid_Mapping_list,Sensor_ID_list,
    Make_ID_list,Process_ID_list,Study_Domain_ID_list,Comment_list,Visualize_list):
        query = (Db, IDvar, Table_Name, Short_Name, Long_Name, Unit, Temporal_Res_ID,Spatial_Res_ID,
        Temporal_Coverage_Begin,Temporal_Coverage_End,Lat_Coverage_Begin,Lat_Coverage_End,
        Lon_Coverage_Begin,Lon_Coverage_End,Grid_Mapping,Make_ID,Sensor_ID,Process_ID,
        Study_Domain_ID,Comment, Visualize)
        # return columnList, query
        DB.lineInsert(server,'[opedia].[dbo].[tblVariables]', columnList, query)
    print('Inserting data into tblVariables')



def findID(datasetName, server, catalogTable):
    """ this function pulls the ID value from the [tblDatasets] for the tblDataset_References to use """
    conn = dc.dbConnect(server)
    cursor = conn.cursor()
    cur_str = """select [ID] FROM [Opedia].[dbo].[tblDatasets] WHERE [Dataset_Name] = '""" + datasetName + """'"""
    cursor.execute(cur_str)
    IDvar = (cursor.fetchone()[0])
    return IDvar
    
def tblKeywords_Insert(variable_metadata_df,dataset_metadata_df,Table_Name,server='Rainier'):
    IDvar = cmn.getDatasetID(dataset_metadata_df['dataset_short_name'].iloc[0])
    for index,row in variable_metadata_df.iterrows():
        VarID = cmn.findVarID(IDvar, variable_metadata_df.loc[index,'var_short_name'],  server)
        keyword_list = (variable_metadata_df.loc[index,'var_keywords']).split(',')
        for keyword in keyword_list:
            keyword = keyword.lstrip()
            query = (VarID, keyword)
            print(query)
            if len(keyword) > 0: # won't insert empty values
                try: # Cannot insert duplicate entries, so skips if duplicate
                    DB.lineInsert(server,'[opedia].[dbo].[tblKeywords]', '(var_ID, keywords)', query)
                except Exception as e:
                    print(e)



# cF.tblDatasets(DB, Dataset_Name, Dataset_Long_Name, Variables, Data_Source, Distributor, Description, Climatology,server)
# cF.tblDataset_References(Dataset_Name, reference_list,server)
# cF.tblVariables(DB_list, Dataset_Name_list, Table_Name_list, short_name_list, long_name_list, unit_list,temporal_res_list, spatial_res_list, Temporal_Coverage_Begin_list, Temporal_Coverage_End_list, Lat_Coverage_Begin_list, Lat_Coverage_End_list, Lon_Coverage_Begin_list, Lon_Coverage_End_list, Grid_Mapping_list, Make_ID_list,Sensor_ID_list, Process_ID_list, Study_Domain_ID_list, comment_list,server)
# cF.tblKeywords(vars_metadata, Dataset_Name,keyword_col,tableName,server)

#
""" new ssf function updates"""
# ssf.buildVarDFSmallTables(tableName,server)
""" add to Datasets_Cruises table """
# cF.lineInsert('tblDataset_Cruises', '(Dataset_ID, Cruise_ID)', '(177,589)'
