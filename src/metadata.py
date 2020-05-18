import sys

sys.path.append("../conf/login/")
import credentials as cr
import common as cmn
import DB
import glob
import pandas as pd
import pycmap

api = pycmap.API(token=cr.api_key)


def ID_Var_Map(series_to_map, res_col, tableName):
    api = pycmap.API()
    query = """SELECT * FROM """ + tableName
    call = DB.DB_query(query)
    series = series_to_map.astype(str).str.lower()
    sdict = dict(zip(call[res_col].str.lower(), call.ID))
    mapped_series = series.map(sdict)
    mapped_series = list(cmn.nanToNA(mapped_series))
    return mapped_series


def import_metadata(branch, tableName):
    branch_path = cmn.vault_struct_retrieval(branch)
    ds_meta_list = glob.glob(
        branch_path + tableName + "/metadata/" + "*dataset_metadata*"
    )
    vars_meta_list = glob.glob(
        branch_path + tableName + "/metadata/" + "*vars_metadata*"
    )
    dataset_metadata_df = pd.read_csv(ds_meta_list[0], sep=",")
    vars_metadata_df = pd.read_csv(vars_meta_list[0], sep=",")

    dataset_metadata_df = cmn.strip_whitespace_headers(dataset_metadata_df)
    vars_metadata_df = cmn.strip_whitespace_headers(vars_metadata_df)
    return dataset_metadata_df, vars_metadata_df


def tblDatasets_Insert(dataset_metadata_df, tableName, server="Rainier"):
    dataset_metadata_df = cmn.nanToNA(dataset_metadata_df)
    Dataset_Name = dataset_metadata_df["dataset_short_name"].iloc[0]
    Dataset_Long_Name = dataset_metadata_df["dataset_long_name"].iloc[0]
    Dataset_Version = dataset_metadata_df["dataset_version"].iloc[0]
    Dataset_Release_Date = dataset_metadata_df["dataset_release_date"].iloc[0]
    Dataset_Make = dataset_metadata_df["dataset_make"].iloc[0]
    Data_Source = dataset_metadata_df["dataset_source"].iloc[0]
    Distributor = dataset_metadata_df["dataset_distributor"].iloc[0]
    Acknowledgement = dataset_metadata_df["dataset_acknowledgement"].iloc[0]
    Contact_Email = dataset_metadata_df["contact_email"].iloc[0]
    Dataset_History = dataset_metadata_df["dataset_history"].iloc[0]
    Description = dataset_metadata_df["dataset_description"].iloc[0]
    Climatology = dataset_metadata_df["climatology"].iloc[0]
    Db = "Opedia"
    # Temps
    Variables = ""
    Doc_URL = ""
    Icon_URL = "https://raw.githubusercontent.com/simonscmap/CMAP_Ingestion/master/static/mission_icons/{tableName}.svg?sanitize=true".format(
        tableName=tableName
    )

    query = (
        Db,
        Dataset_Name,
        Dataset_Long_Name,
        Variables,
        Data_Source,
        Distributor,
        Description,
        Climatology,
        Acknowledgement,
        Doc_URL,
        Icon_URL,
        Contact_Email,
        Dataset_Version,
        Dataset_Release_Date,
        Dataset_History,
    )
    columnList = "(DB,Dataset_Name,Dataset_Long_Name,Variables,Data_Source,Distributor,Description,Climatology,Acknowledgement,Doc_URL,Icon_URL,Contact_Email,Dataset_Version,Dataset_Release_Date,Dataset_History)"
    DB.lineInsert(server, "[opedia].[dbo].[tblDatasets]", columnList, query)
    print("Metadata inserted into tblDatasets.")


def tblDataset_References_Insert(dataset_metadata_df, server="Rainier"):
    Dataset_Name = dataset_metadata_df["dataset_short_name"].iloc[0]
    IDvar = cmn.getDatasetID_DS_Name(Dataset_Name)
    columnList = "(Dataset_ID, Reference)"
    reference_list = dataset_metadata_df["dataset_references"].to_list()
    for ref in reference_list:
        query = (IDvar, ref)
        DB.lineInsert(
            server, "[opedia].[dbo].[tblDataset_References]", columnList, query
        )
    print("Inserting data into tblDataset_References.")


def tblVariables_Insert(
    data_df,
    dataset_metadata_df,
    variable_metadata_df,
    Table_Name,
    process_level="REP",
    CRS="",
    server="Rainier",
):
    Db_list = len(variable_metadata_df) * ["Opedia"]
    IDvar_list = len(variable_metadata_df) * [
        cmn.getDatasetID_DS_Name(dataset_metadata_df["dataset_short_name"].iloc[0])
    ]
    Table_Name_list = len(variable_metadata_df) * [Table_Name]
    Short_Name_list = variable_metadata_df["var_short_name"].tolist()
    Long_Name_list = variable_metadata_df["var_long_name"].tolist()
    Unit_list = variable_metadata_df["var_unit"].tolist()
    Temporal_Res_ID_list = ID_Var_Map(
        variable_metadata_df["var_temporal_res"],
        "Temporal_Resolution",
        "tblTemporal_Resolutions",
    )
    Spatial_Res_ID_list = ID_Var_Map(
        variable_metadata_df["var_spatial_res"],
        "Spatial_Resolution",
        "tblSpatial_Resolutions",
    )
    Temporal_Coverage_Begin_list, Temporal_Coverage_End_list = cmn.getColBounds(
        data_df, "time", list_multiplier=len(variable_metadata_df)
    )
    Lat_Coverage_Begin_list, Lat_Coverage_End_list = cmn.getColBounds(
        data_df, "lat", list_multiplier=len(variable_metadata_df)
    )
    Lon_Coverage_Begin_list, Lon_Coverage_End_list = cmn.getColBounds(
        data_df, "lon", list_multiplier=len(variable_metadata_df)
    )
    Grid_Mapping_list = [CRS] * len(variable_metadata_df)
    Sensor_ID_list = ID_Var_Map(
        variable_metadata_df["var_sensor"], "Sensor", "tblSensors"
    )
    Make_ID_list = len(variable_metadata_df) * list(
        ID_Var_Map(dataset_metadata_df["dataset_make"], "Make", "tblMakes")
    )
    Process_ID_list = ID_Var_Map(
        pd.Series(len(variable_metadata_df) * [process_level]),
        "Process_Stage",
        "tblProcess_Stages",
    )
    Study_Domain_ID_list = ID_Var_Map(
        variable_metadata_df["var_discipline"], "Study_Domain", "tblStudy_Domains"
    )
    Comment_list = cmn.nanToNA(variable_metadata_df["var_comment"]).to_list()
    Visualize_list = cmn.nanToNA(variable_metadata_df["visualize"]).tolist()
    Data_Type_list = cmn.getTableName_Dtypes(Table_Name)["DATA_TYPE"].tolist()
    columnList = "(DB, Dataset_ID, Table_Name, Short_Name, Long_Name, Unit, Temporal_Res_ID, Spatial_Res_ID, Temporal_Coverage_Begin, Temporal_Coverage_End, Lat_Coverage_Begin, Lat_Coverage_End, Lon_Coverage_Begin, Lon_Coverage_End, Grid_Mapping, Make_ID, Sensor_ID, Process_ID, Study_Domain_ID, Comment, Visualize, Data_Type)"

    for (
        Db,
        IDvar,
        Table_Name,
        Short_Name,
        Long_Name,
        Unit,
        Temporal_Res_ID,
        Spatial_Res_ID,
        Temporal_Coverage_Begin,
        Temporal_Coverage_End,
        Lat_Coverage_Begin,
        Lat_Coverage_End,
        Lon_Coverage_Begin,
        Lon_Coverage_End,
        Grid_Mapping,
        Make_ID,
        Sensor_ID,
        Process_ID,
        Study_Domain_ID,
        Comment,
        Visualize,
        Data_Type,
    ) in zip(
        Db_list,
        IDvar_list,
        Table_Name_list,
        Short_Name_list,
        Long_Name_list,
        Unit_list,
        Temporal_Res_ID_list,
        Spatial_Res_ID_list,
        Temporal_Coverage_Begin_list,
        Temporal_Coverage_End_list,
        Lat_Coverage_Begin_list,
        Lat_Coverage_End_list,
        Lon_Coverage_Begin_list,
        Lon_Coverage_End_list,
        Grid_Mapping_list,
        Make_ID_list,
        Sensor_ID_list,
        Process_ID_list,
        Study_Domain_ID_list,
        Comment_list,
        Visualize_list,
        Data_Type_list,
    ):
        query = (
            Db,
            IDvar,
            Table_Name,
            Short_Name,
            Long_Name,
            Unit,
            Temporal_Res_ID,
            Spatial_Res_ID,
            Temporal_Coverage_Begin,
            Temporal_Coverage_End,
            Lat_Coverage_Begin,
            Lat_Coverage_End,
            Lon_Coverage_Begin,
            Lon_Coverage_End,
            Grid_Mapping,
            Make_ID,
            Sensor_ID,
            Process_ID,
            Study_Domain_ID,
            Comment,
            Visualize,
            Data_Type,
        )

        DB.lineInsert(server, "[opedia].[dbo].[tblVariables]", columnList, query)
    print("Inserting data into tblVariables")


def tblKeywords_Insert(
    variable_metadata_df, dataset_metadata_df, Table_Name, server="Rainier"
):
    IDvar = cmn.getDatasetID_DS_Name(dataset_metadata_df["dataset_short_name"].iloc[0])
    for index, row in variable_metadata_df.iterrows():
        VarID = cmn.findVarID(
            IDvar, variable_metadata_df.loc[index, "var_short_name"], server
        )
        keyword_list = (variable_metadata_df.loc[index, "var_keywords"]).split(",")
        for keyword in keyword_list:
            keyword = keyword.lstrip()
            query = (VarID, keyword)
            print(query)
            if len(keyword) > 0:  # won't insert empty values
                try:  # Cannot insert duplicate entries, so skips if duplicate
                    DB.lineInsert(
                        server,
                        "[opedia].[dbo].[tblKeywords]",
                        "(var_ID, keywords)",
                        query,
                    )
                except Exception as e:
                    print(e)


def tblDataset_Cruises_Insert(dataset_metadata_df, server="Rainier"):
    """use pycmap cruise ID to find metatadata..."""
    matched_cruises, unmatched_cruises = cmn.verify_cruise_lists(dataset_metadata_df)
    if unmatched_cruises != []:
        print(
            "The following cruises are not in CMAP: ",
            unmatched_cruises,
            " Please contact the CMAP team to add these cruises.",
        )

    cruise_ID_list = cmn.get_cruise_IDS(matched_cruises)
    dataset_ID = cmn.getDatasetID_DS_Name(
        dataset_metadata_df["dataset_short_name"].iloc[0]
    )
    for cruise_ID in cruise_ID_list:
        query = (dataset_ID, cruise_ID)
        DB.lineInsert(
            server,
            "[opedia].[dbo].[tblDataset_Cruises]",
            "(Dataset_ID, Cruise_ID)",
            query,
        )


def deleteFromtblKeywords(Dataset_ID, server):
    Keyword_ID_list = cmn.getKeywordsIDDataset(Dataset_ID)
    Keyword_ID_str = "','".join(str(key) for key in Keyword_ID_list)
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblKeywords] WHERE [var_ID] IN ('"""
        + Keyword_ID_str
        + """')"""
    )
    DB.DB_modify(cur_str, server)
    print("tblKeyword entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblDataset_Stats(Dataset_ID, server):
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblDataset_Stats] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_Stats entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblDataset_Cruises(Dataset_ID, server):
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblDataset_Cruises] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_Cruises entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblDataset_References(Dataset_ID, server):
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblDataset_References] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_References entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblVariables(Dataset_ID, server):
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblVariables] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblVariables entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblDatasets(Dataset_ID, server):
    cur_str = """DELETE FROM [Opedia].[dbo].[tblDatasets] WHERE [ID] = """ + str(
        Dataset_ID
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset entries deleted for Dataset_ID: ", Dataset_ID)


def dropTable(tableName, server):
    cur_str = """DROP TABLE """ + tableName
    DB.DB_modify(cur_str, server)
    print(tableName, " Removed from DB")


def deleteCatalogTables(tableName, server="Rainier"):
    contYN = input(
        "Are you sure you want to delete all of the catalog tables for "
        + tableName
        + " ?  [yes/no]: "
    )
    Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName)
    if contYN == "yes":
        deleteFromtblKeywords(Dataset_ID, server)
        deleteFromtblDataset_Stats(Dataset_ID, server)
        deleteFromtblDataset_Cruises(Dataset_ID, server)
        deleteFromtblDataset_References(Dataset_ID, server)
        deleteFromtblVariables(Dataset_ID, server)
        deleteFromtblDatasets(Dataset_ID, server)
        dropTable(tableName, server)
    else:
        print("Catalog tables for " + datasetName + " not deleted")


# tblAMT13_Chisholm, csal_ppt_AMT13
def removeKeywords(keywords_list, var_short_name_list, tableName, server="Rainier"):
    keywords_list = cmn.lowercase_List(keywords_list)
    """Removes keyword from specific variable in table"""
    keyword_IDs = cmn.getKeywordIDsTableNameVarName(tableName, var_short_name_list)
    cur_str = (
        """DELETE FROM [Opedia].[dbo].[tblKeywords] WHERE [var_ID] IN """
        + keyword_IDs
        + """ AND LOWER([keywords]) IN """
        + str(tuple(keywords_list))
    )
    DB.DB_modify(cur_str, server)
    print(
        "tblKeyword entries deleted for: ",
        keywords_list,
        var_short_name_list,
        tableName,
    )


#
# keywords_list =  ['abundance',
# 'bacteria',
# 'cyanobacteria',
# 'ecotype',
# 'microbe',
# 'microorganism',
# 'photosynthesis',
# 'picoplankton',
# 'plankton',
# 'Pro',
# 'Pro strain',
# 'Prochlorococcus',
# 'Syn',
# 'Synechococcus',
# 'time series']
# var_short_name_list = ['csal_cmore',
# 'sigma_cmore',
# 'site_Chisholm',
# 'temp_C_cmore',
# 'time_quality_Chisholm']
# tableName = 'tblHOT_BATS_Prochlorococcus_Abundance'
# removeKeywords(keywords_list,var_short_name_list,tableName)
