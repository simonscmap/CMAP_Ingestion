import pandas as pd
from pandas._testing import assert_frame_equal
import sys
import numpy as np

sys.path.append("../src")
import common as cmn


def test_strip_whitespace_headers():
    test_df = pd.DataFrame({" prefix_space": [""], "suffix_space     ": [""]})
    expected_df = pd.DataFrame({"prefix_space": [""], "suffix_space": [""]})
    func_df = cmn.strip_whitespace_headers(test_df)
    assert list(func_df) == list(expected_df), "Whitespace strip test failed."


def test_nanToNA():
    test_df = pd.DataFrame({"no_nans": ["1", 3], "nans": ["2", np.nan]})
    expected_df = pd.DataFrame({"no_nans": ["1", 3], "nans": ["2", ""]})
    func_df = cmn.nanToNA(test_df)
    assert_frame_equal(func_df, expected_df, obj="nan to None test failed")


def test_lowercase_List():
    test_list = ["Asdf", "SeaGlider", "CMAP"]
    expected_test_list = ["asdf", "seaglider", "cmap"]
    func_list = cmn.lowercase_List(test_list)
    assert func_list == expected_test_list, "lowercase_List test failed"


def test_getColBounds():
    test_df = pd.DataFrame({"col_1": [2, "", 4, 8]})
    expected_min_single, expected_max_single = [2], [8]
    expected_min_mult, expected_max_mult = [2, 2, 2, 2, 2], [8, 8, 8, 8, 8]
    func_min_single, func_max_single = cmn.getColBounds(test_df, "col_1")
    func_min_mult, func_max_mult = cmn.getColBounds(test_df, "col_1", 5)
    assert expected_min_single == func_min_single, """getColBounds min single failed"""
    assert expected_max_single == func_max_single, """getColBounds max single failed"""
    assert expected_min_mult == func_min_mult, """getColBounds min mult failed"""
    assert expected_max_mult == func_max_mult, """getColBounds max mult failed"""


def test_vault_struct_retrieval():
    cruise_path_func = cmn.vault_struct_retrieval("cruise")
    float_path_func = cmn.vault_struct_retrieval("float")
    station_path_func = cmn.vault_struct_retrieval("station")
    satellite_path_func = cmn.vault_struct_retrieval("satellite")
    model_path_func = cmn.vault_struct_retrieval("model")
    assimilation_path_func = cmn.vault_struct_retrieval("assimilation")

    assert (
        cruise_path_func
        == "/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/cruise/"
    ), "cruise vault path test failed"
    assert (
        float_path_func
        == "/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/float/"
    ), "float vault path test failed"
    assert (
        station_path_func
        == "/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/station/"
    ), "station vault path test failed"
    assert (
        satellite_path_func
        == "/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/remote/satellite/"
    ), "satellite vault path test failed"
    assert (
        model_path_func
        == "/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/model/"
    ), "model vault path test failed"
    assert (
        assimilation_path_func
        == "/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/assimilation/"
    ), "assimilation vault path test failed"


def test_getDatasetID_DS_Name():
    input_ds_name = "Flombaum"
    ds_ID_func = cmn.getDatasetID_DS_Name(input_ds_name)
    ds_ID_expected = 83
    assert ds_ID_func == ds_ID_expected, "test get datasetID from dataset name failed."


def test_getDatasetID_Tbl_Name():
    input_tblName = "tblFlombaum"
    ds_ID_func = cmn.getDatasetID_Tbl_Name(input_tblName)
    ds_ID_expected = 83
    assert ds_ID_func == ds_ID_expected, "test get datasetID from table name failed."


def test_getKeywordsIDDataset():
    input_DS_id = 83
    keyword_id_list_func = cmn.getKeywordsIDDataset(input_DS_id)
    keyword_id_list_expected = [1126, 1127]
    assert (
        keyword_id_list_func == keyword_id_list_expected
    ), "test get keywords from datasetID failed."


def test_getTableName_Dtypes():
    input_tblName = "tblFlombaum"
    expected_df = pd.DataFrame(
        {
            "COLUMN_NAME": [
                "time",
                "lat",
                "lon",
                "depth",
                "prochlorococcus_abundance_flombaum",
                "synechococcus_abundance_flombaum",
            ],
            "DATA_TYPE": ["date", "float", "float", "float", "float", "float"],
        }
    )
    func_df = cmn.getTableName_Dtypes(input_tblName)
    assert_frame_equal(func_df, expected_df, obj="get tablename datatypes test failed.")


def test_getCruiseDetails():
    input_cruisename = "KOK1606"
    expected_df = pd.DataFrame(
        columns=[
            "ID",
            "Nickname",
            "Name",
            "Ship_Name",
            "Start_Time",
            "End_Time",
            "Lat_Min",
            "Lat_Max",
            "Lon_Min",
            "Lon_Max",
            "Chief_Name",
        ]
    )
    expected_df.loc[len(expected_df), :] = [
        589,
        "Gradients_1",
        "KOK1606",
        "R/V Kaimikai O Kanaloa",
        "2016-04-20T00:04:37.000Z",
        "2016-05-04T02:33:45.000Z",
        21.4542,
        37.8864,
        -158.3355,
        -157.858,
        "Virginia Armbrust",
    ]
    func_df = cmn.getCruiseDetails(input_cruisename)
    assert_frame_equal(
        func_df, expected_df, check_dtype=False, obj="get cruise details test failed."
    )


def test_findVarID():
    input_datasetID = 83
    input_Short_Name = "prochlorococcus_abundance_flombaum"
    VarID_expected = 1126
    varID_func = cmn.findVarID(input_datasetID, input_Short_Name)
    assert varID_func == VarID_expected, "find variable ID test failed."


def test_verify_cruise_lists():
    test_df = pd.DataFrame(
        {"official_cruise_name(s)": ["KOK1606", "cruise_not_in_database"]}
    )
    expected_list_matched = ["kok1606"]
    expected_list_unmatched = ["cruise_not_in_database"]
    func_match_list, func_unmatched_list = cmn.verify_cruise_lists(test_df)
    assert func_match_list == expected_list_matched, "verify_cruise_lists match failed."
    assert (
        func_unmatched_list == expected_list_unmatched
    ), "verify_cruise_lists unmatched failed."


def test_get_cruise_IDS():
    test_cruise_name_list = ["kok1606", "mgl1704"]
    expected_ID_list = [589, 593]
    func_ID_list = cmn.get_cruise_IDS(test_cruise_name_list)
    assert func_ID_list == expected_ID_list, "Get cruise IDs test failed."
