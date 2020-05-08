import pandas as pd
from pandas._testing import assert_frame_equal
import sys
import numpy as np
sys.path.append('../src')
import common as cmn




def test_strip_whitespace_headers():
    test_df = pd.DataFrame({' prefix_space': [''],'suffix_space     ': ['']})
    expected_df = pd.DataFrame({'prefix_space': [''],'suffix_space': ['']})
    func_df = cmn.strip_whitespace_headers(test_df)
    assert list(func_df) == list(expected_df), 'Whitespace strip test failed.'

def test_nanToNA():
    test_df = pd.DataFrame({'no_nans': ['1',3],'nans': ['2',np.nan]})
    expected_df = pd.DataFrame({'no_nans': ['1',3],'nans': ['2','']})
    func_df = cmn.nanToNA(test_df)
    assert_frame_equal(func_df, expected_df,obj = "nan to None test failed")

def test_lowercase_List():
    test_list = ['Asdf','SeaGlider', 'CMAP']
    expected_test_list = ['asdf','seaglider', 'cmap']
    func_list = cmn.lowercase_List(test_list)
    assert func_list == expected_test_list, "lowercase_List test failed"

def test_getColBounds():
    test_df = pd.DataFrame({'col_1': [2,'',4,8]})
    expected_min_single,expected_max_single = [2],[8]
    expected_min_mult,expected_max_mult = [2,2,2,2,2],[8,8,8,8,8]
    func_min_single,func_max_single = cmn.getColBounds(test_df,'col_1')
    func_min_mult,func_max_mult = cmn.getColBounds(test_df,'col_1',5)
    assert expected_min_single == func_min_single, """getColBounds min single failed"""
    assert expected_max_single == func_max_single, """getColBounds max single failed"""
    assert expected_min_mult == func_min_mult, """getColBounds min mult failed"""
    assert expected_max_mult == func_max_mult, """getColBounds max mult failed"""

def test_vault_struct_retrival():
    cruise_path_func = cmn.vault_struct_retrival('cruise')
    float_path_func = cmn.vault_struct_retrival('float')
    station_path_func = cmn.vault_struct_retrival('station')
    satellite_path_func = cmn.vault_struct_retrival('satellite')
    model_path_func = cmn.vault_struct_retrival('model')
    assimilation_path_func = cmn.vault_struct_retrival('assimilation')

    assert cruise_path_func == '/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/cruise/', "cruise vault path test failed"
    assert float_path_func == '/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/float/', "float vault path test failed"
    assert station_path_func == '/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/in-situ/station/', "station vault path test failed"
    assert satellite_path_func == '/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/observation/remote/satellite/', "satellite vault path test failed"
    assert model_path_func == '/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/model/', "model vault path test failed"
    assert assimilation_path_func == '/home/nrhagen/CMAP Data Submission Dropbox/Simons CMAP/vault/assimilation/', "assimilation vault path test failed"

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

    
