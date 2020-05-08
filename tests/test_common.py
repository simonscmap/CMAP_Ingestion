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
