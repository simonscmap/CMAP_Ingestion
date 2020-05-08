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
