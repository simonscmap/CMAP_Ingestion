import pandas as pd
from pandas._testing import assert_frame_equal
import sys
import numpy as np

sys.path.append("../cmapingest")
import common as cmn
import metadata


def test_ID_Var_Map():
    test_series = pd.Series(["In-Situ","CTD", " ","unknownsensors"])
    expected_series = pd.Series([2.0,5.0,0,0])
    func_series = metadata.ID_Var_Map(test_series,"Sensor","tblSensors")
    print(func_series)
    assert list(func_series) == list(expected_series), "ID_Var_Map test failed."
