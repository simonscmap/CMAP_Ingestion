import pandas as pd
import numpy as np

def strip_whitespace_headers(df):
    df.rename(columns=lambda x: x.strip())
    return df

def nanToNA(df):
    df = df.replace(np.nan, '', regex=True)
    return df
