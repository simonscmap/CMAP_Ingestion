import pandas as pd
import numpy as np

def strip_whitespace_headers(df):
    df.rename(columns=lambda x: x.strip())
    return df

def nanToNA(df):
    df = df.replace(np.nan, '', regex=True)
    return df

def getColBounds(df,col,list_multiplier= None):
    min_col = df[col].min()
    max_col = df[col].max()
    if list_multiplier != None:
        min_col = [min_col] * int(list_multiplier)
        max_col = [max_col] * int(list_multiplier)

    return min_col, max_col
