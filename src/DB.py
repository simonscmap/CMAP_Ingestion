import pandas as pd

def build_SQL_suggestion_df(df):
    # sug_df = pd.DataFrame(columns = ['column_name','dtype','max_count'])
    sug_df = pd.DataFrame(columns = ['column_name','dtype'])
    for cn in list(df):
        col_dtype = str(df[cn][df[cn] != ' '].dtype)
        # max_count = len(max(df[cn].astype(str)))
        # sug_list = [cn, col_dtype, max_count]
        sug_list = [cn, col_dtype]
        sug_df.loc[len(sug_df)] = sug_list
    return sug_df
cdt = build_SQL_suggestion_df(df)



def SQL_suggestion_formatter(suggested_df='', tableName='tblTEST', FG = 'Primary', DB = 'Opedia', server = 'Rainer'):

    cdt['null_status'] = 'NULL,'
    cdt.loc[cdt['column_name'] == 'time','dtype'] = '[datetime]'
    cdt.loc[cdt['column_name'] == 'time','null_status'] = 'NOT NULL,'
    cdt.loc[cdt['column_name'] == 'lat','null_status'] = 'NOT NULL,'
    cdt.loc[cdt['column_name'] == 'lon','null_status'] = 'NOT NULL,'
    if 'depth' in list(cdt['column_name']):
        cdt.loc[cdt['column_name'] == 'depth','null_status'] = 'NOT NULL,'
    cdt['null_status'].iloc[-1] = cdt['null_status'].iloc[-1].replace(',','')
    cdt['column_name'] = '[' + cdt['column_name'].astype(str) + ']'
    cdt['dtype'] = cdt['dtype'].replace('object','[float]')
    cdt['dtype'] = cdt['dtype'].replace('float64','[float]')
    cdt['dtype'] = cdt['dtype'].replace('int64','[int]')

    var_string = cdt.to_string(header=False,index=False)


    """ Print table as SQL format """
    SQL_str = """
    USE [{}]

    SET ANSI_NULLS ON
    GO

    SET QUOTED_IDENTIFIER ON
    GO

    CREATE TABLE [dbo].[{}](

    {}


    ) ON [{}]

    GO""".format(
    DB
    ,tableName
    ,var_string
    ,FG
    )
    print(SQL_str)
    return SQasdf = SQL_suggestion_formatter(suggested_df=cdt, tableName='tblSeaglider_148_Mission_15')
L_str
