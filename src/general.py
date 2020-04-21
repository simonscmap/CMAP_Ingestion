import sys
import os
sys.path.append('../conf/')
import vault_structure as vs
import transfer


"""

test- split file, move file(s),

general function arguments:
-tableName - used to create directory - data and metadata paths can be returned
1. input tableName into general func. dir created. move file from staging to dir
eventually input would be staging/filename, tableName --transfer

newly transfered files in new vault
-pull into three dataframes -
-data cleaning func check
    -print df.cols, df.dtypes, pause
    -create SQL table
    -data.py export to /temp/dir
-bcp util/insert funcs in DB.py





"""
# vs.leafStruc('tableName')


filename = 'sg148m15d001-762_cmap.xlsx'
transfer.single_file_split(filename)
transfer.staging_to_vault(filename,vs.cruise + 'tblSeaglider_148_Mission_15', remove_file_flag=True)
