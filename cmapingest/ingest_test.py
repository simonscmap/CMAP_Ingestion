###
# PI = post ingestion test
###
import pandas as pd
from cmapingest import DB


def data_tests():
    dataset_name_valid_bool = datasetINtblDatasets(dataset_name)
    pass


def main(tableName):
    pass


"""

retrieval funcs will live in common...

dataset either full or partially ingested into DB:
-Data tests:
  -does the tableName exist?
  -does the len(tableName) == len(data_df)?
  -does the table contain (time,lat,lon(depth?))
  -can you select top(1) from tableName.
  -
  -Index:
  -Does the index exist?
  -Is the index on space-time?

-Metadata Table tests:
  -tblDatasets:
    -Does the table exist in tblDatasets?
    -Are there any null cols in tblDatasets?
  -tblDatasetStats:
    -Do that stats match the min/max of stats from pandas (df.describe())
    -PI(check that stats have updated...)
  -tblDataset_References:
    -if refs exist in df_dataset_metadata, are they in table.
    -Do the ref strings match
  -tblVariables:
    -Do the num of rows match the num of rows in vars_meta_data
    -Are there any NULL values? Warning on some of them (ie. not history)
   -tblKeywords:
       -Do the len(distinct(keyword_IDs)) match len(vars_metadata).

   UDFCATALOG() where tablename = tablename:
   - does len of UDFCATALOG match len vars_meta_data?






Is data retrievable
Test sp_subset
Do the number of rows match in SQL match the # of rows in the dataframe?
Min,max of ST cols match min,max in dataframe and tblDataset_Stats?



Is data visualizable
Pycmap?
- pick single day in daterange. min/max bounds
Web App?



Is metadata complete
Does metadata retrieval match dataset metadata?

"""
