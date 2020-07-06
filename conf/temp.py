import vault_structure as vs
import pandas as pd


sat = pd.read_csv("/home/nrhagen/Documents/CMAP/CMAP_Ingestion/conf/sat.csv", sep=",")
sat_list = sat["Table_Name"].to_list()
satbranch = vs.satellite

float = pd.read_csv(
    "/home/nrhagen/Documents/CMAP/CMAP_Ingestion/conf/float.csv", sep=","
)
float_list = float["Table_Name"].to_list()
floatbranch = vs.float

station = pd.read_csv(
    "/home/nrhagen/Documents/CMAP/CMAP_Ingestion/conf/station.csv", sep=","
)
station_list = station["Table_Name"].to_list()
stationbranch = vs.station

model = pd.read_csv(
    "/home/nrhagen/Documents/CMAP/CMAP_Ingestion/conf/model.csv", sep=","
)
model_list = model["Table_Name"].to_list()
modelbranch = vs.model

all = pd.read_csv("/home/nrhagen/Documents/CMAP/CMAP_Ingestion/conf/all.csv", sep=",")
all_list = all["Table_Name"].to_list()

combined_list = sat_list + float_list + station_list + model_list

cruise_list = list(set(all_list).difference(combined_list))


def build_tree(branch, namelist):
    for table in namelist:
        vs.leafStruc(branch + table)


# build_tree(satbranch, sat_list)
# build_tree(floatbranch, float_list)
# build_tree(stationbranch, station_list)
# build_tree(modelbranch, model_list)
# build_tree(vs.cruise, cruise_list)
