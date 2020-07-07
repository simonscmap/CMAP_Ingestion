##dev note: Download metadata for cruises without trajectories. If dataset has listed cruise that has metadata in vault, but no r2r traj. Can create cruise traj based on dataset ST.

import sys
import os
import transfer
import shutil

import vault_structure as vs
import common as cmn
import DB
import requests
import data
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import time


##############################################
########## Cruise Helper Funcs ###############
##############################################
def resample_trajectory(df, interval="1min"):
    df.index = pd.to_datetime(df.time)
    rs_df = df.resample(interval).mean()
    rs_df = rs_df.dropna()
    rs_df.reset_index(inplace=True)
    return rs_df


def vault_cruises():
    cruise_dirs = os.listdir(vs.r2r_cruise)
    return cruise_dirs


def retrieve_id_search(cmdf, id_col_str):
    id_return = cmdf[cmdf["id_col"].str.contains(id_col_str)]["info_col"].to_list()
    return id_return


def trim_returned_link(link_str):
    if isinstance(link_str, str):
        link_str = [link_str]
    trimmed_link = [link.replace("<", "").replace(">", "") for link in link_str]
    return trimmed_link


def download_cruise_data_from_url(cruise_name, download_url_str, dataset_category):
    cruise_base_path = vs.r2r_cruise
    vs.makedir(cruise_base_path + cruise_name + "/")
    transfer.requests_Download(
        download_url_str,
        cruise_name + "_" + dataset_category + ".csv",
        cruise_base_path + cruise_name + "/",
    )


def fill_ST_bounds_metadata(cruise_name):
    traj_path = vs.r2r_cruise + cruise_name + "/" + cruise_name + "_trajectory.csv"
    meta_path = vs.r2r_cruise + cruise_name + "/" + cruise_name + "_cruise_metadata.csv"
    meta_df = pd.read_csv(meta_path, sep=",")
    try:
        traj_df = pd.read_csv(traj_path, sep=",")
        traj_df["time"] = pd.to_datetime(traj_df["time"], errors="coerce").dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except:
        pass

    time_min = np.min(traj_df["time"])
    time_max = np.max(traj_df["time"])
    lat_min = np.min(traj_df["lat"])
    lat_max = np.max(traj_df["lat"])
    lon_min = np.min(traj_df["lon"])
    lon_max = np.max(traj_df["lon"])
    meta_df["Start_Time"] = time_min
    meta_df["End_Time"] = time_max
    meta_df.at[0, "Lat_Min"] = lat_min
    meta_df.at[0, "Lat_Max"] = lat_max
    meta_df.at[0, "Lon_Min"] = lon_min
    meta_df.at[0, "Lon_Max"] = lon_max
    meta_df.to_csv(meta_path, sep=",", index=False)


def update_tblCruises():
    cruises_in_vault = cmn.lowercase_List(vault_cruises())
    DB_cruises = set(cmn.lowercase_List(cmn.getListCruises()["Name"].to_list()))
    new_cruises = sorted(list(set(cruises_in_vault) - set(DB_cruises)))
    for cruise in new_cruises:
        try:
            meta_df = cmn.nanToNA(
                pd.read_csv(
                    vs.r2r_cruise
                    + cruise.upper()
                    + "/"
                    + cruise.upper()
                    + "_cruise_metadata.csv"
                )
            )
            DB.lineInsert(
                "Rainier",
                "tblCruise",
                "(Nickname,Name,Ship_Name,Start_Time,End_Time,Lat_Min,Lat_Max,Lon_Min,Lon_Max,Chief_Name)",
                tuple(meta_df.iloc[0].astype(str).to_list()),
            )
            cruise_traj_flag = cmn.cruise_has_trajectory(cruise)
            if cruise_traj_flag == False:
                traj_df = cmn.nanToNA(
                    pd.read_csv(
                        vs.r2r_cruise
                        + cruise.upper()
                        + "/"
                        + cruise.upper()
                        + "_trajectory.csv"
                    )
                )
                Cruise_ID = cmn.get_cruise_IDS([cruise])
                traj_df["Cruise_ID"] = Cruise_ID[0]
                traj_df = traj_df[["Cruise_ID", "time", "lat", "lon"]]
                data.data_df_to_db(traj_df, "tblCruise_Trajectory", clean_data_df=False)

            print(cruise, " Ingested into DB")

        except Exception as ex:
            print(ex, cruise, " not ingested...")


# update_tblCruises()
##############################################
############## Cruise Data ###################
##############################################


def get_cruise_data(cmdf, cruise_name):
    try:
        cruise_data_links = retrieve_id_search(cmdf, "isr2r:hasCruiseof")
        trim_data_links = trim_returned_link(cruise_data_links)

        for data_link in trim_data_links:
            print(data_link)

            data = parse_r2r_page(data_link)
            return data

    except:
        pass


##############################################
########### Cruise Trajectory ################
##############################################
def get_cruise_traj(cmdf, cruise_name):
    cruise_traj_best_str = """http://get.rvdata.us/cruise/{cruise_name}/products/r2rnav/{cruise_name}_bestres.r2rnav""".format(
        cruise_name=cruise_name
    )
    cruise_traj_1min_str = """http://get.rvdata.us/cruise/{cruise_name}/products/r2rnav/{cruise_name}_1min.r2rnav""".format(
        cruise_name=cruise_name
    )
    cruise_traj_control_points_str = """http://get.rvdata.us/cruise/{cruise_name}/products/r2rnav/{cruise_name}_control.r2rnav""".format(
        cruise_name=cruise_name
    )
    try:
        download_cruise_data_from_url(cruise_name, cruise_traj_1min_str, "trajectory")
        clean_cruise_traj(cruise_name)
    except:
        download_cruise_data_from_url(cruise_name, cruise_traj_best_str, "trajectory")
        clean_cruise_traj(cruise_name)
    else:
        download_cruise_data_from_url(
            cruise_name, cruise_traj_control_points_str, "trajectory"
        )
        clean_cruise_traj(cruise_name)


def clean_cruise_traj(cruise_name):
    fpath = vs.r2r_cruise + cruise_name + "/" + cruise_name + "_trajectory.csv"
    try:
        df = pd.read_csv(
            fpath,
            skiprows=3,
            names=[
                "time",
                "lon",
                "lat",
                "Instantaneous Speed-over-ground",
                "Instantaneous Course-over-ground",
            ],
            sep="\t",
        )
    except:
        print(
            "Trajectory CSV download invalid or corrupted. Please manually check download link. Removing ship directory"
        )
        shutil.rmtree(vs.r2r_cruise + cruise_name + "/")

    df = df[["time", "lat", "lon"]]
    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    dfr = resample_trajectory(df, interval="1min")
    dfr.to_csv(fpath, sep=",", index=False)


##############################################
#######  Cruise General Metadata   ###########
##############################################
def get_chief_sci(cmdf):
    try:
        chief_sci_link = (
            retrieve_id_search(cmdf, "r2r:hasParticipant")[0]
            .replace("<", "")
            .replace(">", "")
        )
        chief_sci_df = parse_r2r_page(chief_sci_link)
        chief_sci = retrieve_id_search(chief_sci_df, "rdfs:label")
        chief_sci = chief_sci[0].split(" on")[0]
    except:
        chief_sci = ""
    return chief_sci


def get_cruise_metadata(cmdf, cruise_name):
    try:
        cruise_name = retrieve_id_search(cmdf, "gl:hasCruiseID")[0]
    except:
        cruise_name = ""
    try:
        cruise_nickname = retrieve_id_search(cmdf, "dcterms:title")[0]
    except:
        cruise_nickname = ""
    try:
        cruise_shipname = retrieve_id_search(cmdf, "r2r:VesselName")[0]
    except:
        cruise_shipname = ""
    chief_sci = get_chief_sci(cmdf)
    format_cruise_metadata(cruise_name, cruise_nickname, cruise_shipname, chief_sci)


def format_cruise_metadata(cruise_name, cruise_nickname, cruise_shipname, chief_sci):
    cruise_name = cmn.empty_list_2_empty_str(cruise_name)
    cruise_nickname = cmn.empty_list_2_empty_str(cruise_nickname)
    cruise_shipname = cmn.empty_list_2_empty_str(cruise_shipname)

    fpath = vs.r2r_cruise + cruise_name + "/" + cruise_name + "_cruise_metadata.csv"
    tblCruise_df = pd.DataFrame(
        {
            "Nickname": [cruise_nickname],
            "Name": [cruise_name],
            "Ship_Name": [cruise_shipname],
            "Start_Time": "",
            "End_Time": "",
            "Lat_Min": "",
            "Lat_Max": "",
            "Lon_Min": "",
            "Lon_Max": "",
            "Chief_Name": [chief_sci],
        }
    )
    vs.makedir(vs.r2r_cruise + cruise_name + "/")
    tblCruise_df.to_csv(fpath, sep=",", index=False)


########## Cruise Data Parsing ###################


def gather_cruise_links():
    all_cruise_url = "http://data.rvdata.us/directory/Cruise"
    page = requests.get(all_cruise_url)
    soup = BeautifulSoup(page.content, "html.parser")
    table_rows = soup.findAll("a")
    all_cruise_df = pd.DataFrame(columns=["cruise_name", "cruise_link"])
    for row in table_rows:
        if "/cruise/" in str(row):
            cruise_link = str(row).split("""href=\"""")[1].split('">')[0]
            cruise_name = cruise_link.split("cruise/")[1]
            all_cruise_df.loc[len(all_cruise_df)] = [cruise_name, cruise_link]
    return all_cruise_df


def parse_r2r_page(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    table_rows = soup.findAll("tr")
    cmdf = pd.DataFrame(columns=["id_col", "info_col"])
    for tr in table_rows:
        td = tr.findAll("td")
        row = [tr.text for tr in td]
        rowlen = row + [""] * (2 - len(row))
        rowlen = [row.replace("\n", "").strip() for row in rowlen]
        cmdf.loc[len(cmdf)] = rowlen
    return cmdf


def parse_cruise_metadata(cruise_name="", cruise_url=""):
    """General input function. Can either take cruise link or cruise name as input"""
    if cruise_name != "":
        cruise_url = "http://data.rvdata.us/page/cruise/" + cruise_name.upper()
    try:
        cmdf = parse_r2r_page(cruise_url)
        return cmdf

    except Exception as e:
        print(e)


# cmdf = parse_cruise_metadata("MV0907")
# get_cruise_metadata(cmdf, "MV0907")
# get_cruise_traj(cmdf, "MV0907")
# clean_cruise_traj("MV0907")


def download_all_cruises():
    cruise_links = gather_cruise_links()
    for cruise_name, cruise_link in zip(
        cruise_links["cruise_name"], cruise_links["cruise_link"]
    ):

        try:
            cmdf = parse_cruise_metadata(cruise_name)
            if not cmdf.empty:
                try:
                    get_cruise_traj(cmdf, cruise_name)
                    get_cruise_metadata(cmdf, cruise_name)
                    fill_ST_bounds_metadata(cruise_name)
                    print(cruise_name, " Downloaded")

                except:
                    print(
                        cruise_name,
                        " cruise data not downloaded b/c trajectory or metadata mising...",
                    )
                # try:
                #     get_cruise_metadata(cmdf, cruise_name)
                # except:
                #     print(cruise_name, " cruise metadata not downloaded")
                # try:
                #     get_cruise_traj(cmdf, cruise_name)
                #     fill_ST_bounds_metadata(cruise_name)
                # except:
                #     print(cruise_name, " cruise trajectory not downloaded")

        except:
            print("##########################")
            print(cruise_name, " No applicable cruise data -- cmdf empty")
            print("##########################")


# download_all_cruises()

# download_all_cruises()