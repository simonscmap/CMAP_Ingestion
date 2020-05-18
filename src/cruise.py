########## Dev Notes: Some metadata downloads are failing if a val (ex chief sci) is missing. Should download without...

import sys
import transfer
sys.path.append("../conf/")
import vault_structure as vs
import common as cmn
import requests
import data
import pandas as pd    # time_min, time_max = cmn.getColBounds(traj_df,'time')
import numpy as np
from bs4 import BeautifulSoup
import time


########## Vault Structure Transfer ###################
""" func to put raw cruise data in cruise_folder on vault """

########## Web Functions ###################
"""https request func"""
"""BS finder funcs"""
"""func: gather name ie.  gl:hasCruiseIDe"""
"""func: gather nickname guess ie. Title: gradients"""
"""func: gather shipname ie. shipname : r2r:VesselName"""
"""func: gather chief scientist: first of crew members """
"""func:  download cruise_gps data  """
"""func:  parse gps, sample? min/max time/lat/lon  """


##############################################
########## Cruise Helper Funcs ###############
##############################################


def retrieve_id_search(cmdf,id_col_str):
    id_return = cmdf[cmdf['id_col'].str.contains(id_col_str)]['info_col'].to_list()
    return id_return

def trim_returned_link(link_str):
    if isinstance(link_str, str):
        link_str = [link_str]
    trimmed_link = [link.replace('<','').replace('>','') for link in link_str]
    return trimmed_link

def download_cruise_data_from_url(cruise_name,download_url_str,dataset_category):
    cruise_base_path = vs.r2r_cruise
    vs.makedir(cruise_base_path + cruise_name + '/')
    transfer.requests_Download(download_url_str,cruise_name + '_' + dataset_category + '.csv', cruise_base_path + cruise_name + '/')

def fill_ST_bounds_metadata(cruise_name):
    traj_path = vs.r2r_cruise + cruise_name + '/' + cruise_name  + '_trajectory.csv'
    meta_path =  vs.r2r_cruise + cruise_name + '/' + cruise_name  + '_cruise_metadata.csv'
    meta_df = pd.read_csv(meta_path,sep=',')
    try:
        traj_df = pd.read_csv(traj_path,sep=',')
        traj_df['time'] = pd.to_datetime(traj_df['time'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S' )
    except:
        pass

    time_min = np.min(traj_df['time'])
    time_max = np.max(traj_df['time'])
    lat_min = np.min(traj_df['lat'])
    lat_max = np.max(traj_df['lat'])
    lon_min = np.min(traj_df['lon'])
    lon_max = np.max(traj_df['lon'])
    meta_df['Start_Time'] = time_min
    meta_df['End_Time'] = time_max
    meta_df.at[0,'Lat_Min'] = lat_min
    meta_df.at[0,'Lat_Max'] = lat_max
    meta_df.at[0,'Lon_Min'] = lon_min
    meta_df.at[0,'Lon_Max'] = lon_max
    meta_df.to_csv(meta_path,sep=',',index=False)

##############################################
########### Cruise Trajectory ################
##############################################

def get_cruise_traj(cmdf,cruise_name):
    try: #full 1 min res
        cruise_traj_links = retrieve_id_search(cmdf,"r2r:hasProduct")
        trim_traj_link = trim_returned_link(cruise_traj_links)
        if len(trim_traj_link) > 1:
            trim_link = trim_traj_link[1] # second cruise product should be 1 min temporal res
        else:
            trim_link = trim_traj_link[0]
        cruise_traj_df = parse_r2r_page(trim_link)
        label = retrieve_id_search(cruise_traj_df,"rdfs:label")
        link = trim_returned_link(cruise_traj_df['info_col'][cruise_traj_df['id_col']=='dcterms:source'].iloc[0])
        if "1Min" in label[0]:
            download_cruise_data_from_url(cruise_name,link[0],'trajectory')
            clean_cruise_traj(cruise_name)


    except:
        cruise_track_link = retrieve_id_search(cmdf,"r2r:Track")[0].replace('<','').replace('>','')
        cruise_track_df = parse_r2r_page(cruise_track_link)
        track_string = retrieve_id_search(cruise_track_df,"geosparql:asWKT")[0]
        track_string = track_string.split("(")[1].split(")")[0]
        coord_list = track_string.split(",")
        coord_df = pd.DataFrame(coord_list)
        coord_df[['lon','lat']] = pd.Series(coord_list).str.split(" ",expand=True)
        coord_df['time'] = ''
        coord_df = coord_df[['time','lat','lon']]
        coord_df.to_csv(vs.r2r_cruise + cruise_name + '/' + cruise_name  + '_trajectory.csv',sep=',',index=False)


def clean_cruise_traj(cruise_name):
    fpath = vs.r2r_cruise + cruise_name + '/' + cruise_name  + '_trajectory.csv'
    df = pd.read_csv(fpath,skiprows=3,names=['time','lon','lat','Instantaneous Speed-over-ground','Instantaneous Course-over-ground'],sep='\t')
    df = df[['time','lat','lon']]
    df['time'] = pd.to_datetime(df['time'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S' )
    df.to_csv(fpath,sep=',',index=False)




##############################################
#######  Cruise General Metadata   ###########
##############################################
def get_chief_sci(cmdf):
    try:
        chief_sci_link = retrieve_id_search(cmdf,"r2r:hasParticipant")[0].replace('<','').replace('>','')
        chief_sci_df = parse_r2r_page(chief_sci_link)
        chief_sci = retrieve_id_search(chief_sci_df,"rdfs:label")
        chief_sci = chief_sci[0].split(" on")[0]
    except:
        chief_sci = ''
    return chief_sci


def get_cruise_metadata(cmdf,cruise_name):
    try:
        cruise_name = retrieve_id_search(cmdf, "gl:hasCruiseID")[0]
    except:
        cruise_name = ''
    try:
        cruise_nickname = retrieve_id_search(cmdf, "dcterms:title")
    except:
        cruise_nickname = ''
    try:
        cruise_shipname = retrieve_id_search(cmdf, "r2r:VesselName")
    except:
        cruise_shipname = ''
    chief_sci = get_chief_sci(cmdf)
    format_cruise_metadata(cruise_name,cruise_nickname,cruise_shipname,chief_sci)

def format_cruise_metadata(cruise_name,cruise_nickname,cruise_shipname,chief_sci):
    fpath = vs.r2r_cruise + cruise_name + '/' + cruise_name  + '_cruise_metadata.csv'
    tblCruise_df = pd.DataFrame({'Nickname':cruise_nickname,'Name':cruise_name,'Ship_Name':cruise_shipname,'Start_Time':'','End_Time':'','Lat_Min':'','Lat_Max':'','Lon_Min':'','Lon_Max':'','Chief_Name':chief_sci})
    vs.makedir(vs.r2r_cruise + cruise_name + '/')
    tblCruise_df.to_csv(fpath, sep=',',index=False)




########## Cruise Data Parsing ###################


def gather_cruise_links():
    all_cruise_url = "http://data.rvdata.us/directory/Cruise"
    page = requests.get(all_cruise_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table_rows = soup.findAll('a')
    all_cruise_df = pd.DataFrame(columns=['cruise_name','cruise_link'])
    for row in table_rows:
        if "/cruise/" in str(row):
            cruise_link = str(row).split("""href=\"""")[1].split("\">")[0]
            cruise_name = cruise_link.split("cruise/")[1]
            all_cruise_df.loc[len(all_cruise_df)] = [cruise_name,cruise_link]
    return all_cruise_df



def parse_r2r_page(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    table_rows = soup.findAll('tr')
    cmdf = pd.DataFrame(columns=['id_col','info_col'])
    for tr in table_rows:
        td = tr.findAll('td')
        row = [tr.text for tr in td]
        rowlen = row + [''] * (2 - len(row))
        rowlen = [row.replace('\n','').strip() for row in rowlen]
        cmdf.loc[len(cmdf)] = rowlen
    return cmdf

def parse_cruise_metadata(cruise_name='', cruise_url=''):
    """General input function. Can either take cruise link or cruise name as input"""
    if cruise_name != '':
        cruise_url = "http://data.rvdata.us/page/cruise/" + cruise_name.upper()
    try:
        cmdf = parse_r2r_page(cruise_url)
        return cmdf

    except Exception as e: print(e)



cruise_links = gather_cruise_links()
#
for cruise_name,cruise_link in zip(cruise_links['cruise_name'],cruise_links['cruise_link']):

    try:
        cmdf = parse_cruise_metadata(cruise_name)
        if not cmdf.empty:
            try:
                get_cruise_metadata(cmdf,cruise_name)
            except:
                print(cruise_name, " cruise metadata not downloaded")
            try:
                get_cruise_traj(cmdf,cruise_name)
                fill_ST_bounds_metadata(cruise_name)
            except:
                print(cruise_name, " cruise trajectory not downloaded")

        print(cruise_name, " Downloaded")
    except:
        print("##########################")
        print(cruise_name, " Not Fully Downloaded")
        print("##########################")

# cruise_name = 'AE0818'
# cruise_name = 'AR1-03'
# cruise_url = "get_cruise_metadata(cmdf,cruise_name)http://data.rvdata.us/page/cruise/KM1906"
# cmdf = parse_cruise_metadata(cruise_name)
# if not cmdf.empty:
#     get_cruise_metadata(cmdf,cruise_name)
#     get_cruise_traj(cmdf,cruise_name)
#     fill_ST_bounds_metadata(cruise_name)
