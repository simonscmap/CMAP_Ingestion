import requests
import pandas as pd
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
"""master urls"""

def retrieve_id_search(cmdf,id_col_str):
    id_return = cmdf[cmdf['id_col'].str.contains(id_col_str)]['info_col'].to_list()
    return id_return

def trim_returned_link(link_str):
    trimmed_link = [link.replace('<','').replace('>','') for link in link_str]
    return trimmed_link

def get_cruise_traj(cmdf):
    cruise_traj_links = retrieve_id_search(cmdf,"r2r:hasProduct")# [0].replace('<','').replace('>','')
    trim_traj_link = trim_returned_link(cruise_traj_links)
    if len(trim_traj_link) > 1:
        trim_link = trim_traj_link[1] # second cruise product should be 1 min temporal res
    else:
        trim_link = trim_traj_link[0]
    cruise_traj_df = parse_r2r_page(trim_link)
    label = retrieve_id_search(cruise_traj_df,"rdfs:label")
    if label[0].contains("1Min"):
        return

    return label

def get_chief_sci(cmdf):
    chief_sci_link = retrieve_id_search(cmdf,"r2r:hasParticipant")[0].replace('<','').replace('>','')
    chief_sci_df = parse_r2r_page(chief_sci_link)
    chief_sci = retrieve_id_search(chief_sci_df,"rdfs:label")
    chief_sci = chief_sci[0].split(" on")[0]
    return chief_sci


########## Cruise Data Parsing ###################
""""""

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

def parse_cruise_metadata(cruise_name='', cruise_link=''):
    if cruise_name != '':
        cruise_url = "http://data.rvdata.us/page/cruise/" + cruise_name.upper()
    print(cruise_url)
    cmdf = parse_r2r_page(cruise_url)
    return cmdf

# cruise_url = "http://data.rvdata.us/page/cruise/KM1906"
cmdf = parse_cruise_metadata(cruise_name = 'mgl1704')
# cmdf = parse_cruise_metadata(cruise_name = 'km1513')

cruise_name = retrieve_id_search(cmdf, "gl:hasCruiseID")
cruise_nickname = retrieve_id_search(cmdf, "dcterms:title")
cruise_shipname = retrieve_id_search(cmdf, "r2r:VesselName")
chief_sci = get_chief_sci(cmdf)

ctl = get_cruise_traj(cmdf)
def print_cruise_info(cruise_name, cruise_nickname,cruise_shipname,chief_sci):
    print("Cruise Name: ", cruise_name, "\n")
    print("Cruise Nickname: ", cruise_nickname, "\n")
    print("Cruise Ship Name: ", cruise_shipname, "\n")
    print("Cruise Chief Scientist: ", chief_sci, "\n")

# print_cruise_info(cruise_name, cruise_nickname,cruise_shipname,chief_sci)
