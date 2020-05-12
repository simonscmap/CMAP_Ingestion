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


########## Cruise Data Parsing ###################
""""""
def parse_cruise_metadata(cruise_name='', cruise_link=''):
    if cruise_name != '':
        cruise_url = "http://data.rvdata.us/page/cruise/" + cruise_name.upper()
    page = requests.get(cruise_url)
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
cruise_url = "http://data.rvdata.us/page/cruise/KM1906"
cmdf = parse_cruise_metadata(cruise_name = 'kok1606')
cruise_name = retrieve_id_search(cmdf, "gl:hasCruiseID")
cruise_nickname = retrieve_id_search(cmdf, "dcterms:title")
cruise_shipname = retrieve_id_search(cmdf, "r2r:VesselName")
chief_sci = retrieve_id_search(cmdf,"r2r:hasParticipant")
