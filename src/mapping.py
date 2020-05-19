"""Folium based mapping functions adapted from pycmap"""

from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium import webdriver
import common as cmn
import folium
from folium.plugins import HeatMap, MarkerCluster, Fullscreen, MousePosition
import time
import os
from PIL import Image
from PIL import ImageOps
import DB
import numpy as np
static_outputdir = "/home/nrhagen/Documents/CMAP/CMAP_Ingestion/static/mission_icons/"


def addLayers(m):
    """Adds webtiles to folium map"""
    tiles = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
    folium.TileLayer(tiles=tiles, attr=(" ")).add_to(m)
    return m


def addMarkers(m, df):
    """Adds CircleMarker points to folium map"""
    mc = MarkerCluster(
        options={"spiderfyOnMaxZoom": "False", "disableClusteringAtZoom": "1"}
    )
    for i in range(len(df)):
        folium.CircleMarker(
            location=[df.lat[i], df.lon[i]],
            radius=5,
            color="darkOrange",
            opacity=0.5,
            fill=False,
        ).add_to(mc)
    mc.add_to(m)
    return m


def html_to_static(m, tableName):
    """Outputs folium map to html and static map"""
    m.save(static_outputdir + tableName + ".html")
    options = FirefoxOptions()
    options.add_argument("--headless")
    driver = webdriver.Firefox(options=options)
    fpath = os.getcwd()
    driver.get("file://" + static_outputdir + tableName + ".html")
    # driver.set_window_size(4000, 3000)
    time.sleep(4)
    driver.save_screenshot("map.png")
    driver.close()
    time.sleep(1)
    img = Image.open("map.png")
    border = (50, 25, 25, 40)  # left, up, right, bottom

    ImageOps.crop(img, border).save(static_outputdir + tableName + ".png")



def folium_map(df, tableName):
    """Creates folium map object from input DataFrame"""
    df = df.sample(2000)
    df.reset_index(drop=True, inplace=True)
    data = list(zip(df.lat, df.lon))

    m = folium.Map(
        [df.lat.mean(), df.lon.mean()],
        tiles=None,
        zoom_start=9,
        control_scale=True,
        prefer_canvas=True,
    )
    lat_abs = np.abs(np.max(df['lat']) - np.min(df['lat'])) * 0.03
    lon_abs = np.abs(np.max(df['lon']) - np.min(df['lon'])) * 0.03

    sw = df[['lat', 'lon']].min().values.tolist()
    ne = df[['lat', 'lon']].max().values.tolist()

    # sw[0] = sw[0] - lat_abs
    # sw[1] = sw[1] - lon_abs
    # ne[0] = ne[0]  + lat_abs
    # ne[1] = ne[1] + lon_abs
    m.fit_bounds([sw, ne])
    m = addLayers(m)
    HeatMap(data, gradient={0.65: "#0A8A9F", 1: "#5F9EA0"}).add_to(m)
    m = addMarkers(m, df)
    html_to_static(m, tableName)

tableName = "tblKM1513_HOE_legacy_2A_Dyhrman_Omics"
qry  = """SELECT  * FROM {tableName}""".format(tableName = tableName)
df = DB.dbRead(qry)

m = folium_map(df,tableName)
