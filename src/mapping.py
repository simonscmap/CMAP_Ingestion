import geopandas
from cartopy import crs as ccrs
import cartopy.io.img_tiles as cimgt
import cartopy
import matplotlib.pyplot as plt
import numpy as np
import sys
sys.path.append('../conf/')
import vault_structure as vs




static_outputdir = '/home/nrhagen/Documents/CMAP/CMAP_Ingestion/static/mission_icons/'

tableName = 'tblSeaglider_148_Mission_15'

def importDataMemory(tableName):
    data_df = data.import_single_data(vs.cruise + tableName)
    dataset_metadata_df,variable_metadata_df = metadata.import_metadata(vs.cruise + tableName)
    data_dict = {'data_df':data_df, 'dataset_metadata_df':dataset_metadata_df,'variable_metadata_df':variable_metadata_df}
    return data_dict



def pandas2geopandas(df,zoom_level=None):
    if len(df) > 2000:
        df = df.sample(2000)
    gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df.lon, df.lat))

    if zoom_level != None:
        gdf.crs = {'init' :'epsg:3857'}
    else:
        crs = cartopy.crs.Mollweide(central_longitude=-140)
        gdf.crs = {'init' :'epsg:4326'}
    return gdf


def get_extent(df,bound_multiplier = 1):
    min_lon = min(df['lon'])
    max_lon = max(df['lon'])
    min_lat = min(df['lat'])
    max_lat = max(df['lat'])
    lon_range = np.abs(min_lon - max_lon)
    lat_range = np.abs(min_lat - max_lat)

    min_lon_bound = min_lon - (lon_range * bound_multiplier)
    max_lon_bound = max_lon + (lon_range * bound_multiplier)
    min_lat_bound = min_lat - (lat_range * bound_multiplier)
    max_lat_bound = max_lat + (lat_range * bound_multiplier)
    if min_lon_bound < -180:
        min_lon_bound = -180
    if max_lon_bound > 180:
        max_lon_bound = 180
    if min_lat_bound < -90:
        min_lat_bound = -90
    if max_lat_bound > 90:
        max_lat_bound = 90

    extent_dict = {'min_lon':min_lon_bound,'max_lon':max_lon_bound,'min_lat':min_lat_bound,'max_lat':max_lat_bound}
    return extent_dict

def cartopy_sparse_map(data_df,tableName,outputdir = static_outputdir,zoom_level=None):


    gdf = pandas2geopandas(data_df,zoom_level)
    arcgis_url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
    tiles = cimgt.GoogleTiles(url=arcgis_url)

    if zoom_level != None:
        ax = plt.axes(projection=cartopy.crs.PlateCarree())
        extent_dict = get_extent(data_df)
    
        # ax.set_extent([extent_dict['min_lon'], extent_dict['max_lon'],extent_dict['min_lat'], extent_dict['max_lat']], crs=ccrs.PlateCarree())
        ax.add_image(tiles,7,interpolation='spline36')
        ax.autoscale_view()
        # ax.set_aspect('auto')
        ax.set_aspect(1./ax.get_data_ratio())
    else:
        ax = plt.axes(projection=ccrs.Mollweide(central_longitude=-140))
        ax.set_global()
        ax.add_image(tiles,4,interpolation='spline36')

    ax.outline_patch.set_linewidth(2)
    ax.outline_patch.set_edgecolor('#424242')
    ax.set_facecolor('#424242')
    gdf.plot(ax=ax, color='#FF8C00', markersize= 3,alpha = 0.4,rasterized=True)
    # plt.show()
    plt.savefig(static_outputdir + tableName + '.svg',dpi=200,transparent=True,bbox_inches='tight',rasterized=True)
    plt.close()
