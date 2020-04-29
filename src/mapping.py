import geopandas
from cartopy import crs as ccrs
import cartopy.io.img_tiles as cimgt
import cartopy
import matplotlib.pyplot as plt


static_outputdir = '/home/nrhagen/Documents/CMAP/CMAP_Ingestion/static/mission_icons/'


def pandas2geopandas(df):
    if len(df) > 2000:
        df = df.sample(2000)
    gdf = geopandas.GeoDataFrame(
    df, geometry=geopandas.points_from_xy(df.lon, df.lat))
    gdf.crs = {'init' :'epsg:4326'}
    crs = cartopy.crs.Mollweide(central_longitude=-140)
    crs_proj4 = crs.proj4_init
    gdf['geometry'] = gdf['geometry'].to_crs(crs_proj4)
    return gdf


def cartopy_sparse_map(data_df,tableName,outputdir = static_outputdir):
    gdf = pandas2geopandas(data_df)
    arcgis_url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
    tiles = cimgt.GoogleTiles(url=arcgis_url)
    ax = plt.axes(projection=ccrs.Mollweide(central_longitude=-140))
    ax.set_global()
    ax.add_image(tiles,4,interpolation='spline36')
    ax.outline_patch.set_linewidth(2)
    ax.outline_patch.set_edgecolor('#424242')
    ax.set_facecolor('#424242')
    gdf.plot(ax=ax, color='#FF8C00', markersize= 3,alpha = 0.4,rasterized=True)
    plt.savefig(static_outputdir + tableName + '.svg',dpi=200,transparent=True,bbox_inches='tight',rasterized=True)
    plt.close()
