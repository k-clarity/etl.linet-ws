import os
import geopandas as gpd
from geoalchemy2 import Geometry
from utils import conex_bd_engine, lerrJson


def insert_bd (gdf,args):   
 try:
    credenciales = lerrJson(args.wsconnection)
    engine = conex_bd_engine (credenciales["postgres_ws_user"],credenciales["postgres_pwd_ws"],credenciales["postgres_hostname_ws"],credenciales["postgres_port_ws"],credenciales["postgres_database_ws"])
    gdf = gpd.GeoDataFrame(gdf)
    gdf = gdf.drop(['geom_x', 'resultado'], axis=1)
    gdf.rename( columns = { "geom_y": "geom"}, inplace=True) 
    gdf = gdf.set_geometry("geom")    
    gdf = gdf.set_crs(4326, allow_override=True)
    #print(gdf)
    gdf.to_postgis(credenciales["tbl_ins"], engine, schema = credenciales["schema_db_ws"], if_exists='append', dtype={"geom": Geometry("POINT", srid=4326)})
    engine.dispose()
 except Exception as e:
    print(e)
    print ()

