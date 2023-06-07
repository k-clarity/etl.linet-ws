import os
import geopandas as gpd
import pandas as pd
from datetime import timedelta
from utils import conex_bd_engine, lerrJson


def data_linet (hora_inicial,hora_final,args):   
 try:
    credenciales = lerrJson(args.awsconnection)
    engine = conex_bd_engine (credenciales["postgres_aws_user"],credenciales["postgres_pwd_aws"],credenciales["postgres_hostname_aws"],credenciales["postgres_port_aws"],credenciales["postgres_database_aws"])
    sql = "SELECT fecha,geom from " +credenciales["tbl_aws"]+ " where fecha >= '" +hora_inicial+ "' and fecha < '" +hora_final+ "' order by fecha asc"
    gdf = gpd.read_postgis(sql, engine)
    pda =  pd.DataFrame(gdf)  
    engine.dispose() 
    return pda
 except Exception as e:
    #print(e)
    print ()    

def data_ws (hora_inicial,hora_final,args):   
 try:
   credenciales = lerrJson(args.wsconnection)
   engine = conex_bd_engine (credenciales["postgres_ws_user"],credenciales["postgres_pwd_ws"],credenciales["postgres_hostname_ws"],credenciales["postgres_port_ws"],credenciales["postgres_database_ws"])
   sql = "SELECT  DATE_TRUNC('millisecond', fecha::timestamp) as fecha, the_geom as geom, altura, tipo, corriente, error from " +credenciales["tbl_ws"]+ " where fecha >= '" +hora_inicial+ "' and fecha < '" +hora_final+ "' order by fecha"
   #gdf = gpd.GeoDataFrame.from_postgis(sql, engine) 
   gdf = gpd.read_postgis(sql, engine)  
   pda =  pd.DataFrame(gdf)  
   engine.dispose() 
   return pda
 except Exception as e:
   #print(e)
   print ()   