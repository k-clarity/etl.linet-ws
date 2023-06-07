import geopandas as gpd
import pandas as pd
from utils import conex_bd_engine

def merge_df (data_linet,data_ws):   
 try:
    #POSTGRES_USER = os.getenv('POSTGRES_USER_AWS')
    merged_df = pd.merge(data_linet, data_ws , how='outer', indicator='resultado',sort=True, left_on='fecha', right_on='fecha')
    merged_df_filter = merged_df[merged_df.resultado == 'right_only']
    return merged_df_filter
 except Exception as e:
    print(e)
    print ()