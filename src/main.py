import os, datetime
from get_data import data_linet
from get_data import data_ws
from transform import merge_df
from insert_bd import insert_bd
from utils import horas, parse_args
import geopandas as gpd
import pandas as pd

#load_dotenv()

args = parse_args()

if __name__ == '__main__':
    hora_actual = datetime.datetime.utcnow()
    hora_inicial, hora_final = horas(hora_actual)
    data_linet = data_linet(hora_inicial,hora_final,args)
    data_ws = data_ws(hora_inicial,hora_final,args)
    merge = merge_df (data_linet,data_ws)
    insert_bd(merge,args)