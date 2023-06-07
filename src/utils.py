import os
from datetime import timedelta
from sqlalchemy import create_engine
import argparse
import json
#load_dotenv()

def conex_bd_engine (POSTGRES_USER,POSTGRES_PWD,POSTGRES_HOSTNAME,POSTGRES_PORT,POSTGRES_DATABASE):   
 try:
    POSTGRES_USER = POSTGRES_USER
    POSTGRES_PASSWORD   = POSTGRES_PWD
    POSTGRES_HOSTNAME   = POSTGRES_HOSTNAME
    POSTGRES_PORT       = POSTGRES_PORT
    POSTGRES_DATABASE   = POSTGRES_DATABASE  
    engine = create_engine("postgresql://{0}:{1}@{2}:{3}/{4}".format(
        POSTGRES_USER,
        POSTGRES_PASSWORD,
        POSTGRES_HOSTNAME,
        POSTGRES_PORT,
        POSTGRES_DATABASE
    ))
    return engine
 except Exception as e:
    #print(e)
    print ()     

def horas (hora_actual):   
 try:
   hora_inicial = hora_actual - timedelta(minutes=3)
   hora_inicial = hora_inicial.strftime('%Y-%m-%d %H:%M')
   hora_final = hora_actual - timedelta(minutes=2)
   hora_final = hora_final.strftime('%Y-%m-%d %H:%M')
   return hora_inicial,hora_final
 except Exception as e:
    #print(e)
    print ()      
    
def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
            description="ETL Linet ws")
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    required.add_argument('--db-aws', dest="awsconnection", help="bd connection file aws", required=True)
    required.add_argument('--db-ws', dest="awconnection", help="bd connection file ws", required=True)
    args = parser.parse_args()
    return args

def lerrJson(args):
   with open (args) as credenciales:
      return json.load(credenciales)
   