from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import json
from sqlalchemy import create_engine
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.operators.python import PythonOperator

api_url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab"
staging_db_constring = "mysql://root:mysql@192.168.122.17:3307/finalproject"
dwh_constring = "postgresql://postgres:postgres@digiskola-postgresql-db-1:5432/dwh"

def create_dim_province():
#def read_sql_and_print_df(**kwargs):
    engine = create_engine(staging_db_constring)
    sql_query = pd.read_sql_query(
        '''select kode_prov as province_id, nama_prov as province_name from covid_jabar''', engine)
df = pd.DataFrame(sql_query, columns = ['province_id', 'province_name'])

print(df)


#def read_sql_and_print_df(**kwargs):
#    engine = create_engine(staging_db_constring)
#    sql_query = pd.read_sql_query(
#        '''SELECT kode_prov AS province_id, nama_prov AS province_name FROM covid_jabar''', engine)
#    df = pd.DataFrame(sql_query, columns=['province_id', 'province_name'])
#    
#print(df)

#def read_sql_and_print_df(**kwargs):
#    engine = create_engine(staging_db_constring)
#    sql_query = pd.read_sql_query(
#        '''SELECT kode_prov AS province_id, nama_prov AS province_name FROM covid_jabar''', engine)
#    df = pd.DataFrame(sql_query, columns=['province_id', 'province_name'])
#    print(df)