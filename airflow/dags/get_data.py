from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import json
from datetime import datetime, timedelta

import logging
from sqlalchemy import create_engine, event
import pandas as pd
import requests
from airflow.operators.python import PythonOperator

from airflow import DAG

default_args = {
    "owner": "ricky",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

api_url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab"
staging_db_constring = "mysql://root:mysql@192.168.122.17:3307/finalproject"
dwh_constring = "postgresql://postgres:postgres@192.168.122.17:5435/dwh"


def get_data():
    r = requests.get(api_url)
    content = r.json()["data"]["content"]
    with open("dags/covid_data.json", "w") as f:
        json.dump(content, f)
    return content


def insert_data_staging():
    content = get_data()
    df = pd.DataFrame(content)
    df.to_sql("covid_jabar", staging_db_constring, if_exists="replace", index=False)


def create_dim_province():
    engine = create_engine(staging_db_constring)
    sql_query = pd.read_sql_query(
        '''select distinct kode_prov as province_id, nama_prov as province_name from covid_jabar''', engine)
    df = pd.DataFrame(sql_query, columns = ['province_id', 'province_name'])
    print(df.to_string(index=False))

    engine_postgres = create_engine(dwh_constring)
    df.to_sql('dim_province', engine_postgres, index=False, if_exists='replace')


def create_dim_district():
    engine = create_engine(staging_db_constring)
    sql_query = pd.read_sql_query(
        '''select kode_kab as district_id, kode_prov as province_id, nama_kab as district_name from covid_jabar''', engine)
    df = pd.DataFrame(sql_query, columns = ['district_id', 'province_id','district_name'])
    print(df.to_string(index=False))

    engine_postgres = create_engine(dwh_constring)
    df.to_sql('dim_district', engine_postgres, index=False, if_exists='replace')


def create_dim_case():
    engine = create_engine(staging_db_constring)
    sql_query = pd.read_sql_query(
        '''select closecontact_dikarantina, closecontact_discarded, closecontact_meninggal, confirmation_meninggal, confirmation_sembuh, probable_diisolasi, probable_discarded, probable_meninggal, suspect_diisolasi, suspect_discarded, suspect_meninggal from covid_jabar''', engine)
    df = pd.DataFrame(sql_query, columns = ['closecontact_dikarantina', 'closecontact_discarded', 'closecontact_meninggal', 'confirmation_meninggal', 'confirmation_sembuh', 'probable_diisolasi', 'probable_discarded', 'probable_meninggal', 'suspect_diisolasi', 'suspect_discarded', 'suspect_meninggal'])
    print(df.to_string(index=False))

    df['id'] = range(1, len(df) + 1)
    
    print(df.to_string(index=False))

    df_melted = pd.melt(df, id_vars=["id"], value_vars=['closecontact_dikarantina', 'closecontact_discarded', 'closecontact_meninggal',
                                                         'confirmation_meninggal', 'confirmation_sembuh', 'probable_diisolasi', 'probable_discarded',
                                                         'probable_meninggal', 'suspect_diisolasi', 'suspect_discarded', 'suspect_meninggal'])
    

    df_melted[['id', 'status_name', 'status_detail']] = df_melted.apply(lambda row: pd.Series([row['id'], row['variable'].split('_')[0], row['variable'].split('_')[1]]), axis=1)
    
    df_melted['status'] = df_melted['status_name'] + '_' + df_melted['status_detail']
    
    df_melted = df_melted.drop(['variable', 'value'], axis=1)
    
    print(df_melted)

    engine_postgres = create_engine(dwh_constring)
    df_melted.to_sql('dim_case', engine_postgres, index=False, if_exists='replace')


def create_fact_district_daily():
    engine = create_engine(staging_db_constring)
    sql_query = '''SELECT * FROM covid_jabar'''
    
    df = pd.read_sql_query(sql_query, engine)
    print(df.head())

    engine_postgres = create_engine(dwh_constring)
    df_pg = pd.read_sql_table('dim_case', engine_postgres)

    print(df_pg.head())

    df_piv = pd.DataFrame(df_pg)
    pivot_df = df_piv.pivot_table(index='status', aggfunc='size').reset_index(name='count')

    print(pivot_df)

def create_fact_province_daily():
    pass


with DAG(
    dag_id="final_project_data_covid_etl",
    default_args=default_args,
    start_date=datetime(2023, 11, 20),
    catchup=False,
    schedule_interval="@daily",
    ) as dag :

    op_print_hello_world = BashOperator(
        task_id='print_hello_world',
        bash_command='echo "HelloWorld!"'
    )
    insert_data_staging = PythonOperator(
        task_id="insert_data_staging", 
        python_callable=insert_data_staging
    )
    create_dim_province = PythonOperator(
        task_id="create_dim_province",
        python_callable=create_dim_province
    )
    create_dim_district = PythonOperator(
        task_id="create_dim_district",
        python_callable=create_dim_district
    )
    create_dim_case = PythonOperator(
        task_id="create_dim_case",
        python_callable=create_dim_case
    )
    create_fact_district_daily = PythonOperator(
        task_id="create_fact_district_daily",
        python_callable=create_fact_district_daily
    )
    create_fact_province_daily = PythonOperator(
        task_id="create_fact_province_daily",
        python_callable=create_fact_province_daily
    )

op_print_hello_world >> insert_data_staging >> create_dim_province >> create_dim_district >> create_dim_case >> create_fact_district_daily >> create_fact_province_daily