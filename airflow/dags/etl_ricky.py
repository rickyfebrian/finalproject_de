#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow.operators.python import PythonOperator

from airflow import DAG

default_args = {
    "owner": "oce",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

api_url = "http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab"
staging_db_constring = "mysql://root:mysql@192.168.122.17:3307/finalproject"
dwh_constring = "postgresql://postgres:postgres@digiskola-postgresql-db-1:5432/dwh"


def get_data():
    r = requests.get(api_url)
    content = r.json()["data"]["content"]
    with open("dags/covid_data.json", "w") as f:
        json.dump(content, f)
    return content


def ingest_data_staging():
    content = get_data()
    df = pd.DataFrame(content)
    df.to_sql("covid_jabar", staging_db_constring, if_exists="replace", index=False)


#def create_dim_province():
#    pass
#
#
#def create_dim_district():
#    pass
#
#
#def create_dim_case():
#    pass


with DAG(
    dag_id="data_covid_etl",
    default_args=default_args,
    start_date=datetime(2023, 11, 20),
    schedule_interval='@daily',
):
    ingest_staging_db = PythonOperator(
        task_id="ingest_staging_db", python_callable=ingest_data_staging
    )
