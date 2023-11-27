import requests
import json
from sqlalchemy import create_engine
import pandas as pd
import logging

def request_api():
    try:
        # Get data
        url = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab'
        headers = {'accept': 'application/json'}

        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Check for errors in the HTTP response
        data = response.json()
        print(data)

        # Connection and Load
        db_params = {
            "user": 'root',
            "password": 'mysql',
            "host": '192.168.122.17',
            "port": 3307,
            "database": 'mysql'
        }

        engine = create_engine(f'mysql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
        table_name = "users_visitor_ricky"

        # Assuming 'data' is a list of tuples where each tuple contains a list of dictionaries and a status code
        for entry in data:
            engine.execute(
                f"INSERT INTO {table_name} (data, status_code) VALUES (%s, %s)",
                (json.dumps(entry[0]), entry[1])
            )

        print("Data successfully loaded into MySQL!")

    except Exception as e:
        print(f"Error: {e}")
        print(f"Additional error information: {repr(e)}")

    finally:
        # Dispose of the engine to release resources
        engine.dispose()

# Call the function
request_api()
