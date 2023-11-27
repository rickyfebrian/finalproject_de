import requests
import json
from sqlalchemy import create_engine
import pandas as pd
import logging

def request_api():
    # Get data
    url = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab'
    headers = {'accept': 'application/json'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Check for errors in the HTTP response
        data = response.json()
        print(data)

        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(data)

        # Save the raw response to a file
        with open('data_from_api.txt', 'w') as file:
            json.dump(data, file)

        # Connection
        db_params = {
            'user': 'mysql',
            'password': 'mysql',
            'host': '192.168.122.17',
            'port': 3307,
            'database': 'covid',
        }

        # Establish MySQL connection using SQLAlchemy
        engine = create_engine(f'mysql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

        # Table name in MySQL
        table_name = 'covid_jabar'

        # Write DataFrame to MySQL using pandas
        df.to_sql(table_name, engine, index=False, if_exists='replace')

        print("Data successfully written to MySQL!")

    except Exception as e:
        print(f"Error: {e}")
        print(f"Additional error information: {repr(e)}")

    finally:
        # Dispose of the engine to close the database connection
        engine.dispose()

    logging.info("Success")

# Call the function
request_api()
