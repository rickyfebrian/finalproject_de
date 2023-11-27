import mysql.connector
import requests
import json
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

        # Save the raw response to a file
        with open('data_from_api.txt', 'w') as file:
            json.dump(data, file)

    except requests.exceptions.RequestException as e:
        print(f"Error in making the API request: {e}")

    # Connection
    mysql_config = {
        'host': '192.168.122.17',
        'port': 3307,  # Corrected to an integer, not a string
        'user': 'mysql',
        'password': 'mysql',
        'database': 'covid',
    }

    try:
        # Establish MySQL connection
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()

        # Drop table if it exists
        drop_table_query = "DROP TABLE IF EXISTS covid_jabar"
        cursor.execute(drop_table_query)

        # Your MySQL insertion logic goes here
        # For example:
        # cursor.execute("INSERT INTO your_table_name (column1, column2, ...) VALUES (%s, %s, ...)", (value1, value2, ...))

        # Commit changes
        connection.commit()

        print("Data successfully written to MySQL!")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

    # Assuming 'data' is a pandas DataFrame, you can use the following line to write it to MySQL
    # data.to_sql(con=connection, name='covid_jabar', index=False, if_exists='replace')
    logging.info("Success")

# Call the function
request_api()
