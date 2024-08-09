import os
import pandas as pd
import psycopg2

# Define the store_weather_data function
def store_weather_data(processed_df, db_config):
    try:
        # Establish database connection using the db_config dictionary
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        cur = conn.cursor()
        
        # Create the table if it doesn't exist
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ordered_by_city (
                city TEXT PRIMARY KEY,
                avg_temperature REAL,
                avg_humidity REAL,
                avg_wind_speed REAL
            );
        ''')

        # Insert data into the table
        for index, row in processed_df.iterrows():
            cur.execute('''
                INSERT INTO ordered_by_city (city, avg_temperature, avg_humidity, avg_wind_speed)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (city) DO NOTHING;
            ''', (row['city'], row['avg_temperature'], row['avg_humidity'], row['avg_wind_speed']))

        conn.commit()
        cur.close()
        conn.close()
        print("Data inserted successfully.")
    
    except Exception as e:
        print(f"An error occurred while storing data: {e}")

# Modify the setup_database_and_insert_data function to use store_weather_data
def setup_database_and_insert_data(dbname, user, password, host, port, csv_directory):
    try:
        # Define the database configuration dictionary
        db_config = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }

        # Check if the file exists in the specified directory
        file_name = "ordered_by_city.csv"
        file_path = os.path.join(csv_directory, file_name)
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return  

        # Load the CSV into a DataFrame
        df = pd.read_csv(file_path)
        df = df.rename(columns={
            'location': 'city',
            'temperature': 'avg_temperature',
            'humidity': 'avg_humidity',
            'wind_speed': 'avg_wind_speed'
        })
        df = df[['city', 'avg_temperature', 'avg_humidity', 'avg_wind_speed']]

        # Store the processed DataFrame into the database
        store_weather_data(df, db_config)

    except Exception as e:
        print(f"An error occurred: {e}")

# Configuration for database connection and file directory
csv_directory = r"C:\Users\sharo"  
dbname = 'postgre_weather_data'
user = 'postgres'
password = 'shah123'
host = 'localhost'
port = '5432'

# Run the function
setup_database_and_insert_data(dbname, user, password, host, port, csv_directory)
