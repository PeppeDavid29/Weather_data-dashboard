import os
import pandas as pd
import psycopg2

def setup_database_and_insert_data(dbname, user, password, host, port, csv_directory):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cur = conn.cursor()
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ordered_by_city (
                city TEXT PRIMARY KEY,
                avg_temperature REAL,
                avg_humidity REAL,
                avg_wind_speed REAL
            );
        ''')

        print("Files in directory:", os.listdir(csv_directory))

        file_name = "ordered_by_city.csv"
        file_path = os.path.join(csv_directory, file_name)
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return  

        df = pd.read_csv(file_path)
        df = df.rename(columns={
            'location': 'city',
            'temperature': 'avg_temperature',
            'humidity': 'avg_humidity',
            'wind_speed': 'avg_wind_speed'
        })
        df = df[['city', 'avg_temperature', 'avg_humidity', 'avg_wind_speed']]

        for index, row in df.iterrows():
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
        print(f"An error occurred: {e}")

csv_directory = r"C:\Users\sharo"  
dbname = 'postgre_weather_data'
user = 'postgres'
password = 'shah123'
host = 'localhost'
port = '5432'

setup_database_and_insert_data(dbname, user, password, host, port, csv_directory)
