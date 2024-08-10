import psycopg2
import pandas as pd
import os

def store_weather_data(df, db_config):
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        cur = conn.cursor()

        # Print debug information
        print("Database connection established.")

        # Create the table if it doesn't exist
        cur.execute('''
            CREATE TABLE IF NOT EXISTS ordered_by_state (
                state TEXT PRIMARY KEY,
                avg_temperature REAL,
                avg_humidity REAL,
                avg_wind_speed REAL
            );
        ''')

        print("Table checked/created successfully.")

        # Insert data into the table
        for index, row in df.iterrows():
            cur.execute('''
                INSERT INTO ordered_by_state (state, avg_temperature, avg_humidity, avg_wind_speed)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (state) DO NOTHING;
            ''', (row['state'], row['avg_temperature'], row['avg_humidity'], row['avg_wind_speed']))

        conn.commit()
        cur.close()
        conn.close()
        print("Data inserted successfully.")
    
    except Exception as e:
        print(f"An error occurred while storing data: {e}")

def read_and_process_csv(csv_directory, db_config):
    try:
        file_name = "ordered_by_state.csv"
        file_path = os.path.join(csv_directory, file_name)
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return

        df = pd.read_csv(file_path)
        df = df.rename(columns={
            'location': 'state',
            'temperature': 'avg_temperature',
            'humidity': 'avg_humidity',
            'wind_speed': 'avg_wind_speed'
        })
        df = df[['state', 'avg_temperature', 'avg_humidity', 'avg_wind_speed']]

        # Store the processed DataFrame into the database
        store_weather_data(df, db_config)

    except Exception as e:
        print(f"An error occurred while processing CSV: {e}")

# Example usage (this code will not run when imported)
if __name__ == "__main__":
    # Configuration for database connection and file directory
    csv_directory = r"C:\Users\sharo"  
    db_config = {
        'dbname': 'postgre_weather_data',
        'user': 'postgres',
        'password': 'shah123',
        'host': 'localhost',
        'port': '5432'
    }

    read_and_process_csv(csv_directory, db_config)
