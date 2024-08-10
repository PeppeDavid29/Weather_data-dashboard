from Weather_Api import fetch_weather
from Weather_data_injection import process_weather_data
from Weather_data_transform import store_weather_data
import pandas as pd

def main():
    api_key = '1958bf82f3cb9ccc4a2994e1e1b11b65'
    states = [
        'Kerala', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 
        'Chhattisgarh', 'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh', 
        'Jharkhand', 'Karnataka', 'Madhya Pradesh', 'Maharashtra', 'Manipur', 
        'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Punjab', 'Rajasthan', 
        'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh'
    ]

    db_config = {
        'dbname': 'postgre_weather_data',
        'user': 'postgres',
        'password': 'shah123',
        'host': 'localhost',
        'port': '5432'
    }

    for state in states:
        try:
    
            weather_data = fetch_weather(api_key, state)
            if weather_data is None:
                print(f"Weather data for {state} is None, skipping...")
                continue
            
           
            weather_df = pd.DataFrame([weather_data])
            
           
            processed_df = process_weather_data(weather_df)
            if processed_df is None or processed_df.empty:
                print(f"Processed DataFrame for {state} is None or empty, skipping...")
                continue

            
            store_weather_data(processed_df, db_config)
            print(f"Data for {state} inserted successfully.")

        except Exception as e:
            print(f"An error occurred while processing {state}: {e}")

if __name__ == "__main__":
    main()
