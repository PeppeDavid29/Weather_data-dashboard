from Weather_Api import fetch_weather
from Weather_data_injection import process_weather_data
from Weather_data_transform import store_weather_data

def main():
    api_key = '1958bf82f3cb9ccc4a2994e1e1b11b65'
    states = [
        'Kerala', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 
        'Chhattisgarh', 'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh', 
        'Jharkhand', 'Karnataka', 'Madhya Pradesh', 'Maharashtra', 'Manipur', 
        'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Punjab', 'Rajasthan', 
        'Sikkim', 'Tamil Nadu', 'Hyderabad', 'Tripura', 'Uttar Pradesh'
    ]

    db_config = {
        'dbname': 'postgre_weather_data',
        'user': 'postgres',
        'password': 'shah123',
        'host': 'localhost',
        'port': '5432'
    }

    for state in states:
        api_url = f"http://api.openweathermap.org/data/2.5/weather?q={state}&appid={api_key}&units=metric"
        
        # Fetch weather data
        weather_data = fetch_weather(api_url, api_key)
        
        # Process the weather data
        processed_df = process_weather_data(weather_data)
        
        # Store the processed data in the database
        store_weather_data(processed_df, db_config)

if __name__ == "__main__":
    main()

