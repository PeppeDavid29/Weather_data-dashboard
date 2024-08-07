import requests
import queue
import threading
import csv
import time

def fetch_weather(api_key, location):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={location}&appid={api_key}&units=metric"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        weather = {
            "location": data["name"],
            "temperature": data["main"]["temp"],
            "description": data["weather"][0]["description"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "wind_speed": data["wind"]["speed"]
        }
        return weather
    else:
        return None

def process_weather_data(q, csv_writer, lock):
    while not q.empty():
        location, api_key = q.get()
        weather_data = fetch_weather(api_key, location)
        if weather_data:
            print(f"Weather in {weather_data['location']}:")
            print(f"Temperature: {weather_data['temperature']}°C")
            print(f"Description: {weather_data['description']}")
            print(f"Humidity: {weather_data['humidity']}%")
            print(f"Pressure: {weather_data['pressure']} hPa")
            print(f"Wind Speed: {weather_data['wind_speed']} m/s")
            print()

            with lock:
                csv_writer.writerow(weather_data)
        else:
            print(f"Failed to retrieve weather data for {location}")
        q.task_done()

def main():
    api_key = '1958bf82f3cb9ccc4a2994e1e1b11b65'
    states = [
        'Kerala', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 
        'Chhattisgarh', 'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh', 
        'Jharkhand', 'Karnataka', 'Madhya Pradesh', 'Maharashtra', 'Manipur', 
        'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Punjab', 'Rajasthan', 
        'Sikkim', 'Tamil Nadu', 'Hyderabad', 'Tripura', 'Uttar Pradesh'
    ]

    q = queue.Queue()
    lock = threading.Lock()

    # Add states to the queue
    for state in states:
        q.put((state, api_key))

    with open(r'C:\Users\sharo\weather_data.csv', mode='w', newline='', encoding='utf-8') as file:
        fieldnames = ["location", "temperature", "description", "humidity", "pressure", "wind_speed"]
        csv_writer = csv.DictWriter(file, fieldnames=fieldnames)
        csv_writer.writeheader()

        num_threads = 5
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=process_weather_data, args=(q, csv_writer, lock))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

if __name__ == "__main__":
    main()



