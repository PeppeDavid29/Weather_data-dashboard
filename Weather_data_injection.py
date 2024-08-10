from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

def process_weather_data(weather_df):
    if weather_df is None:
        print("Error: The DataFrame is None. Please check the input data.")
        return None

    
    print("Processing Pandas DataFrame:")
    print(weather_df.head())
    
    
    processed_df = weather_df[weather_df['location'].notnull()]
    return processed_df


spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .getOrCreate()

try:
    
    df_spark = spark.read.csv(r"C:/Users/sharo/weather_data.csv", header=True, inferSchema=True)
    
    
    if df_spark is None or df_spark.count() == 0:
        print("Error: Failed to read the CSV file or the DataFrame is empty. Please check the file path and format.")
    else:
        
        df_spark.show()

       
        processed_df = process_weather_data(df_spark)

        if processed_df is not None:
            
            ordered_df = processed_df.orderBy("location")

            
            output_csv_path = "D:/sql/ordered_by_city.csv"

            
            ordered_df.coalesce(1).write.csv(output_csv_path, header=True, mode="overwrite")

            print(f"Weather data ordered by city successfully saved to {output_csv_path}")

except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    
    spark.stop()
