from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

def process_weather_data(weather_df):
    if weather_df is None:
        print("Error: The DataFrame is None. Please check the input data.")
        return None

    # Display the first few rows to verify the DataFrame (Pandas equivalent of Spark's printSchema)
    print("Processing Pandas DataFrame:")
    print(weather_df.head())
    
    # Filter out rows where the 'location' column is null
    processed_df = weather_df[weather_df['location'].notnull()]
    return processed_df

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .getOrCreate()

try:
    # Read the weather data CSV file into a Spark DataFrame
    df_spark = spark.read.csv(r"C:/Users/sharo/weather_data.csv", header=True, inferSchema=True)
    
    # Check if the DataFrame is successfully loaded
    if df_spark is None or df_spark.count() == 0:
        print("Error: Failed to read the CSV file or the DataFrame is empty. Please check the file path and format.")
    else:
        # Show the first few rows to verify the data
        df_spark.show()

        # Process the weather data to filter out rows with null 'location'
        processed_df = process_weather_data(df_spark)

        if processed_df is not None:
            # Order the DataFrame by 'location'
            ordered_df = processed_df.orderBy("location")

            # Define the output path for the ordered CSV
            output_csv_path = "D:/sql/ordered_by_city.csv"

            # Save the ordered DataFrame as a CSV file
            ordered_df.coalesce(1).write.csv(output_csv_path, header=True, mode="overwrite")

            print(f"Weather data ordered by city successfully saved to {output_csv_path}")

except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    # Stop the Spark session
    spark.stop()
