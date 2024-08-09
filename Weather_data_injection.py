from pyspark.sql import SparkSession
from Weather_data_injection import inject_data

def inject_data(data):
    # Assuming you want to inject data somewhere, add the logic here.
    # Example: print the data
    print(f"Injecting data: {data}")

spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .getOrCreate()

try:
    # Read CSV data into a Spark DataFrame
    df_spark = spark.read.csv(r"C:/Users/sharo/weather_data.csv", header=True, inferSchema=True)
    df_spark.show()

    # Order DataFrame by location
    ordered_df = df_spark.orderBy("location")

    # Define output path for the ordered data
    output_csv_path = "D:/sql/ordered_by_city.csv"

    # Save the ordered data to a new CSV file
    ordered_df.coalesce(1).write.csv(output_csv_path, header=True, mode="overwrite")

    print(f"Weather data ordered by city successfully saved to {output_csv_path}")

except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    # Stop the Spark session
    spark.stop()


