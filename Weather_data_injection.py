from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from Weather_data_injection import inject_data  # Ensure this is correct

spark = SparkSession.builder \
    .appName("WeatherDataProcessing") \
    .getOrCreate()

try:
    df_spark = spark.read.csv(r"C:/Users/sharo/weather_data.csv", header=True, inferSchema=True)
    df_spark.show()

    ordered_df = df_spark.orderBy("location")

    output_csv_path = "D:/sql/ordered_by_city.csv"

    ordered_df.coalesce(1).write.csv(output_csv_path, header=True, mode="overwrite")

    print(f"Weather data ordered by city successfully saved to {output_csv_path}")

except Exception as e:
    print(f"Unexpected error: {e}")

finally:
    spark.stop()

