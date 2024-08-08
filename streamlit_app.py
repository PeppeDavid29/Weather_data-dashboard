import streamlit as st
import pandas as pd
import psycopg2

from Weather_Api import fetch_weather_data 
from Weather_data_injection import inject_data
from Weather_data_transform import transform_data

# Sample data
data = {"city": "New York", "temperature": 75}  # Replace with actual data

# Call the transform_data function
transformed_data = transform_data(data)

# Print the result
print(transformed_data)  # This will print the transformed data

# Example: Displaying in Streamlit
st.write("Transformed Data:", transformed_data)
