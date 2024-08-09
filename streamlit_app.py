import streamlit as st
import pandas as pd
import psycopg2

from Weather_Api import fetch_weather
from utils import inject_data
from Weather_data_transform import transform_data

def get_db_connection():
  conn = psycopg2.connect(
dbname = 'postgre_weather_data',
user = 'postgres',
password = 'shah123',
host = 'localhost',
port = '5432'
    )
  return conn

# Fetch Weather Data
if st.button('Fetch Weather Data'):
    weather_data = fetch_weather_data()
    st.write("Weather Data Fetched")

# Inject Data into Database
if st.button('Inject Data into Database'):
    inject_data(weather_data)
    st.write("Data Injected into Database")

# Transform Data
if st.button('Transform Data'):
    transformed_data = transform_data(weather_data)
    st.write("Data Transformed")
    st.dataframe(transformed_data)

# Display Data from Database
if st.button('Show Data'):
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM your_table_name", conn)
    st.dataframe(df)
    conn.close()
