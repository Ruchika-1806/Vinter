### Api to serve the weather data. This assumes an exsisiting postgres database with the name mydatabse

import psycopg2
import psycopg2.extras as extras
from datetime import date
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import numpy as np
from sqlalchemy import create_engine
import weather
from meteo import get_weather_data

# Define the database connection parameters
db_params = {
    "user": "ruchika",
    "password": "",
    "host": "localhost",
    "port": "5432",
    "database": "mydatabase"
}

# Define a connection function to the database
def connect_db():
    try:
        conn = psycopg2.connect(**db_params)
        return conn, "No Error"
    except Exception as e:
        print(e)
        return None, e

# Define a Pydantic model for the request body
class WeatherRequest(BaseModel):
    city_name: str
    start_date: date
    end_date: date

# Define the FastAPI app
app = FastAPI()

# Define the route to retrieve weather data
@app.get("/weather")
def get_weather(city_name: str, start_date: date, end_date: date):
    start_date=str(start_date)
    end_date=str(end_date)
    # Connect to the database
    conn, error = connect_db()
    if conn is None:
        return {"error": "Failed to connect to the database\n"+str(error)}
    
    # Define the SQL query to retrieve weather data
    sql = f"SELECT * FROM weather WHERE location like '{city_name}' AND date_time BETWEEN DATE('{start_date}') AND DATE('{end_date}');"
    print(f"\n===========\n{sql}\n==========\n")
    
    # Execute the query and retrieve the results
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    
    # If no data was retrieved, get the latest data from the Meteomatics API and store it in the database
    if not rows:
        conn.close()

        engine = create_engine('postgresql://ruchika@localhost:5432/mydatabase')

        weather_data = get_weather_data(city_name, start_date, end_date)
        if weather_data is None:
            return {"error": "Failed to retrieve weather data from Meteomatics API"}
        
        weather_data.to_sql(name="weather", con=engine, if_exists="append", index=False)
        
        # tuples = [tuple(row) for row in weather_data.to_numpy()]
        # cols = ','.join(list(weather_data.columns))

        # query = "INSERT INTO weather (Date, Temperature, Humidity, Wind_Speed, Precipitation, City) VALUES %s"

        # # # Execute the SQL statement
        # # extras.execute_values(cur, query, tuples)
        
        # # Insert the new weather data into the database
        # # cur.execute("INSERT INTO weather (Date, Temperature, Humidity, Wind_Speed, Precipitation, City) VALUES %s",
        # #             (tuples,))
        # conn.commit()
        
        # Retrieve the new weather data from the database
        conn, error = connect_db()
        # Execute the query and retrieve the results
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        
    # Close the database connection and return the weather data
    conn.close()
    return {"data": rows}

