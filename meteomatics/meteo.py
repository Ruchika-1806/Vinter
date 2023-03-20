## Main code to get weather data from metomatics

import requests
import pandas as pd
from opencage.geocoder import OpenCageGeocode
import io
from datetime import datetime, timedelta
import pytz


def get_weather_data(city, start_date, end_date):
    # Convert city name to latitude and longitude
    geocoder = OpenCageGeocode("a986b4597fa3401ab751d580ce14a05c")
    results = geocoder.geocode(city, no_annotations='1')
    lat = results[0]['geometry']['lat']
    lon = results[0]['geometry']['lng']
    
    # Construct API request URL
    url = f"https://api.meteomatics.com/{start_date}T00:00:00Z--{end_date}T23:59:59Z:PT1H/t_2m:C,relative_humidity_2m:p,wind_speed_10m:kmh,precip_1h:mm/{lat},{lon}/csv"
    
    # Call Meteomatics API with username and password
    response = requests.get(url, auth=('self_ruchika', '0JEVPdx57d'))
    
    # Convert CSV response to pandas DataFrame
    data = pd.read_csv(io.StringIO(response.text), sep=";")
    
    # Rename columns to be more human-readable
    data = data.rename(columns={
        'validdate': 'date_time',
        't_2m:C': 'temperature',
        'relative_humidity_2m:p': 'humidity',
        'wind_speed_10m:kmh': 'wind_speed',
        'precip_1h:mm': 'precipitation'
    })

    data['location'] = city
    #print(data.head())
    # Convert time column to datetime format
    data['date_time'] = pd.to_datetime(data['date_time'])
    data['date_time'] = pd.to_datetime(data['date_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    data['date_time'] = pd.to_datetime(data['date_time'])
    
    return data

def get_dates():
    # Get current date in UTC time zone
    utc = pytz.UTC
    current_date = datetime.now(utc)
    current_date_str = current_date.strftime('%Y-%m-%d')
    # print("Current date in UTC:", current_date_str)

    # Get tomorrow's date in UTC time zone
    tomorrow = current_date + timedelta(days=1)
    tomorrow_str = tomorrow.strftime('%Y-%m-%d')
    # print("Tomorrow's date in UTC:", tomorrow_str)

    return current_date_str, tomorrow_str



# # TEST
# print("================\n WEATHER DATA \n================\n")
# city = "New York"
# start_date, end_date = get_dates()
# data = get_weather_data(city, start_date, end_date)
# print(data)




