## sets up the weather table if it doesn't exsist

from meteo import get_weather_data, get_dates
from sqlalchemy import create_engine

engine = create_engine('postgresql://ruchika@localhost:5432/mydatabase')

city = "New York"
start_date, end_date = get_dates()
data = get_weather_data(city, start_date, end_date)

with engine.begin() as cnx:
    query ="DROP TABLE IF EXISTS weather;"
    cnx.execute(query)
data.to_sql(name='weather', con=engine, if_exists = 'replace', index=False)
