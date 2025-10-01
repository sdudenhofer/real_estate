import polars as pl
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from sqlmodel import SQLModel, Field
from dotenv import load_dotenv
import os
import duckdb
from time import sleep
import pyarrow as pa

load_dotenv()

con = duckdb.connect('real_estate.db')
geolocator = Nominatim(user_agent='dudenhofer_realestate', timeout=100)
raw_data = "./data/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
q = (
    pl.scan_csv(raw_data)    
      .sort("State")  
      .select(["State", "RegionName"])
)
initial_data = pl.scan_csv(raw_data).sort("State")
initial_df = initial_data.collect()
data = q.collect()             

con.execute("""
                CREATE TABLE IF NOT EXISTS city_geocode (
                    id INTEGER,
                    city_state TEXT,
                    latitude REAL,
                    longitude REAL
                )
            """)
con.commit()

con.sql("CREATE TABLE IF NOT EXISTS redfin_data AS SELECT * FROM initial_df")


count = 0
for rows in data.iter_rows(named=True):
        city_state = f"{rows['RegionName']}, {rows['State']}"
        try:
            location = geolocator.geocode(city_state)
        except GeocoderTimedOut as g:
            print(f"GeocoderTimedOut for {city_state}: {g}")
            location = None
            sleep(5)
        if location:
            try:
                con.execute(f"INSERT INTO city_geocode (id, city_state, latitude, longitude) VALUES ({count}, '{city_state}', {location.latitude}, {location.longitude})")
                con.commit()
                print(f"Inserted: {city_state} -> ({location.latitude}, {location.longitude})")
            except Exception as e:
                print(f"Error inserting {city_state}: {e}")
        else:
            print(f"No Latitude, Longitude Data for: {city_state}")
        count += 1
