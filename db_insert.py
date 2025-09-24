import polars as pl
from geopy.geocoders import Nominatim
from sqlmodel import SQLModel, Field
from dotenv import load_dotenv
import os

load_dotenv()
geolocator = Nominatim(user_agent='dudenhofer_realestate', timeout=100)
raw_data = "./data/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
q = (
    pl.scan_csv(raw_data)       # lazy, no full load
      .sort("State")  # predicate pushdown
      .select(["State", "RegionName"]) # projection pushdown
)

data = q.collect() 
for rows in data.iter_rows(named=True):
        city_state = f"{rows['RegionName']}, {rows['State']}"
        location = geolocator.geocode(city_state)
        if location:
            print(location.latitude, location.longitude)
        else:
            print(f"No Latitude, Longitude Data for: {city_state}")

# if __name__ == "__main__":
#    create_latlong()