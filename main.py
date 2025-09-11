import polars as pl
from geopy.geocoders import Nominatim
from sqlmodel import SQLModel, Field, create_engine, Session, select, table
from supabase import create_client, Client
from dotenv import load_dotenv
import os

load_dotenv()
geolocator = Nominatim(user_agent='dudenhofer_realestate', timeout=100)
raw_data = "./data/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
reader = pl.read_csv(raw_data)
data = pl.DataFrame(reader)

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)

class geoData(SQLModel, table=True):
    id: int = Field(default= None, primary_key=True)
    city: str = Field(max_length=100)
    state: str = Field(max_length=2)
    latitude: str = Field(max_length=10)
    longitude: str = Field(max_length=10)

def create_latlong():
    for rows in data[['RegionName', 'State']].iter_rows(named=True):
        city_state = f"{rows['RegionName']}, {rows['State']}"
        location = geolocator.geocode(city_state)
        try:
            response = (
                supabase.table("geoData")
                .upsert({
                    "city": rows['RegionName'],
                    "state": rows['State'],
                    "latitude": location.latitude if location else None,
                    "longitude": location.longitude if location else None
                }, ignore_duplicates=True)
                .execute()
            )
            print(response)
        except Exception as exception:
            print(exception)

if __name__ == "__main__":
    create_latlong()