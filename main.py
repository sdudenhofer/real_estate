from operator import sub
import pandas as pd
import polars as pl
from fastapi import FastAPI, Depends
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.adapters import AioHTTPAdapter
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from sqlmodel import SQLModel, Field, create_engine, Session, select, table
from typing import Annotated
from contextlib import asynccontextmanager


geolocator = Nominatim(user_agent='dudenhofer_realestate', timeout=100)
raw_data = "./data/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
reader = pl.read_csv(raw_data)
data = pl.DataFrame(reader)

db_file = "./data/realestate.db"
engine = create_engine(f"sqlite:///{db_file}")

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()

app = FastAPI(lifespan=lifespan)

class geoData(SQLModel, table=True):
    id: int = Field(default= None, primary_key=True)
    city: str = Field(max_length=100)
    state: str = Field(max_length=2)
    latitude: str = Field(max_length=10)
    longitude: str = Field(max_length=10)

async def create_latlong(latlong: geoData, session: SessionDep):
    latlong = await geoData.model_validate(latlong)
    for row in data[['RegionName', 'State']].iter_rows():
        city_state = f"{row[0]}, {row[1]}"
        location = geolocator.geocode(city_state)
        # Calculate distance from a reference point (e.g., New York City)
        reference_point = (40.7128, -74.0060)
        if location:
            distance = geodesic(reference_point, (location.latitude, location.longitude)).miles
        else:
            distance = None

    print(f"Distance from New York City: {distance} miles")


class realEstate(SQLModel, table=True):
    id: int
    cityStateid: int # reference to geoData table
    year: str
    january: float
    february: float
    march: float
    april: float
    may: float
    june: float
    july: float
    august: float
    september: float
    october: float
    november: float
    december: float

@app.get("/api/latlong")
async def latlong_data():
    return()
#async def home():
#    for row in data_list:
#        city = row[2]
#        state = row[5]
#        city_state = f"{city}, {state}"
#        location = geolocator.geocode(city_state)
#        if location:
#            data.loc[(data['RegionName'] == city) & (data['State'] == state), 'Latitude'] = location.latitude
#            data.loc[(data['RegionName'] == city) & (data['State'] == state), 'Longitude'] = location.longitude
#            print(f'Geocoded {city_state}: ({location.latitude}, {location.longitude})')
#        else:
#            print('City Not Found')

if __name__ == "__main__":
    create_latlong()
