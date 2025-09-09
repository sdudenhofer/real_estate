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


def lifespan(app: FastAPI):
    create_db_and_tables()

app = FastAPI()

class geoData(SQLModel, table=True):
    id: int = Field(default= None, primary_key=True)
    city: str = Field(max_length=100)
    state: str = Field(max_length=2)
    latitude: str = Field(max_length=10)
    longitude: str = Field(max_length=10)

def create_latlong(session: SessionDep):
    latlong = geoData.model_validate(data)
    for row in data[['RegionName', 'State']].iter_rows():
        city_state = f"{row[0]}, {row[1]}"
        location = geolocator.geocode(city_state)
        # Calculate distance from a reference point (e.g., New York City)


    print(f"Distance from New York City: {distance} miles")

if __name__ == "__main__":
    create_latlong(Session)
