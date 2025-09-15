from fastapi import FastAPI
import polars as pl
from supabase import create_client, Client
from dotenv import load_dotenv
import os
from geopy.distance import geodesic

load_dotenv()

app = FastAPI()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_data(data):
        for row in data:
            split_data = str(row).split(",")
            lat_long = f"{split_data[3]}, {split_data[4]}"
            city_state = f"{split_data[1]}, {split_data[2]}"
            return str(lat_long), str(city_state)

@app.get("/latlong_data")
async def latlong_data():
    # Fetch data from Supabase
    response = supabase.table("geoData").select("*").execute()
    return {"data": response}

latlong_list = []

@app.get("/process_distance")
async def process_distance():
    latlong_data_pull = supabase.table("geoData").select('id', 'latitude', 'longitude').execute()
    latlong_list.append(latlong_data_pull)
    for row in latlong_list:
        lat = str(row).split(", ")[1]
        lat2 = lat.split(":")[1]
        lat3 = lat2.replace("'", "")
        long = str(row).split(", ")[2]
        long2 = long.split(":")[1]
        long3 = long2.replace("'", "")
        long4 = long3.strip(')')
        city_id = str(row).split(", ")[0]
        id = city_id.split(":")[1]
        latlong = lat3 + ", " + long4
        return latlong, id
    


