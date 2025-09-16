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

def parse_data(data, data1):
    for row in data:
        initial_strip = str(row).split(",")
        secondary_strip = str(initial_strip).strip("}")
        tertiary_strip = str(secondary_strip).strip('"')
        return tertiary_strip
    for rows in data1:
        initial_strip1 = str(rows).split(",")
        secondary_strip1 = str(initial_strip1).strip("}")
        tertiary_strip1 = str(secondary_strip1).strip('"')
        return tertiary_strip1

@app.get("/latlong_data")
async def latlong_data():
    # Fetch data from Supabase
    response = supabase.table("geoData").select("*").execute()
    return {"data": response}

latlong_list = []
secondary_list = []

@app.get("/process_distance")
async def process_distance():
    latlong_data_pull = supabase.table("geoData").select('id', 'latitude', 'longitude').execute()
    secondary_data_pull = supabase.table("geoData").select('id', 'latitude', 'longitude').execute()
    latlong_list.append(latlong_data_pull)
    secondary_list.append(secondary_data_pull)
    output = parse_data(latlong_list, secondary_list)
    return output
    

    


