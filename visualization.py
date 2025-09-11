from fastapi import FastAPI
import polars as pl
from supabase import create_client, Client
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv()

app = FastAPI()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.get("/visualize")
async def visualize_data():
    # Fetch data from Supabase
    response = supabase.table("geoData").select("*").execute()
    # data = jsonify(response)
    df = pl.dataframe(response.data)
    grouped_by = df.groupby("state").agg()
    return_data = pl.json_normalize(grouped_by)
    return {"data": return_data}