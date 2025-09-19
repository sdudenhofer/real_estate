from fastapi import FastAPI
import polars as pl
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import pandas as pd
import polars as pl
import json
from sqlalchemy import create_engine


load_dotenv()

app = FastAPI()

# Fetch variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Construct the SQLAlchemy connection string
DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)
# If using Transaction Pooler or Session Pooler, we want to ensure we disable SQLAlchemy client side pooling -
# https://docs.sqlalchemy.org/en/20/core/pooling.html#switching-pool-implementations
# engine = create_engine(DATABASE_URL, poolclass=NullPool)

# Test the connection
try:
    conn = engine.connect()
except SystemError as e:
    print(f"Error: {e}")

@app.get("/latlong_data")
async def latlong_data():
    # Fetch data from Supabase
    response = supabase.table("geoData").select("*").execute()
    return {"data": response}

latlong_list = []
secondary_list = []

@app.get("/process_distance")
async def process_distance():
    query = "SELECT * FROM geoData"
    polar_df = pl.read_database(query, engine)
    return polar_df


