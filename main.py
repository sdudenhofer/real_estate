import polars as pl
from fastapi import FastAPI
import great_expectations as gx
import ray

# Initialize modules
ray.init()
app = FastAPI()

@ray.remote
async def import_data():
    

