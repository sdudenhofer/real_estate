import polars as pl
from sqlmodel import SQLModel, Field, create_engine, Session
from dotenv import load_dotenv
import psycopg2
import os
from geopy.distance import geodesic
from random import randint

load_dotenv()
pg_user = os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PASSWORD")
pg_db = os.getenv("POSTGRES_DB")

initial_data = pl.read_csv("data/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv")
latlong_data = pl.read_csv("data/uscities.csv")
initial_df = pl.DataFrame(initial_data)
latlong_df = pl.DataFrame(latlong_data)
general_data = initial_df.select(pl.col("RegionName", "StateName", "RegionID"))
df = general_data.with_columns(
    pl.concat_str(["RegionName", "StateName"], separator=", ").alias("city_state")
)
ll_df = latlong_df.with_columns(
    pl.concat_str(pl.col("city_ascii"), pl.col("state_id"), separator=", ").alias(
        "city_state"
    )
)
# Add a new column for latitude and longitude
df = df.join(ll_df, on="city_state", how="left")


engine = create_engine(f"postgresql://{pg_user}:{pg_password}@localhost/{pg_db}")
session = Session(engine)

try:
    initial_df.write_database(
        connection=engine, table_name="redfin", if_table_exists="replace"
    )
    df.write_database(
        connection=engine, table_name="latlong", if_table_exists="replace"
    )
except Exception as e:
    print(f"An error occurred: {e}")


query = (
    "SELECT city_state, lat, lng FROM latlong WHERE lat IS NOT NULL AND LNG IS NOT NULL"
)
distance_data = pl.read_database(query=query, connection=engine)
distance_df = pl.DataFrame(distance_data)

distance_df = distance_df.with_columns(pl.col("city_state").str.replace("'", ""))

distance_df = distance_df.with_columns(pl.col("city_state").str.replace("'", ""))


class DistanceCalculator(SQLModel, table=True):
    distance_id: int = Field(primary_key=True)
    regionid_1: int
    city_state_1: str
    lat_1: float
    long_1: float
    regionid_2: int
    city_state_2: str
    lat_2: float
    long_2: float
    distance: float


SQLModel.metadata.create_all(engine)

length = randint(100, 999)

for row in distance_df.iter_rows():
    city_state_1 = row[0]
    lat_1 = row[1]
    long_1 = row[2]
    try:
        distance_query = f"SELECT city_state, lat, lng FROM latlong WHERE city_state != '{city_state_1}' AND lat IS NOT NULL AND lng IS NOT NULL"
        distance_df = pl.read_database(query=distance_query, connection=engine)
    except psycopg2.ProgrammingError as e:
        print(f"An error occurred: {e}")
    for rows in distance_df.iter_rows():
        city_state_2 = rows[0]
        lat_2 = rows[1]
        long_2 = rows[2]
        distance = geodesic((lat_1, long_1), (lat_2, long_2)).miles
        dbWrite = DistanceCalculator(
            distance_id=length,
            regionid_1=1,
            city_state_1=city_state_1,
            lat_1=lat_1,
            long_1=long_1,
            regionid_2=2,
            city_state_2=city_state_2,
            lat_2=lat_2,
            long_2=long_2,
            distance=distance,
        )
        session.add(dbWrite)
        session.commit()
        print(
            f"{city_state_1} to  {city_state_2}, distance={distance} added to database"
        )
        length += 3
