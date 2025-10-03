import polars as pl
import duckdb
from geopy.distance import geodesic
import loguru

connection = duckdb.connect('real_estate.db')
logging = loguru.logger
logging.add("logs/populate_distance.log", rotation="10 MB")
region_id = connection.execute("SELECT RegionID, State, RegionName FROM redfin_data").pl()

city_state = region_id.with_columns(
    pl.concat_str([
        pl.col("RegionName"),pl.lit(", "),pl.col("State")
        ],
    ).alias("city_state"))

schema = {
    "RegionID": pl.Int32,
    "city_state": pl.String,
    "latitude": pl.Float32,
    "longitude": pl.Float32
}

empty_df = pl.DataFrame([], schema=schema)
error_df = pl.DataFrame([], schema=schema)
connection.execute("""
                CREATE TABLE IF NOT EXISTS distance_data (
                    RegionID1 INTEGER,
                    RegionID2 INTEGER,
                    distance_miles REAL
                )
            """)

for row in city_state.iter_rows(named=True):
    city_state_str = f"{row['RegionName']}, {row['State']}"
    city_state_str = city_state_str.replace("'", "''")  # Escape single quotes for SQL query
    RegionID = row['RegionID']
    try:
        latlong_data = connection.execute(f"SELECT RegionID, city_state, latitude, longitude FROM city_geocode WHERE city_state = '{city_state_str}'").pl()
        empty_df.extend(latlong_data)
    except Exception as e:
        logging.error(f"Error retrieving lat/long for {city_state_str}: {e}")
        error_df.extend([RegionID, city_state_str, 'NULL', 'NULL'])
        continue

distance_info = empty_df['RegionID', 'latitude', 'longitude'].get_columns()
distance_info2 = empty_df['RegionID', 'latitude', 'longitude'].get_columns()

for RegionID, latitude, longitude in distance_info:
    for RegionID_1, latitude_1, longitude_1 in distance_info2:
        if (RegionID != RegionID_1):
            coord1 = (latitude, longitude)
            coord2 = (latitude_1, longitude_1)
            try:
                distance = geodesic(coord1, coord2).miles
                connection.execute(f"INSERT INTO distance_data (RegionID1, RegionID2, distance_miles) VALUES ({RegionID}, {RegionID_1}, {distance})")
                connection.commit()
                logging.success(f"Inserted distance between {RegionID} and {RegionID_1}: {distance} miles")
            except Exception as e:
                 logging.error(f"Error calculating/inserting distance between {coord1} and {coord2}: {e}")
        else:
            logging.info(f"Skipping distance calculation for same RegionID: {RegionID}")