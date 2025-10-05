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

connection.execute("""
                CREATE TABLE IF NOT EXISTS distance_data (
                    RegionID1 INTEGER,
                    city_state1 TEXT,
                    RegionID2 INTEGER,
                    city_state2 TEXT,
                    distance_miles REAL
                )
            """)

for row in city_state.iter_rows(named=True):
    city_state_str = f"{row['RegionName']}, {row['State']}"
    city_state_str = city_state_str.replace("'", "''")  # Escape single quotes for SQL query
    RegionID = row['RegionID']
    try:
        latlong_data = connection.execute(f"SELECT id as RegionID, city_state, latitude, longitude FROM city_geocode WHERE city_state = '{city_state_str}'").pl()
        empty_df.extend(latlong_data)
    except Exception as e:
        logging.error(f"Error retrieving lat/long for {city_state_str}: {e}")
        empty_df.extend([RegionID, city_state_str, 'NULL', 'NULL'])
        continue
q = empty_df.select(["RegionID", "city_state", "latitude", "longitude"]).unique()

coords = []

for values in empty_df.iter_rows(named=True):
    for rows in q.iter_rows(named=True):
        first_id = values['RegionID']
        second_id = rows['RegionID']
        coord1 = (values['latitude'], values['longitude'])
        coord2 = (rows['latitude'], rows['longitude'])
        dupe_coord = (coord1, coord2)
        dupe_coord_rev = (coord2, coord1)
        if (first_id != second_id) and dupe_coord not in coords and dupe_coord_rev not in coords:
            try:
                distance = geodesic(coord1, coord2).miles
                connection.execute(f"INSERT INTO distance_data (RegionID1, city_state1, RegionID2, city_state2, distance_miles) VALUES ({values['RegionID']}, '{values['city_state']}', {rows['RegionID']}, '{rows['city_state']}', {distance})")
                connection.commit()
                print(f"Inserted distance between {first_id} and {second_id}: {distance} miles")
                coords.append(dupe_coord)
            except Exception as e:
                 logging.error(f"Error calculating/inserting distance between {coord1} and {coord2}: {e}")
                 next
        else:
            logging.info(f"Skipping distance calculation for same RegionID: {first_id}, {second_id}")