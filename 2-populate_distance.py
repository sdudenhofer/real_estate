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
    "longitude": pl.Float32,
    "state": pl.String
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

state_df = region_id.select(["State"]).unique()

for row in city_state.iter_rows(named=True):
    city_state_str = f"{row['RegionName']}, {row['State']}"
    city_state_str = city_state_str.replace("'", "''")  # Escape single quotes for SQL query
    RegionID = row['RegionID']
    state = row['State']
    try:
        latlong_data = connection.execute(f"SELECT id as RegionID, city_state, latitude, longitude, '{state}' as state FROM city_geocode WHERE city_state = '{city_state_str}'").pl()
        empty_df.extend(latlong_data)
    except Exception as e:
        logging.error(f"Error retrieving lat/long for {city_state_str}: {e}")
        empty_df.extend([RegionID, city_state_str, 'NULL', 'NULL'])
        continue
q = empty_df.select(["RegionID", "city_state", "latitude", "longitude", "state"])

coords = []

# Process each state(s) distance calculations
for state in state_df.iter_rows(named=True):
    state_name = state['State']
    df_by_state = q.filter(pl.col('state') == state_name)
    for value in df_by_state.iter_rows(named=True):
        initial_id = value['RegionID']
        initial_coord = (value['latitude'], value['longitude'])
        for line in df_by_state.iter_rows(named=True):
            compare_id = line['RegionID']
            compare_coord = (line['latitude'], line['longitude'])
            dupe_coord = (initial_coord, compare_coord)
            dupe_coord_rev = (compare_coord, initial_coord)
            if (initial_id != compare_id) and dupe_coord not in coords and dupe_coord_rev not in coords:
                try:
                    distance = geodesic(initial_coord, compare_coord).miles
                    connection.execute(f"INSERT INTO distance_data (RegionID1, city_state1, RegionID2, city_state2, distance_miles) VALUES ({value['RegionID']}, '{value['city_state']}', {line['RegionID']}, '{line['city_state']}', {distance})")
                    connection.commit()
                    # print(f"Inserted distance between {initial_id} and {compare_id}: {distance} miles")
                    coords.append(dupe_coord)
                except Exception as e:
                     logging.error(f"Error calculating/inserting or duplicate record(s) {initial_coord} and {compare_coord}: {e}")
            else:
                logging.info(f"Skipping distance calculation for RegionIDs: {initial_id}, {compare_id}")
        print(f"State {state_name} processed...")