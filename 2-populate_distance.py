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
                    RegionID2 INTEGER,
                    distance_miles REAL
                )
            """)

for row in city_state.iter_rows(named=True):
    city_state_str = f"{row['RegionName']}, {row['State']}"
    city_state_str = city_state_str.replace("'", "''")  # Escape single quotes for SQL query
    RegionID = row['RegionID']
    try:
        latlong_data = connection.execute(f"SELECT {RegionID} as RegionID, city_state, latitude, longitude FROM city_geocode WHERE city_state = '{city_state_str}'").pl()
        # latlong_data = latlong_data.with_columns(pl.lit(RegionID).alias("RegionID"))
        empty_df.extend(latlong_data)
    except Exception as e:
        error_data = pl.DataFrame([RegionID, city_state, 'NULL', 'NULL'], schema=schema)
        # print(f"Error fetching latlong for {row['city_state']}: {e}")
        continue

distance_info = empty_df['RegionID', 'latitude', 'longitude'].get_columns()

for rows in distance_info:
    region_id = rows[0]
    lat = float(rows[1])
    long = float(rows[2])
    if lat is None or long is None:
        logging.warning(f"Skipping RegionID {region_id} due to missing coordinates.")
    for values in empty_df.iter_rows(named=True):
        if type(values['latitude']) is not float or type(values['longitude']) is not float:
            values['latitude'] = 0.0
            values['longitude'] = 0.0
        if region_id != values['RegionID']:
            coord1 = (lat, long)
            coord2 = (values['latitude'], values['longitude'])
            try:
                distance = geodesic(coord1, coord2).miles
                connection.execute(f"INSERT INTO distance_data (RegionID1, RegionID2, distance_miles) VALUES ({region_id}, {values['RegionID']}, {distance})")
                connection.commit()
                print(f"Inserted distance between {RegionID} and {values['RegionID']}: {distance} miles")
            except Exception as e:
                 logging.error(f"Error calculating/inserting distance between {coord1} and {coord2}: {e}")
                 logging.warning(f"Coordinates: {coord1}, {coord2}")
    print(f"Completed distances for RegionID {region_id}")
