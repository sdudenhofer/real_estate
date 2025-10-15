import duckdb as db
import polars as pl
from geopy.distance import geodesic
from geopy.distance import great_circle

# Process base data
file_location = 'realestate_data.db'
conn = db.connect(file_location)
latlong_file = "data/uscities.csv"
file = "data/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
df = pl.read_csv(file).sort("State")
conn.execute("CREATE TABLE IF NOT EXISTS redfin_data AS SELECT * FROM df")
latlong_df = pl.read_csv(latlong_file)
conn.execute("CREATE TABLE IF NOT EXISTS latlong_data AS SELECT * FROM latlong_df")

# Gather lat/long data for Geo information
latlongData = conn.execute("SELECT ll.lat, ll.lng, rd.RegionName, rd.State, rd.RegionID\
    FROM redfin_data rd\
    RIGHT JOIN latlong_data ll on rd.RegionName = ll.city_ascii and rd.State = ll.state_id").pl()

# Making sure data is only unique
unique_latlongData = latlongData.unique()
conn.execute("CREATE TABLE IF NOT EXISTS unique_latlong AS SELECT * FROM unique_latlongData")

# print(conn.execute("SELECT * FROM unique_latlong").pl())

distance_data_1 = conn.execute("SELECT lat, lng, RegionID FROM unique_latlongData").pl()
distance_data_2 = conn.execute("SELECT lat, lng, RegionID FROM unique_latlongData").pl()

conn.execute("CREATE TABLE IF NOT EXISTS mileage_data  ( \
                    id INTEGER PRIMARY KEY, \
                    region_id_start INTEGER,\
                    region_id_destination INTEGER, \
                    distance_miles REAL)")
conn.commit()

count = 0
for row in distance_data_1.iter_rows(named=True):
    latitude = row['lat']
    longitude = row['lng']
    region_id = row['RegionID']
    coords_1 = f"{latitude}, {longitude}"
    for rows in distance_data_2.iter_rows(named=True):
        latitude_2 = rows['lat']
        longitude_2 = rows['lng']
        region_id_2 = rows['RegionID']
        coords_2 = f"{latitude_2}, {longitude_2}"
        circle_distance = great_circle(coords_1, coords_2).miles
        if region_id is None:
            region_id = 0
        if region_id_2 is None:
            region_id_2 = 0
        if circle_distance <= 1200:
            conn.execute(f"INSERT INTO mileage_data (id, region_id_start, region_id_destination, distance_miles) VALUES ({count}, {region_id}, {region_id_2}, {circle_distance})")
        else:
            print(f"Distance is to great for Beginning City: {region_id}, Destination City: {region_id_2} -> {circle_distance}")
        count += 1
