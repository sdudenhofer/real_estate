import calendar

import duckdb
import polars as pl
import polars.selectors as cs

housing_data = pl.read_csv("data/City_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv")
housing_df = pl.DataFrame(housing_data)

latlong_data = pl.read_csv("data/uscities.csv")
latlong_df = pl.DataFrame(latlong_data)

df = housing_df.with_columns(
    pl.concat_str(["RegionName", "StateName"], separator=", ").alias("city_state")
)

latlng_df = latlong_df.with_columns(
    pl.concat_str(pl.col("city_ascii"), pl.col("state_id"), separator=", ").alias(
        "city_state"
    )
)

dataframe = df.join(latlng_df, on="city_state", how="left")


conn = duckdb.connect("database/realestate_data.db")
conn.sql("create table realestatedata as select * from dataframe")
year = ["2020", "2021", "2022", "2023", "2024", "2025"]
month_names = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]

# accumulate per-year frames then concat at the end
frames = []
for yr in year:
    # find all columns for this year (e.g. "2020-01-31", "2020-02-29", ...)
    date_cols = [c for c in dataframe.columns if c.startswith(f"{yr}-")]
    if not date_cols:
        continue

    # keep RegionID plus the date columns for this year
    cols = ["RegionID"] + date_cols
    dframe = dataframe.select(cols)

    # build a mapping from actual date column name -> month name using calendar for last-day correctness
    mapping = {}
    y = int(yr)
    for i, mname in enumerate(month_names, start=1):
        last_day = calendar.monthrange(y, i)[1]
        date_col = f"{yr}-{i:02d}-{last_day}"
        if date_col in dframe.columns:
            mapping[date_col] = mname

    # rename returns a new DataFrame — assign it
    if mapping:
        dframe = dframe.rename(mapping)

    # add Year column so each RegionID will have a separate row per year
    dframe = dframe.with_columns(pl.lit(int(yr)).alias("Year"))

    # ensure a consistent column order and fill missing months with nulls
    out_cols = ["RegionID", "Year"] + month_names
    exprs = [
        pl.col(c) if c in dframe.columns else pl.lit(None).alias(c) for c in out_cols
    ]
    dframe = dframe.select(exprs)

    frames.append(dframe)

# concat all year frames vertically; if none found, create empty frame with expected schema
if frames:
    year_dataframe = pl.concat(frames, how="vertical", rechunk=True)
else:
    year_dataframe = pl.DataFrame(
        schema={"RegionID": int, **{m: float for m in month_names}}
    )

conn = duckdb.connect("database/realestate_data.db")
conn.sql("create table realestate_by_year as select * from year_dataframe")
conn.close()
