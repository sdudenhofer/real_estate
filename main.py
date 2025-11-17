import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import os
from sqlmodel import SQLModel, Field, create_engine, Session
from dotenv import load_dotenv
import altair as alt

load_dotenv()
pg_user = os.getenv("POSTGRES_USER")
pg_password = os.getenv("POSTGRES_PASSWORD")
pg_db = os.getenv("POSTGRES_DB")

engine = create_engine(f"postgresql://{pg_user}:{pg_password}@localhost/{pg_db}")

state_query = "select distinct rd.state_name from realestatedata rd "
state_data = pd.read_sql(state_query, engine)
state_df = pd.DataFrame(state_data)


st.header("Real Estate Data")

# show list of state names (not the full dataframe) in the selectbox
states = state_df["state_name"].tolist()
state = st.selectbox("Select a State to get started.", states)
st.header(f"Data for {state}")

select_data_query = f'''
SELECT * FROM realestatedata WHERE state_name =  %s ORDER BY "city_state"
'''
sd_data = pd.read_sql(select_data_query, engine, params=(state,))
sd_df = pd.DataFrame(sd_data)

# st.dataframe(sd_df)

# let the user pick a city (filtered to the selected state) and then the RegionID for that city
city_options = sd_df["city_state"].dropna().unique().tolist()
if not city_options:
    st.write("No cities found for selected state.")
else:
    city_options.sort(reverse=True)
    city = st.selectbox("Select a City to view", city_options)

    # find RegionID(s) that match the selected city within the already-state-filtered sd_df
    region_ids = sd_df.loc[sd_df["city_state"] == city, "RegionID"].dropna().unique().tolist()
    if not region_ids:
        st.write("No RegionID found for selected city.")
        region_id = None
    elif len(region_ids) == 1:
        region_id = int(region_ids[0])
        # st.hidden(f"Using RegionID: {region_id}")
    else:
        # if multiple RegionIDs exist for the same city_state, let the user choose
        region_id = int(st.selectbox("Multiple RegionIDs found — choose one", region_ids))

    year = st.selectbox("Select a Year to View:", ["2020", "2021", "2022", "2023", "2024", "2025"])

    st.header("Year Data by Month")
    if year and region_id is not None:
        # use parameterized query and proper identifier quoting (double quotes) for PostgreSQL
        year_query = '''
        SELECT "RegionID", "January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"
        FROM realestate_by_year
        WHERE "Year" = %s AND "RegionID" = %s
        '''
        params = (int(year), int(region_id))
        yd_data = pd.read_sql(year_query, engine, params=params)

        if yd_data.empty:
            st.write("No data for selected RegionID/year.")
        else:
            months = ["January","February","March","April","May","June","July","August","September","October","November","December"]
            row = yd_data.iloc[0][months].astype(float)

            # create a DataFrame indexed by month so the line chart x-axis is months
            plot_df = pd.DataFrame({"Value": row.values}, index=months)
            # make the index a categorical with the desired month order and sort by that order
            plot_df.index = pd.CategoricalIndex(plot_df.index, categories=months, ordered=True)
            plot_df = plot_df.sort_index()
            st.line_chart(plot_df)
    else:
        st.write("Please select a region and year to view the data.")

st.header("Average Home Value by Month")
avg_data_query = f'''
SELECT "city_state", "Year", rby."RegionID", AVG(COALESCE("January", 0)) AS avg_january, 
  AVG(COALESCE("February", 0)) AS avg_february,
  AVG(COALESCE("March", 0)) as avg_march,
  AVG(COALESCE("April", 0)) as avg_april,
  AVG(COALESCE("May", 0)) as avg_may,
  AVG(COALESCE("June", 0)) as avg_june,
  AVG(COALESCE("July", 0)) as avg_july,
  AVG(COALESCE("August", 0)) as avg_august,
  AVG(COALESCE("September", 0)) as avg_september,
  AVG(COALESCE("October", 0)) as avg_october,
  AVG(COALESCE("November", 0)) as avg_november,
  AVG(COALESCE("December" , 0)) as avg_december
  FROM realestate_by_year rby
  LEFT OUTER JOIN realestatedata rd ON rby."RegionID" = rd."RegionID"
  WHERE rby."RegionID" = '{region_id}'
  GROUP BY rby."RegionID", "Year", "city_state"
  '''
avg_data = pd.read_sql(avg_data_query, engine)
avg_month = pd.DataFrame(avg_data)

# reshape months into long form so x-axis = Month and color = Year
month_cols = [
    "avg_january",
    "avg_february",
    "avg_march",
    "avg_april",
    "avg_may",
    "avg_june",
    "avg_july",
    "avg_august",
    "avg_september",
    "avg_october",
    "avg_november",
    "avg_december",
]
month_map = {
    "avg_january": "January",
    "avg_february": "February",
    "avg_march": "March",
    "avg_april": "April",
    "avg_may": "May",
    "avg_june": "June",
    "avg_july": "July",
    "avg_august": "August",
    "avg_september": "September",
    "avg_october": "October",
    "avg_november": "November",
    "avg_december": "December",
}

melted = avg_month.melt(
    id_vars=["city_state", "Year", "RegionID"], value_vars=month_cols, var_name="Month", value_name="Value"
)
melted["Month"] = melted["Month"].map(month_map)
# ensure month ordering on x-axis
month_order = [
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
melted["Month"] = pd.Categorical(melted["Month"], categories=month_order, ordered=True)

chart = (
    alt.Chart(melted)
    .mark_line(point=True)
    .encode(
        x=alt.X("Month:N", sort=month_order, title="Month"),
        y=alt.Y("Value:Q", title="Average Home Value"),
        color=alt.Color("Year:N", title="Year"),
        tooltip=["city_state", "RegionID", "Year", "Month", alt.Tooltip("Value:Q", format=",.2f")],
    )
    .properties(height=400)
)

table_data = melted.loc[melted["Year"] == int(year), ["Month", "Value"]].copy()

with st.expander("Show Average Home Value Data Table"):
    st.table(table_data, border="horizontal")
st.altair_chart(chart, use_container_width=True)
