# Generated Real Estate Data

## This currently processes the data from CSV files, writes them to a database and then makes calculations and generates the visualizations

This project was built using real estate data from RedFin and Latitude and Longitude data from [SimpleMaps](https://simplemaps.com/data/us-cities)


In order to run this:
#### You will need a Postgresql database
#### Create a .env File

- Clone this repo
- Jump into the directory
- Run uv sync
- Run uv run data_load.py
- Run uv run streamlit run main.py  <-- This will launch a local instance of streamlit


`git clone https://github.com/sdudenhofer/real_estate.git` <br>
`cd real_estate` <br>
`uv sync`<br>
`uv run streamlit run main.py` <br>

## Work currently in progress

Writing to Supabase database rather than a local database