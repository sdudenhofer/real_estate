# Real estate information

### Currently this project processes a csv file from Redfin that contains monthly real estate sales data. Then it uses Nominatam to generate latitude and longitude coordinates. We then are generating the distance from each point to each other point. The reasoning for this project is I would like to see sales data compared to other cities that are within x miles. 

To get started:

- Clone this repo
- Use uv to grab the needed modules
```
uv sync
```
- To run the app 
```
uv run fastapi dev
```

### Next Steps

[] - Generate radius' for each location to see real estate data with in X miles
[] - Generate Map and then build out pricing data scales
[] - Currently saving to database in docker file