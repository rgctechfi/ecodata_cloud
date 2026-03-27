import requests
import pandas as pd

'''
IMF DataMapper API documentation
The IMF DataMapper API can be used to retrieve time series as used in the DataMapper. Endpoints are available to get lists of indicators, countries, regions and analytical groups used in these time series.

The current version of the API is v1.

Available base endpoints
 - https://www.imf.org/external/datamapper/api/v1/indicators list of available indicators
 - https://www.imf.org/external/datamapper/api/v1/countries list of countries;
 - https://www.imf.org/external/datamapper/api/v1/regions list of defined geographical regions;
 - https://www.imf.org/external/datamapper/api/v1/groups list of defined analytical groups.
Each of these will return a JSON object with the label and optionally additional information per item. The keys of the objects are the ID's to be used when retrieving time series.

Retrieving time series
Time series consist of an indicator ID and zero or more ID's for specific countries, regions and groups. These can be appended in any order to the base URL of the API.

It is possible to restrict the time series to a more specific period using the periods querystring parameter. This should be set to a comma separated list of requested years.

Examples
 - https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH retrieve all values for Real GDP Growth;
 - https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH/USA/CHN retrieve Real GDP Growth values for the United States and the People's Republic of China;
 - https://www.imf.org/external/datamapper/api/v1/NGDP_RPCH?periods=2019,2020 retrieve Real GDP Growth values for 2019 and 2020.
 '''


# Define API query URL (JSON)
url = "https://www.imf.org/external/datamapper/api/v1/indicators"
# Fetch data
response = requests.get(url)
response.raise_for_status()

# Parse JSON and load into pandas DataFrame
data = response.json()
indicators = data.get("indicators", {})
df = pd.DataFrame.from_dict(indicators, orient="index")
df.index.name = "indicator"
df = df.reset_index()

# Export to CSV for easier viewing
csv_path = "api_indicators.csv"
df.to_csv(csv_path, index=False)

# Display full dataframe without truncation
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 0)
print(df.to_string(index=False))
print(f"\nCSV exporte vers: {csv_path}")

# Example with General government gross debt, all countries, all years
