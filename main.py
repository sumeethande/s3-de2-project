import streamlit as st
from ingestor.data_fetch import DataFetcher
from dotenv import dotenv_values
import json
import requests
from geopy.geocoders import Nominatim

env_config = dotenv_values(".env")
TANKER_API_KEY = env_config.get("TANKER_API_KEY")

#Creating a sidebar with a user input form
with st.sidebar:
    with st.form("visualization-form", clear_on_submit=True):
        city_name = st.text_input(label="Location:", placeholder="City Name")
        # latitude = st.text_input(label="Enter Latitude:", placeholder="49.480076577107425")
        # longitude = st.text_input(label="Enter Longitude:", placeholder="8.470187850948623")
        search_radius = st.number_input(label="Enter a Radius(Km):", min_value=1.0, max_value=25.0, value=5.0, help="min 1km and max 25km", format="%f")
        fuel_type = st.selectbox(label="Select Fuel type", options=["e5", "e10", "diesel", "all"])
        submitted = st.form_submit_button(label="Search", type="primary")

st.header("Fueler!⛽", divider="grey")
st.caption("Search for fuel prices across gas stations all over Germany with just a click!")

if submitted:
   #Code after clicking the form submit button
    url = "https://europe-west3-de-class-activity.cloudfunctions.net/data-fetch-test"

    #Getting latitude and longitude from city name
    geopy_obj = Nominatim(user_agent="main")
    location = geopy_obj.geocode(f"{city_name}, Germany")

    parameters = {
            'lat':f'{location.latitude}',
            'lng':f'{location.longitude}',
            'rad':f'{search_radius}',
            'type':f'{fuel_type}',
            'sort':"dist",
        }
    
    with st.spinner("Getting data..."):
        response = requests.get(url=url, params=parameters)
    st.write(response.text)