#This file contains code to fetch data from Tanker Koenig website
#The functions designed in this code are in accordance to the API requests possible as mentioned
#in the API documentation. The API documentation link is given below.
#https://creativecommons.tankerkoenig.de/?page=info

import requests

class DataFetcher:
    def __init__(self, api_key:str):
        self.api_key = api_key

    def radius_search(self, latitude:float, longitude:float, radius:float, type:str, sort='dist'):
        parameters = {
            'lat':f'{latitude}',
            'lng':f'{longitude}',
            'rad':f'{radius}',
            'type':f'{type}',
            'sort':f'{sort}',
            'apikey':f'{self.api_key}'
        }
        url = "https://creativecommons.tankerkoenig.de/json/list.php"

        return requests.get(url=url, params=parameters)

    #Can return prices of fuel of max 10 stations at a time.
    #Needs Station ID's as input. It should be a comma separated string
    #for example: "4429a7d9-fb2d-4c29-8cfe-2ca90323f9f8,446bdcf5-9f75-47fc-9cfa-2c3d6fda1c3b,60c0eefa-d2a8-4f5c-82cc-b5244ecae955,44444444-4444-4444-4444-444444444444"
    def price_query(self, station_ids:str):
        parameters = {
            'ids':f'{station_ids}',
            'apikey':f'{self.api_key}'
        }
        url = "https://creativecommons.tankerkoenig.de/json/prices.php"

        return requests.get(url, params=parameters)
    
    #Detailed info about single station using Station ID
    def detailed_query(self, station_id):
        parameters = {
            'id':f'{station_id}',
            'apikey':f'{self.api_key}'
        }
        url = "https://creativecommons.tankerkoenig.de/json/detail.php"
        
        return requests.get(url=url, params=parameters)