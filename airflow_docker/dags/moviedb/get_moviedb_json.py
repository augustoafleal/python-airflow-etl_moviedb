import requests
import json
import os
from datetime import datetime

key_path = '/opt/airflow/config/'
output_path = '/opt/airflow/dags/moviedb/data/process/'

def getKeys():
    print('> Getting API keys.')
    with open(f'{key_path}config.json', 'r') as json_file:
        data = json.load(json_file)

    return data['tmdb']['key']

def getJsonMovies(key):
    print('> Getting movies data.')

    url = "https://api.themoviedb.org/3/movie/top_rated?language=en-US&page="
        
    headers = {
        "accept": "application/json",
        "Authorization": key
    }

    page_limit = 5

    movies_json = []

    for page in range (1, page_limit + 1):
        response = requests.get(f'{url}{page}', headers=headers)
        
        if response:
            json_response = response.json()
            
            if 'results' in json_response:
                movies_json.extend(json_response['results'])

        if json_response['total_pages'] < page + 1:
            break

    return movies_json

def create_file(movies_json):
    print('> Creating json file.')

    json_output = f'{output_path}moviedb_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

    with open(json_output, "w") as json_file:
        json.dump(movies_json, json_file, indent=4)

    print(f'> File {os.path.basename(json_output)} succesfully created.')

def createJson():
    print('> Starting process.')
    key = getKeys()
    movies_json = getJsonMovies(key)
    if len(movies_json) > 0: 
        create_file(movies_json)
        return True
    return False