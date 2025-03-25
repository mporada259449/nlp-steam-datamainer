import requests
import os
from dotenv import load_dotenv
import json
import time
import multiprocessing

load_dotenv('.env')
STEAM_API_KEY = os.environ.get('STEAM_API_KEY')
STEAM_URL=os.environ.get('STEAM_URL')

def get_page_of_reviewes_for_game(gameid, params):
    response = requests.get(f'{STEAM_URL}/{gameid}', params=params)
    data = response.json()
    reviews = data.get('reviews', [])
    cursor = data.get('cursor')

    return reviews, cursor

def save_reviews(file, new_reviews):
    with open(file, "r") as f:
        existing_data = json.load(f)

    existing_data.extend(new_reviews)

    with open(file, "w") as f:
        json.dump(existing_data, f, indent=4)

def get_all_reviews_for_game(file_name, gameid, params):
    while True:
        reviews, cursor = get_page_of_reviewes_for_game(gameid, params)
        if not reviews:
            break

        save_reviews(file_name, reviews)

        if not cursor:
            break
        
        params['cursor'] = cursor
        time.sleep(1)

def worker(gameid, params):
    file_name = f'reviews_{gameid}.json'
    if not os.path.exists(file_name):
        with open(file_name, "w") as f:
            json.dump([], f)
    
    get_all_reviews_for_game(file_name, gameid, params)

def single_thread_run(gameids, params):
    for gameid in gameids:
        worker(gameid, params)

def parallel_run(gameids, params):
    num_workers = min(len(gameids), multiprocessing.cpu_count())

    with multiprocessing.Pool(processes=num_workers) as pool:
        pool.starmap(worker, [(gameid, params) for gameid in gameids])


if __name__=='__main__':
    with open('games.json', 'r') as f:
        gameids = json.load(f)
    
    params = {
        'json': 1,
        'num_per_page': 20,
        'filter': 'recent',
        'cursor': '*'
    }

    parallel_run(gameids, params)
    

