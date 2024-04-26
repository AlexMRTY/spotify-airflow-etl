import json
import requests
import os
import pandas as pd
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook


CLIENT_ID = Variable.get('CLIENT_ID')
CLIENT_SECRET = Variable.get('CLIENT_SECRET')
REFRESH_TOKEN = Variable.get('REFRESH_TOKEN')

AUTH_URL = Variable.get('AUTH_URL')
TOKEN_URL = Variable.get('TOKEN_URL')


def refresh_token():
    url = TOKEN_URL
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': REFRESH_TOKEN,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(url, data=payload, headers=headers)
    access_token = response.json()["access_token"]
    print(access_token)
    return access_token

def fetch_data_from_spotify(access_token: str = ''):
    url = 'https://api.spotify.com/v1/me/top/tracks'
    headers = {
        'Authorization': 'Bearer {}'.format(access_token)
    }
    response = requests.get(
        'https://api.spotify.com/v1/me/top/{type}?limit={limit}&time_range={time_range}'.format(
            type='tracks',
            limit='10',
            time_range='short_term'
        ), 
        headers=headers
        )
    # with open('records.json', 'r', encoding='utf-8') as json_file:
    #     response = json.load(json_file)
    return response.json()

def parse_data_into_df(data: dict):
    # Extracting data from json and converting to row/column format.
    track_id = []
    track_name = []
    album_name = []
    artist_name = []
    track_popularity = []
    track_uri = []

    for track in data['items']:
        track_id.append(track['id'])
        track_name.append(track['name'])
        album_name.append(track['album']['name'])
        artist_name.append(track['artists'][0]['name'])
        track_popularity.append(track['popularity'])
        track_uri.append(track['uri'])

    track_dict = {
        "track_id": track_id,
        "track_name": track_name, 
        "album_name": album_name, 
        "artist_name": artist_name, 
        "track_popularity": track_popularity, 
        "track_uri": track_uri
    }

    track_df = pd.DataFrame(
        track_dict,
        columns=[
            "track_id",
            "track_name",
            "album_name",
            "artist_name",
            "track_popularity",
            "track_uri"
        ]
    )

    return track_df

def generate_query(df: pd.DataFrame):
    query = """INSERT INTO track (track_id, track_name, album_name, artist_name, track_popularity, track_uri) 
            VALUES 
            """
    for index, row in df.iterrows():
        query += f"""('{row['track_id']}', '{row['track_name']}', '{row['album_name']}', '{row['artist_name']}', {row['track_popularity']}, '{row['track_uri']}'),"""
    query = query[:-1]
    query += "ON CONFLICT (track_id) DO NOTHING;"
    return query

def push_data_to_db(df: pd.DataFrame):

    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')
    postgres_conn = postgres_hook.get_conn()
    cursor = postgres_conn.cursor()

    cursor.execute(generate_query(df))

    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()
    
def extract_spotify_data():
    access_token = refresh_token()
    data = fetch_data_from_spotify(access_token)
    df = parse_data_into_df(data)
    push_data_to_db(df)


    
# data_frame = parse_data_into_dataframe(data)
# save_data_to_csv(data)


# # Retrive Code
# auth_code = requests.get(AUTH_URL, {
#     'client_id': CLIENT_ID,
#     'response_type': 'code',
#     'redirect_uri': 'http://localhost:3000',
#     'scope': 'user-top-read',
# })
# print(auth_code)


# response = requests.post(
#     "https://accounts.spotify.com/api/token",
#     data={
#         "grant_type": "authorization_code",
#         "code": CODE,
#         "redirect_uri": 'http://localhost:3000',
#     },
#     auth=(CLIENT_ID, CLIENT_SECRET),
# )
# access_token = response.json()["access_token"]
# print(access_token)