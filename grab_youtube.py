import json
from datetime import datetime
from dateutil import parser

import pandas as pd

SECRET = '{SECRET}'

from youtube import API
import requests


def fetch(api, videoId):
    video = api.get('videos', id=videoId)
    comments = []
    responseComments = requests.get(
        "https://www.googleapis.com/youtube/v3/commentThreads?key={SECRET_KEY}"
        "&textFormat=plainText&part=snippet&videoId=" + videoId + "&maxResults=100").json()
    for responseObject in responseComments['items']:
        comment = responseObject['snippet']['topLevelComment']['snippet']
        comments.append({
            'author': comment['authorDisplayName'],
            'content': comment['textDisplay'],
            'score': str(comment['likeCount']),
            "time": comment['publishedAt']
        })

    parsed_date = parser.parse(video['items'][0]['snippet']['publishedAt'])
    year = parsed_date.year
    month = parsed_date.month
    day = parsed_date.day

    time = str(month) + '-' + str(day) + '-' + str(year)

    data = {
        "posted_time": video['items'][0]['snippet']['publishedAt'],
        "gathered_timestamp": str(datetime.now()),
        "link": "https://www.youtube.com/watch?v=" + videoId,
        "title": video['items'][0]['snippet']['title'],
        "comments": comments
    }

    with open('data/youtube/json/data-' + str(time) + '.json', 'w+') as f:
        json.dump(data, f)

    print("Registering...")

    exported = pd.read_json('data/youtube/json/data-' + str(time) + '.json')
    exported = exported.comments.apply(pd.Series)
    exported.to_csv('data/youtube/csv/data-' + str(time) + '.csv')

    print("Success. Exported to " + 'data/youtube/json/data-' + str(time) + '.json' + ' and ' + 'data/youtube/csv/data-' + str(time) + '.csv')


api = API(client_id='corona_mine', client_secret='', api_key=SECRET)

lists = [
    "FNm86mtWDes",
    "eWFgJrLTc7k"
]

for videoId in lists:
    fetch(api, videoId)
