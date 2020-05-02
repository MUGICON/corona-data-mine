import sys
import json
import time
import logging
import urllib.parse
from datetime import datetime
from dateutil import parser
import nltk
import pandas as pd
import os
from os import environ as e
import requests
import re
import tweepy
from requests_oauthlib import OAuth1
from tweepy.auth import OAuthHandler
from tweetscrape.conversation_tweets import TweetScrapperConversation
import csv
from collections import namedtuple

total = []


def fetch(tweetId):
    auth = tweepy.OAuthHandler("{USER_ID}", "{USER_PASS}")
    auth.set_access_token("{ACCESS_TOKEN}",
                          "{ACCESS_SECRET}")
    api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

    print("Starting to fetch tweet information")
    comments = []
    response = api.get_status(tweetId)
    print("Done, now fetching comments")
    api = tweepy.API(auth)

    TweetScrapperConversation(
        username=response['user']['screen_name'],
        parent_tweet_id=int(response['id_str']),
        num_tweets=100,
        tweet_dump_path='twitter_conv.csv',
        tweet_dump_format='csv'
    ).get_thread_tweets(False)

    index = 1

    commentsResponse0 = []
    commentsResponse = []

    with open('twitter_conv.csv', 'r', encoding="utf-8") as responseCsvFile:
        reader = csv.DictReader(responseCsvFile)
        commentsResponse0 = list(reader)

    os.remove('twitter_conv.csv')

    seen = set()
    for d in commentsResponse0:
        t = tuple(d.items())
        if t not in seen:
            seen.add(t)
            commentsResponse.append(d)

    for comment in commentsResponse:
        content = re.sub(r"http\S+", "", comment['text'])

        if 'pic.twitter.com' in content:
            continue

        content = ' '.join(
            re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", content).split())

        content = ' '.join(w for w in nltk.wordpunct_tokenize(content) if w.lower() in content or not w.isalpha())

        if not content.strip():
            continue

        print("Fetching comments " + str(index))

        time = datetime.utcfromtimestamp(int(comment['time']) / 1000)

        comments.append({
            'author': comment['author'],
            'content': content,
            'time': time,
            'score': int(comment['favorite_count'])
        })

        if index >= 100 or index >= len(commentsResponse):
            break
        index += 1

    date = parser.parse(response['created_at'])

    year = date.year
    month = date.month
    day = date.day

    time = str(month) + '-' + str(day) + '-' + str(year)

    data = {
        "posted_time": str(date),
        "gathered_timestamp": str(datetime.now()),
        "link": "https://twitter.com/" + response['user']['screen_name'] + "/status/" + tweetId,
        "comments": comments
    }

    with open('data/twitter/json/data-' + str(time) + '.json', 'w+') as f:
        json.dump(data, f, default=str)

    print("Registering...")

    total.append(data)

    exported = pd.read_json('data/twitter/json/data-' + str(time) + '.json')
    exported = exported.comments.apply(pd.Series)
    exported.to_csv('data/twitter/csv/data-' + str(time) + '.csv')

    print("Success. Exported to " + 'data/twitter/json/data-' + str(
        time) + '.json' + ' and ' + 'data/twitter/csv/data-' + str(time) + '.csv')

tweets = [
    '1246917153082822661',
    '1245537795927572480',
    '1245388223444070400',
    '1243662566104215557',
    '1242882601993621506',
    '1242139404292050951',
    '1240767501795016704',
    '1240005620482412547',
    '1239611619883266048',
    '1239244465715494914',
    '1238581439471521792',
    '1238259897743138816',
    '1237780394478571522',
    '1237122102991486979',
    '1234967676138725377',
    '1233896047497355264',
    '1233162240334888962',
    '1232322887488593922',
    '1229526621834694658',
    '1227773492495626240'
]


from os import walk

f = []
for (dirpath, dirnames, filenames) in walk('data/twitter/json/'):
    f.extend(filenames)

seenContent = []

for filename in f:
    with open('data/twitter/json/' + filename) as file:
        if filename != 'data-merged.json':
            print(filename)
            content = json.load(file)
            comments = []
            for subContent in content['comments']:
                if subContent not in seenContent:
                    seenContent.append(subContent)
                    comments.append(subContent)

            if len(comments) <= 0: continue

            content['comments'] = comments
            total.append(content)

with open('data/twitter/json/data-merged.json', 'w+') as f:
    json.dump(total, f)
    
