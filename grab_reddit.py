from datetime import datetime
import praw
import pandas as pd
import json
import re

total = []

def fetch(redditInstance, id):
    submission = redditInstance.submission(id=id)
    submission.comment_sort = 'top'

    submission.comments.replace_more(limit=100)

    print("Finished fetching submission " + id + "... Fetching comments...")

    index = 0

    totalBody = ""

    comments = []

    commentsCache = submission.comments.list()
    for comment in commentsCache:
        print("Processing comment " + str(index))

        if comment.author is not None and comment.author.name != 'AutoModerator':
            content = ' '.join(
                re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", comment.body.replace("\u2019", "'").replace("\n", " ").replace("\"", "'")).split())

            if not content:
                continue

            comments.append({
                'author': comment.author.name,
                'content': content,
                'score': str(comment.score),
                "time": str(datetime.utcfromtimestamp(comment.created_utc))
            })

            totalBody += content

            print("Finished processing and appended to array")
            if index == 100 or index >= len(commentsCache):
                break
            index += 1

    parsed_date = datetime.utcfromtimestamp(submission.created_utc)
    year = parsed_date.year
    month = parsed_date.month
    day = parsed_date.day

    time = str(month) + '-' + str(day) + '-' + str(year)

    data = {
        "posted_time": str(parsed_date),
        "gathered_timestamp": str(datetime.now()),
        "link": submission.permalink,
        "title": submission.title,
        "comments": comments
    }

    with open('data/reddit/json/data-' + str(time) + '.json', 'w+') as f:
        json.dump(data, f)

    print("Registering...")

    total.append(data)
    exported = pd.read_json('data/reddit/json/data-' + str(time) + '.json')
    exported = exported.comments.apply(pd.Series)
    exported.to_csv('data/reddit/csv/data-' + str(time) + '.csv')

    print("Success. Exported to " + 'data/reddit/json/data-' + str(time) + '.json' + ' and ' + 'data/reddit/csv/data-' + str(time) + '.csv')


reddit = praw.Reddit(client_id='{CLIENT_ID}',
                     client_secret="{CLIENT_SECRET}",
                     user_agent='{USER_AGENT}')

print("Finished connection... Fetching submissions in the list...")

submissions = [
    "evvzf9",
    "ezo8xn",
    "f1ih07",
    "fbvz7n",
    "fge7ve",
    "fiusxv",
    "floift",
    "fpy8ax",
    "fngmjl"
]


for submission in submissions:
    fetch(reddit, submission)

with open('data/reddit/json/data-merged.json', 'w+') as f:
    json.dump(total, f)

