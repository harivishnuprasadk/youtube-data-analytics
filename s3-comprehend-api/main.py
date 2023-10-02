import json
import urllib.parse
import boto3
from comprehend import *
import datetime

dyn_resource = boto3.resource('dynamodb')
table = dyn_resource.Table('YoutubeAnalyticsData')
s3 = boto3.resource('s3')

def detect_sentiment(data):
    sentiment_list = []
    for values in data:
        fg = comprehend(values['id'], values['Comment'])

        sentiment_list.append({'id': fg['id'],
                               'Comment': fg['Comment'],
                               'Sentimentoverall': fg['Sentiment'],
                               'SentimentScoreMixed': str(fg['SentimentScore']['Mixed']),
                               'SentimentScorNegative': str(fg['SentimentScore']['Negative']),
                               'SentimentScoreNeutral': str(fg['SentimentScore']['Neutral']),
                               'SentimentScorePositive': str(fg['SentimentScore']['Positive'])
                               })
    return sentiment_list


def lambda_handler(event, context):
    try:
        bucket = 'datadumb'
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        # print(key)
        obj = s3.Object(bucket, key)
        data = obj.get()['Body'].read()
        data = json.loads(data.decode("utf-8"))
        sentiment = detect_sentiment(data)
        new_list = []

        for dict_a, dict_b in zip(sentiment, data):
            combined_dict = {
                "id": dict_a["id"],
                "likecount": dict_b["likeCount"],
                "viewcount": dict_b["viewCount"],
                "searchword": dict_b["searchword_x"],
                "videoTitle": dict_b["videoTitle"],
                "comment": dict_a["Comment"],
                "Sentimentoverall": dict_a["Sentimentoverall"],
                "SentimentScoreMixed": dict_a["SentimentScoreMixed"],
                "SentimentScorNegative": dict_a["SentimentScorNegative"],
                "SentimentScoreNeutral": dict_a["SentimentScoreNeutral"],
                "SentimentScorePositive": dict_a["SentimentScorePositive"]
            }
            new_list.append(combined_dict)

        for obj in new_list:
            table.put_item(Item=
                           {'videoId': obj['id'],
                            'Time Stamp': str(datetime.datetime.now()),
                            "searchword": obj['searchword'],
                            "videoTitle": obj['videoTitle'],
                            "Likecount": obj['likecount'],
                            "Viewcount": obj['viewcount'],
                            "comment": obj['comment'],
                            "Sentimentoverall": obj['Sentimentoverall'],
                            "SentimentScorNegative": obj['SentimentScorNegative'],
                            "SentimentScoreMixed": obj['SentimentScoreMixed'],
                            "SentimentScoreNeutral": obj['SentimentScoreNeutral'],
                            "SentimentScorePositive": obj['SentimentScorePositive']
                            })
    except Exception as e:
        print(e)
        raise e
