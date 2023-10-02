import pandas as pd
import googleapiclient.discovery
import googleapiclient.errors
import boto3
from youtube_extract import *
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import pandas
import logging
import sys
import json
s3 = boto3.resource("s3")

logging.basicConfig(stream=sys.stdout,level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
    
logger = logging.getLogger(__name__)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = "YOUR_API_KEY"

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=DEVELOPER_KEY)
    
def video_search(skeyword):
   
    logger.info("Searching for the keyword "+ skeyword)
    request = youtube.search().list(
        part='snippet',
        type = 'video',
        q=skeyword,
        order='relevance',
        # location='20.7365168,72.0978116',
        # locationRadius='50km',
        maxResults='2')
    response = request.execute()
    id_lts=[]
    for item in response['items']:
        id_lts.append(item['id']['videoId'])

    return id_lts


def get_details(list_ids,skeyword):
    comments_lt=[]
    stats_lt=[]
    with ThreadPoolExecutor(max_workers=int(6)) as executor:
        futures = []
        for values in list_ids:
            futures.append(executor.submit(get_comments, values,logger,skeyword))
            futures.append(executor.submit(get_statistics, values,logger,skeyword))

    for future in as_completed(futures):
        rs = future.result()
        if (rs[1] == 'com'):
            comments_lt.append(rs[0])
        if (rs[1] == 'sts'):
            stats_lt.append(rs[0])

    return  comments_lt,stats_lt

def final(comments_lt,stats_lt,skeyword):
   
    logger.info('Converting to pandas dataframe')
    df_comments = pd.DataFrame()
    for a in   comments_lt:
        df = pd.json_normalize(a)
        df_comments = pd.concat([df, df_comments])
    df_statistics=pd.json_normalize(stats_lt)
    df_final=df_statistics.merge(df_comments,how='inner',on='id')
    #df_final.to_csv("tst.csv")
    df_final_json= df_final.to_dict("records")
    logger.info('writing to s3 ')
    s3.Bucket('datadumb').put_object(Key='youtubeextract/'+skeyword+'/'+skeyword+'.json', Body=json.dumps(df_final_json))

def main(skeyword):
    
    list_ids=video_search(skeyword)
    comments_lt,stats_lt=get_details(list_ids,skeyword)
    final_out=final(comments_lt,stats_lt,skeyword)


def lambda_handler(event, context):
        keyword = event["queryStringParameters"]["keyword"]
        main(keyword)
        return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'success': True,
            'message':"File Upload to S3 Complete!!"
        }),
        "isBase64Encoded": False
    }
