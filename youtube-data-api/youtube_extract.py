import googleapiclient.discovery
import googleapiclient.errors
import pandas as pd
api_service_name = "youtube"
api_version = "v3"
DEVELOPER_KEY = "YOUR_API_KEY1"
DEVELOPER_KEY1="YOUR_API_KEY2"

youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=DEVELOPER_KEY)


youtube_1 = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=DEVELOPER_KEY1)




def get_statistics(id,logger,skeyword):

    logger.info('Running statistics api for '+ id)
    req = youtube.videos().list(part="statistics",
                                id=id)

    response_com = req.execute()
    # print('Youtube Statistics Response',response_com)
    for item in response_com['items']:
        if "likeCount" in item['statistics']:
            lt_com= {"id":item['id'],"viewCount" : item['statistics']['viewCount'],
                    "likeCount":item['statistics']['likeCount'],"searchword":skeyword}
        else:
            lt_com= {"id":item['id'],"viewCount" : item['statistics']['viewCount'],
                    "likeCount":0,"searchword":skeyword}

    logger.info("statistics api completed for " + id)
    return lt_com ,'sts'


def get_comments(id,logger,skeyword):
    logger.info('Running comments api for '+ id)
    request = youtube_1.commentThreads().list(
        part="snippet",
        videoId=id,
        maxResults=10,
        order='relevance'
    )

    try:
     response = request.execute()
     cv= []    
     for item in response['items']:
         if 'snippet' in item:
             cv.append ({"id": item['snippet']['videoId'],
                   "Comment": item['snippet']['topLevelComment']['snippet']['textDisplay'],"searchword":skeyword})
                     
         else:
             cv.append ({"id": id, "Comment": "disabled","searchword":skeyword})

    except Exception as e:
        cv.append ({"id": id, "Comment": "disabled","searchword":skeyword})

    logger.info("comments api completed for "+ id)
    return cv ,'com'