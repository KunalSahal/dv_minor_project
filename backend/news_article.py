import asyncio
import feedparser as fp
from transformers import pipeline
from db_connect import news_article_collection

'''
/rss 
1. gives the original xml format which is suitable for fp.parse to parse through it properply 
2. seperating title, summary based on tag
'''
parsed_data=fp.parse("https://finance.yahoo.com/news/rss/")
pipe=pipeline("sentiment-analysis", model="ProsusAI/finbert", driver=-1)

'''
Data Extraction & Transformation
'''
def sentiment_index(label, score):
    match label:
        case 'positive': return +score
        case 'negative': return -score
        case _ : return 0 

filtered_data=[]
for i in list(parsed_data['entries']):
    date=i['published'].split('T')
    insert_data={'_id': i['link'], 'pub_date':date[0], 'title':i['title']}
    filtered_data.append(insert_data)
    
for i in filtered_data:
    analyzed_data=pipe(i['title'])[0]
    i['sentiment_index']=sentiment_index(analyzed_data['label'], round(analyzed_data['score'],2))
    
# for i in filtered_data:
#     counter=0

'''
Data Loading
'''
async def insert_data(filtered_data):
    for data in filtered_data:
        await news_article_collection.update_one( 
                {
                    '_id': data['_id']
                },
                {
                    '$set': data
                }, 
                upsert=True
            )
    
async def main():
    await insert_data(filtered_data)

asyncio.run(main())
    
    
    
    
    
    

