import asyncio
from db_connect import news_article_collection
    
async def get_data():
    docs = await news_article_collection.find().to_list(100)
    print(docs)
    
async def main():
    await get_data()

asyncio.run(main())