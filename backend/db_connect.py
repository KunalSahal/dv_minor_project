import os
import certifi
from dotenv import load_dotenv
from pymongo import AsyncMongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

client = AsyncMongoClient(
    MONGO_URI,
    tlsCAFile=certifi.where()
)
db = client["global_market_db"]
news_article_collection = db["news_articles"]
stocks_collection = db["stocks"]




