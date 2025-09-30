from pymongo import MongoClient
import os

MONGO_URI = os.getenv("Mongo_Uri")

# Connect to Mongo Atlas
try:
    client = MongoClient(MONGO_URI)
    print( "Connection Succeded")
except Exception as e:
    print("❌ ERROR:", str(e))
    raise 

db = client["AMI_DB"]