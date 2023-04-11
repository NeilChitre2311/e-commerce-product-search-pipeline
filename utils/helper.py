import configparser
import json
import os
import pymongo
import requests
from user_definition import *


def fetch_categories():
    
    config = configparser.ConfigParser()
    dirname = os.path.abspath(os.path.dirname(__file__))
    config.read(dirname+"/config.ini")
    
    url = config["API"]["CATEGORY_URL"]
    querystring = {"country":"US","lang":"en-US"}
    
    headers = {
            "X-RapidAPI-Key": config["API"]["RAPIDAPI_KEY"],
            "X-RapidAPI-Host": config["API"]["RAPIDAPI_HOST"]
        }
    categories = {}
    try:
        response = requests.request("GET", url, headers=headers, params=querystring)
        categories_response = json.loads(response.text)
        
        for brand in categories_response['brands']:
            for children in brand['children']:
                categories[children['link']['categoryId']] = {"web_url":children['link']['webUrl'],"app_url":children['link']['appUrl']}
        
    except Exception as e:
        print(e)
    
    return categories

def categories_indexing():    
    connection_url = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_ip_address}"
    client = pymongo.MongoClient(connection_url)
    db = client[database_name]
    collection = db[categories_collection]
    
    categories = fetch_categories()
    
    for category in list(categories.keys()):
        try: 
            data = {}
            data["_id"] = int(category)
            data["web_url"] = categories[category]["web_url"]
            data["category_name"] = (
                categories[category]["web_url"].split("/")[-3].replace("-", " ").title()
            )
            data["complete"] = False
            data["offset"] = 0
            collection.replace_one({"_id": int(category)}, data, upsert=True)
        except Exception as e:
            print(e)