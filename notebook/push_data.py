import os
import sys
import json
import certifi
import pandas as pd
import numpy as np
import pymongo
from src.exception import MyException
from src.logger import logging
from dotenv import load_dotenv

load_dotenv()
MONGO_DB_URI=os.getenv("MONGO_DB_URI")

ca=certifi.where()

class MongoDBClient:
    def __init__(self, database_name, collection_name):
        try:
            self.client = pymongo.MongoClient(MONGO_DB_URI, tlsCAFile=ca)
            self.database = self.client[database_name]
            self.collection = self.database[collection_name]
        except Exception as e:
            raise MyException(e, sys) from e

    def insert_data(self, data):
        try:
            if isinstance(data, pd.DataFrame):
                data.reset_index(drop=True, inplace=True)
                data_dict = json.loads(data.to_json(orient="records"))
                self.collection.insert_many(data_dict)
                logging.info("Data inserted successfully into MongoDB")
            else:
                raise ValueError("Data must be a pandas DataFrame")
        except Exception as e:
            raise MyException(e, sys) from e
        
if __name__=='__main__':
    try:
        # Load the data
        data = pd.read_csv('notebook/phisingData.csv')
        
        # Create MongoDB client
        mongo_client = MongoDBClient(database_name='Network-Security', collection_name='Phising-Data')
        
        # Insert data into MongoDB
        mongo_client.insert_data(data)
        
    except Exception as e: 
        logging.error(f"An error occurred: {e}")        