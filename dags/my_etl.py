import logging
from datetime import datetime
from typing import Dict
import pandas as pd

import requests
from airflow.decorators import dag, task

API_URL :str = 'https://randomuser.me/api/?results=50'

@dag( dag_id='taskflow_api_dag', description='This is a demo about taskflow api',start_date=datetime(2025,2,5),schedule='@daily',catchup=False)
def taskflow():
    @task(task_id='extract', retries=2)
    def extract_users():
        return requests.get(API_URL).json().get("results", [])
    
    @task(task_id="transform")
    def process_data(response) -> list:
        users:list = []
        for user in response:
            record = {
                "firstname": user.get("name",[]).get("first",[]),
                "lastname": user.get("name",[]).get("last",[]),
                "gender": user.get("gender",[]),
                "email": user.get("email",[]),
                "phone": user.get("phone",[])
            }
            users.append(record)
        logging.info(users)
        return users
    
    @task
    def store_data(data):
        logging.info(f"Storing data...")
        df = pd.DataFrame(data)
        df.to_csv('/tmp/random_users.csv',index=False)
    
    store_data(process_data(extract_users()))

taskflow()