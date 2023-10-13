#clean environment
from IPython import get_ipython
get_ipython().magic('reset -f')
from sqlalchemy import create_engine
import pandas as pd
import pyodbc
import json
import requests
import numpy as np

url = "http://data.gcis.nat.gov.tw/od/data/api/85F5E823-A0D5-4C2C-B7FA-765D80050ABA?$format=json&$filter=Business_Item eq I503010"
skip = 0
batch_size = 1000
data_list = []

while True:
    current_url = f"{url}&$skip={skip}&$top={batch_size}"
    response = requests.get(current_url)

    if response.status_code == 200:
        try:
            batch_data = response.json()
        except ValueError:
            # Handle the case where the response is not valid JSON
            print("Response is not valid JSON.")
            break

        if not batch_data:
            break

        data_list.extend(batch_data)
        skip += batch_size
    else:
        print(f"Request failed with status code {response.status_code}")
        break

I_df = pd.json_normalize(data_list)

I_df.to_excel('I類公司.xlsx', index = False)
