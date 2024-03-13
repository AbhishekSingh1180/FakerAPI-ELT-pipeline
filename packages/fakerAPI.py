import requests
import pandas as pd
import pendulum
import random
import os


def extract_data() -> None :
    # Extract data from API
    url = 'https://fakerapi.it/api/v1/products'

    price_max = round(random.uniform(100,200),2)

    params = {
        '_quantity' : 5,
        '_taxes' : 12,
        '_price_max' : price_max,
        '_seed' : 1
    }

    response = requests.get(url=url, params=params).json()

    if response['code'] == 404:
        raise Exception(response)

    df = pd.json_normalize(response, record_path='data')

    df['created_ts'] = pd.Timestamp(pendulum.now()).tz_convert('UTC')

    df.drop(columns=['image','images','categories','tags'], inplace=True)
    # df.info(verbose='false',memory_usage='deep')

    # Optimize dtype
    schema_dtype = {
        'id' : 'int32', 
        'name' : 'category', 
        'ean' : 'int64',
        'upc' : 'int64',
        'net_price' : 'float16',
        'taxes' : 'float16',
        'price' : 'float16'
    }

    df = df.astype(schema_dtype)

    # Remove trailling spaces
    str_col = list(df.select_dtypes(include=['object','category']).columns)
    df[str_col] = df[str_col].map(lambda x : x.strip())

    # Round float value to 2 
    float_col = list(df.select_dtypes(include='float16').columns)
    df[float_col] = df[float_col].map(lambda x : round(x,2))

    # Compress and create CSV file
    file_suffix = pendulum.now(tz='UTC').strftime("%Y-%m-%d_%H:%M:%S.%f_%z")
    file_name = f'product_{file_suffix}.csv.gz'
    file_path = f'/opt/airflow/temp/{file_name}'

    os.system('rm -rf temp/*')

    df.to_csv(file_path, index=False, compression='gzip')