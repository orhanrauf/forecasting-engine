import logging
import math 
import numpy as np
import pandas as pd
import json
import azure.functions as func

# TODO: logging, testing


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function received a request.')

    key = req.headers.get('Key')
    interval = req.headers.get('Interval')
    
    #if HTTP request contains key, put json body inside dictionary and retrieve suggested values and outliers
    if key:
        data = req.get_json()
        suggested_values = suggest_missing_values_and_outliers(data, key, interval)  
        return suggested_values


    # if the HTTP request does not contain a key that is called 'key', return 400P

    else:
        return func.HttpResponse(
            "Please pass a key on to the query string",
            status_code=400
            )

def suggest_missing_values_and_outliers(values: dict, key: str, interval: str) -> dict:
    # format pandas dataframe from dictionary
    data = pd.DataFrame.from_dict(values.get('series'), orient='index')
    data.columns = values.get('column names')
    data.index.name = 'datetime'
    data.index = pd.DatetimeIndex(data.index)

    print(data)
    
    # reindex the dataframe: if there are missing stamps, puts NaNs for the values in missing stamps 
    data = data.reindex(pd.date_range(data.index.min(), data.index.max(), freq=interval))

    # keep column for missing value records and impute missing values
    for name in values.get('column names'):
        data[name + '_misses_value'] = data[name].isna()

    data = fill_missing_values(data)
    data.fillna(method = 'ffill', inplace=True) #take care of boundary values that could be missing 

    # keep column for outlier records, make outliers NaN and impute missing values
    for name in values.get('column names'):
        data[name + '_outliers'] = is_outliers(data[name])
        data[name] = data.apply(lambda row: None if row[name + "_outliers"] else row[name], axis=1)

    #for name in values.get('column names'):
    #   data[name] = data.apply(lambda row: data[name].mean() if row[name]==None else row[name], axis=1)
    
    data = fill_missing_values(data) 

    #make sure all corner cases are considered
    for name in values.get('column names'):
        data[name].fillna(data[name].mean(), inplace=True)


    json_response = data.to_json()

    print(data[values.get('column names')].to_json())
    headers = {'key':key, 'interval':interval}

    return func.HttpResponse(
        json_response,
        status_code=200,
        headers = {'Key':key, 'Interval':interval, 'Content-Type':'application/json' }
        )

#TODO: write good function that takes care of seasonal nature
def fill_missing_values(data: pd.DataFrame) -> pd.DataFrame:

    for name in data.columns:
        data[name].interpolate(method='linear', inplace=True)
    
    return data

#TODO: write good function that takes care of seasonal nature
def is_outliers(data: pd.DataFrame) -> pd.DataFrame:
    return (data.quantile(0.05 ) > data) | (data > data.quantile(0.95))

