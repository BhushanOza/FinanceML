# AUTHOR: BHUSHAN OZA

import os
import pandas as pd
from datetime import datetime
import numpy as np
from math import log
import ast
from time import gmtime, strftime
import json

# read
path = '.'
filepaths = [f for f in os.listdir(path) if f.startswith('BHPAX')]
df = pd.concat(map(pd.read_csv, filepaths))

# process
df = df[['Date-Time','Type','Price','Volume']]
df = df.drop(df[df.Type != 'Trade'].index)
df = df.drop(df[pd.isna(df.Price)].index)
df['PV'] = df.Price * df.Volume
for i in range(len(df['Date-Time'])):
    df.iat[i, df.columns.get_loc('Date-Time')] = (df.iat[i, df.columns.get_loc('Date-Time')])[0:10]+' '+(df.iat[i, df.columns.get_loc('Date-Time')])[11:19]

# scan
period = input('Enter aggregation period in minutes: ')
print('Selected period is '+str(period)+' minutes.')  

# calculate
df['Date-Time'] = pd.to_datetime(df['Date-Time'])
newDf = pd.DataFrame()
newDf['TradeCount'] = df.groupby(pd.Grouper(key='Date-Time', axis=0, freq=(str(period)+'T'))).size()
averaged = df.groupby(pd.Grouper(key='Date-Time', axis=0, freq=(str(period)+'T'))).mean(numeric_only=True)
newDf['AveragePrice'] = averaged.iloc[:,0]
newDf = newDf.fillna(0)
newDf = newDf.drop(newDf[newDf.TradeCount <= 0].index)
summed = df.groupby(pd.Grouper(key='Date-Time', axis=0, freq=(str(period)+'T'))).sum(numeric_only=True)
newDf['DollarVolumeTraded'] = summed.iloc[:,2]
newDf['ShareVolumeTraded'] = summed.iloc[:,1]
newDf = newDf[['DollarVolumeTraded', 'ShareVolumeTraded', 'TradeCount', 'AveragePrice']]

# convert
newDf = newDf.reset_index()
newDf['Date-Time'] = pd.to_datetime(newDf['Date-Time'],format='%Y-%m-%d %00:00:00.00 %Z' ).astype(str)
newJSON = newDf.to_json(orient="records")
newJSON = ast.literal_eval(newJSON)
now = datetime.now()
date_time = now.strftime("%Y-%m-%d %H:%M:%S.%f")
tzs = 'UTC+10'
jsonfile_TSB = {"data_source":"reuters", 
                "dataset_type": "Intraday aggregated stock stats", 
                "dataset_id": "http://bucket-name.s3-website-Region.amazonaws.com", 
                "time_object":{"timestamp":date_time, "timezone":tzs},
                "events":[]}
for i in range(len(newJSON)):
    datetime_object = pd.to_datetime(newJSON[i]['Date-Time'], format='%Y-%m-%d %H:%M:%S.%f' )
    datetime_object= datetime_object.strftime('%Y-%m-%d %H:%M:%S.%f')
    jsonfile_TSB['events'].append({"time_object":{"timestamp":datetime_object, "duration":int(period), "duration_unit":"minutes","timezone":tzs},
                                   "event_type":'intraday aggregated stats',   
                                   "attribute":newJSON[i]})

# write
with open("jsonfile_TSB.json", "w") as write_file:
    json.dump(jsonfile_TSB, write_file, indent=4)