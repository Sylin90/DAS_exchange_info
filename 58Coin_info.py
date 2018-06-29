
# coding: utf-8

# In[5]:

import numpy as np
import scipy as sp
import pandas as pd
import matplotlib
import sklearn
import datetime
import dateutil.parser

def getDateTimeFromISO8601String(s):
    d = dateutil.parser.parse(s)
    return d
from pymongo import MongoClient
def find_open_price(symbol):
    open_time = getDateTimeFromISO8601String(datetime.datetime.now().date().isoformat()).strftime('%s')
    client = MongoClient('mongodb://online:gcd2e17eadb5d@dds-wz9424a1dbd524d41381-pub.mongodb.rds.aliyuncs.com:3717,dds-wz9424a1dbd524d42896-pub.mongodb.rds.aliyuncs.com:3717/das?replicaSet=mgset-4129285')
    #client.database_names
    db = client['das']
    collection = db['DAS.coinmarketcap.token.price']
    coll=collection.find({'symbol':symbol,'last_updated':{'$gt':int(open_time)}}).sort('last_updated').limit(1)
    price_list = []
    for item in coll:
        price_list.append(item)
    client.close()
    return price_list[0]['quotes']['USD']['price']

def find_ccc_price(symbol):
    open_time = getDateTimeFromISO8601String(datetime.datetime.now().date().isoformat()).strftime('%s')
    import urllib, json
    url = 'https://min-api.cryptocompare.com/data/pricehistorical?fsym='+symbol+'&tsyms=USD&ts='+open_time
    response = urllib.urlopen(url)
    a=json.loads(response.read()).items()
    return a[0][1]['USD']
def get_column(word_list,n):
    column=[]
    for i in range(len(word_list)):
        column.append(word_list[i][n])
    return column

def find_index(word_list,n): #k元数组
    index = None
    for i in range(len(word_list)):
        if word_list[i]==n:
            index = i
    return index

from pymongo import MongoClient
client = MongoClient('mongodb://online:gcd2e17eadb5d@dds-wz9424a1dbd524d41381-pub.mongodb.rds.aliyuncs.com:3717,dds-wz9424a1dbd524d42896-pub.mongodb.rds.aliyuncs.com:3717/das?replicaSet=mgset-4129285')
#client['das'].authenticate("online", "gcd2e17eadb5d",mechanism='SCRAM-SHA-1')
db = client['das']
collection = db['DAS.exchange.bycoin']
coin_list = []
coll = collection.find({},{'id':1,'name':1,'symbol':1,'logo':1})
for item in coll:
    coin_list.append(item)   
client.close()
df_coin = pd.DataFrame(coin_list)
df_coin = df_coin[['id','name','symbol','logo']]


# In[10]:

url = 'https://api.58coin.com/v1/spot/ticker'
import requests
resp = requests.get(url)
coin_pairs = resp.json()['result']


# In[16]:

from forex_python.converter import CurrencyRates
c = CurrencyRates()
currency_rates = c.get_rates('USD')
currencies = currency_rates.keys()

from pymongo import MongoClient
import time
start_time = time.time()
no_coin = []
market_id = 157
market_logo = 'https://cdn.mytoken.org/Ftoz3MheVEPyAG6HRSSzdbav3XGw'
market_url = 'https://www.58coin.com/'
for pair in coin_pairs:
    print pair['symbol'],'计算用时：'+str(round(time.time() - start_time,2)) + '秒'
    pair_info = pair['symbol'].split('_')
    try:
        coin_id = df_coin[df_coin.symbol==pair_info[0]].id.values[0]
        logo = df_coin[df_coin.symbol==pair_info[0]].logo.values[0]
    except:
        try:
            coin_id = df_coin[df_coin.name==pair_info[0]].id.values[0]
            logo = df_coin[df_coin.name==pair_info[0]].logo.values[0]
        except:
            print pair_info[0]+'cant find this coin'
            continue
            
    coin_info = pair
    try:
        last_price = float(coin_info['last'])
        volume = float(coin_info['volume'])*last_price
    except:
        continue
    if (volume==0) or (last_price==0):
        continue
    
    percent = coin_info['change']
    client = MongoClient('mongodb://online:gcd2e17eadb5d@dds-wz9424a1dbd524d41381-pub.mongodb.rds.aliyuncs.com:3717,dds-wz9424a1dbd524d42896-pub.mongodb.rds.aliyuncs.com:3717/das?replicaSet=mgset-4129285')
    try:
        client['das'].authenticate("online", "gcd2e17eadb5d",mechanism='SCRAM-SHA-1')
    except:
        client['das'].authenticate("online", "gcd2e17eadb5d",mechanism='SCRAM-SHA-1')
        print 'mongo timeout exchange'
        continue
    #client.database_names
    db = client['das']
    collection = db['DAS.exchange.basic']
    collection.update_one({'name':'58Coin'},{'$set':{'trading'+'.'+pair_info[1]+'.'+pair_info[0]:{'coin_id':coin_id,'logo':logo,'update_time':coin_info['time'],'last_price':last_price,'percent':percent,'volume':volume}}},upsert=True)
    
    collection = db['DAS.exchange.bycoin']
    collection.update_one({'id':coin_id},{'$set':{pair_info[1]+'.'+'58Coin':{'update_time':coin_info['time'],'last_price':last_price,'percent':percent,'volume':volume,'exchange_id':market_id,'logo':market_logo,'url':market_url}}},upsert=True)
    client.close()


# In[ ]:



