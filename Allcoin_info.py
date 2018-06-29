
# coding: utf-8

# In[2]:

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
db = client['das']
collection = db['DAS.exchange.bycoin']
coin_list = []
coll = collection.find({},{'id':1,'name':1,'symbol':1,'logo':1})
for item in coll:
    coin_list.append(item)   
client.close()
df_coin = pd.DataFrame(coin_list)
df_coin = df_coin[['id','name','symbol','logo']]


# In[3]:

import requests
import time
page = requests.get('https://coinmarketcap.com/exchanges/allcoin/')

from forex_python.converter import CurrencyRates
c = CurrencyRates()
currency_rates = c.get_rates('USD')
currencies = currency_rates.keys()

from bs4 import BeautifulSoup
soup = BeautifulSoup(page.content, 'html.parser')
trading_dict = dict()
market_id = 52
market_logo = 'https://cdn.mytoken.org/Fs3FzBgLkPBFW5uedHK6STkaQ-DW'
market_url = 'https://www.allcoin.com/'

td_list = list(soup.find_all('td'))
ticker_list = []
ticker_count = 0
start_time = time.time()
for n in td_list:
    if 'data-sort' not in str(n):
        continue
    else:
        text= n.get_text()
        if text==None:
            continue
        if '/' in text and (text not in ticker_list):
            print text,'计算用时：'+str(round(time.time() - start_time,2)) + '秒'
            ticker_list.append(text)
            ticker = text.split('/')
            
            try:
                coin_id = df_coin[df_coin.symbol==ticker[0]].id.values[0]
                logo = df_coin[df_coin.symbol==ticker[0]].logo.values[0]
            except:
                try:
                    coin_id = df_coin[df_coin.name==ticker[0]].id.values[0]
                    logo = df_coin[df_coin.name==ticker[0]].logo.values[0]
                except:
                    print ticker[0]+'cant find this coin'
                    ticker_count=3
                    continue
            ticker_count = 0
            
            if ticker[1]=='CKUSD':
                ticker[1]='USDT'

        if ('$' in text) and (ticker_count == 0):
            volume = float(text.replace('\n','').replace('$','').replace(',','').replace('*',''))
            ticker_count = ticker_count+1
            continue
            if volume == 0:
                ticker_count = 3
                continue
        if ('$' in text) and (ticker_count == 1):
            price = float(text.replace('\n','').replace('$','').replace(',','').replace('*',''))
            ticker_count = ticker_count+1
        if ticker_count ==2:
            ticker_count=0
            
            #获取开盘价
            try:
                open_price = find_open_price(ticker[0])
            except:
                try:
                    open_price = find_ccc_price(ticker[0])
                except:
                    print pair_info[0],' cant find coin'
                    continue
    
            if ticker[1] in currencies:
                base_open_price = 1/currency_rates[ticker[1]]
            else:
                try:
                    base_open_price = find_open_price(ticker[1])
                except:
                    base_open_price = find_ccc_price(ticker[1])
        
            percent = (price-open_price)*100/open_price
            volume = volume/base_open_price
            price = price/base_open_price
    
            client = MongoClient('mongodb://online:gcd2e17eadb5d@dds-wz9424a1dbd524d41381-pub.mongodb.rds.aliyuncs.com:3717,dds-wz9424a1dbd524d42896-pub.mongodb.rds.aliyuncs.com:3717/das?replicaSet=mgset-4129285')
            db = client['das']
            collection = db['DAS.exchange.basic']
            collection.update_one({'name':'Allcoin'},{'$set':{'trading'+'.'+ticker[1]+'.'+ticker[0]:{'coin_id':coin_id,'logo':logo,'update_time':int(time.time()),'last_price':price,'percent':percent,'volume':volume}}},upsert=True)
            
            collection = db['DAS.exchange.bycoin']
            collection.update_one({'id':coin_id},{'$set':{ticker[1]+'.'+'Allcoin':{'update_time':int(time.time()),'last_price':price,'percent':percent,'volume':volume,'exchange_id':market_id,'logo':market_logo,'url':market_url}}},upsert=True)
            client.close()
            


# In[ ]:



