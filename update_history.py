#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on 2012-6-25
@author: lzs
''' 

import urllib.request
import http.cookiejar
from urllib.parse import quote
import urllib.response
import logging
import logging.config
import json 
import time
import pymongo
from pymongo import MongoClient
import analysis
from analysis import update_dl_stocks

LASTDAY = {} # 个股最近一天的日期记录
NEW_LASTDAY = {} # 个股 最新更新的 交易日期
UPDATE_LASTDAY = {} # 个股 最新的 交易日期
NEW_VOLUME = [] # 新增近几天的交易

class Crawler:
    def __init__(self):
        self.__makeMyOpener()
    
    def __makeMyOpener(self):
        head = {
            'Connection': 'Keep-Alive',
            'Accept': 'text/html, application/xhtml+xml, */*',
            'Accept-Language': 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3',
            'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11'
        }
        cj = http.cookiejar.CookieJar()
        self.__opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        header = []
        for key, value in head.items():
            elem = (key, value)
            header.append(elem)
        self.__opener.addheaders = header
    
    def __fetch(self, url, count=1):
        '''get .csv file from given url'''
        uop = self.__opener.open(url, timeout = 1000)
        data = uop.readlines()
        if uop.getcode() == 200:
            logging.info('success fetch %s !' % url)
        else:
            logging.info('fail fetch %s !' % url)
            logging.info('retry fetch %d times . . .' % count)
            count += 1
            if count > 10:
                logging.error('exit script !')
                exit(-1)
            return self.__fetch(url, count+1)
        return data
    def process(self, url, i):
        try:
            data = self.__fetch(url)
            data = eval(data[0].decode('utf8').strip())
            if data['resultcode'] != '200':
                return
            data = data['result'][0]['data']
        except Exception as e:
            logging.info('{}不存在'.format(url))
            return
        
        global LASTDAY, NEW_VOLUME,  NEW_LASTDAY, UPDATE_LASTDAY

        try:
            symbol = int(i)
            ndate = time.localtime(time.time())
            year = ndate.tm_year
            month = ndate.tm_mon  
            day = ndate.tm_mday
            date = '{}{:0>2}{:0>2}'.format(year,month,day)
            date = int(date)
            openn = float(data['todayStartPri'].replace('"',''))
            high = float(data['todayMax'].replace('"',''))
            low = float(data['todayMin'].replace('"',''))
            close = float(data['nowPri'].replace('"',''))
            volume = int(data['traNumber'].replace('"',''))*100
            if volume == 0: # 如果停牌
                return
            xx = {
                'symbol':symbol,
                'date':date,
                'year':year,
                'month':month,
                'day':day,
                'open':openn,
                'high':high,
                'low':low,
                'close':close,
                'volume':volume,
            }
            
            if symbol in LASTDAY:
                if date > LASTDAY[symbol]:
                    UPDATE_LASTDAY.update({symbol:date})
                    NEW_VOLUME.append(xx)
            else:
                NEW_VOLUME.append(xx)
                NEW_LASTDAY.update({symbol:date})
        except Exception as e:
            logging.error(str(e))
            exit(-1)

def update_db():
    client = MongoClient('127.0.0.1',27028)
    db_auth = client.stock
    db_auth.authenticate('xiaohe','stock123)#)^')
    ld_coll = db_auth.lastday
    volume_coll = db_auth.volume
    global NEW_VOLUME, NEW_LASTDAY, UPDATE_LASTDAY
    logging.info('The length of NEW_VOLUME:{}'.format(len(NEW_VOLUME)))
    logging.info('The length of NEW_LASTDAY:{}'.format(len(NEW_LASTDAY)))
    logging.info('The length of UPDATE_LASTDAY:{}'.format(len(UPDATE_LASTDAY)))
    if len(NEW_VOLUME) > 0:
        for each in NEW_VOLUME:
            try:
                volume_coll.insert_one(each)
            except Exception as e:
                logging.error(each)
    if len(NEW_LASTDAY) > 0:
        for each in NEW_LASTDAY:
            ld_coll.insert_one({'symbol':each, 'lastday':NEW_LASTDAY[each]})
    for each in UPDATE_LASTDAY:
        ld_coll.update_one({'symbol':each},{'$set':{'lastday':UPDATE_LASTDAY[each]}})
    

def _logging_conf():
    logging.basicConfig(level=logging.INFO,  
        format='%(asctime)s [line:%(lineno)d] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename='logging.log',
        filemode='w')

def get_history():
    '''get history data from mongodb'''
    client = MongoClient('127.0.0.1',27028)
    db_auth = client.stock
    db_auth.authenticate('xiaohe','stock123)#)^')
    ld_coll = db_auth.lastday
    global LASTDAY
    for each in ld_coll.find():
        LASTDAY.update({each['symbol']: each['lastday']})

def update():
    get_history()
    logging.info('[success] get last trade date of all stocks.')
    e_date = time.localtime(time.time())
    s_date = time.localtime(time.time()-3600*24*3)
    
    splider=Crawler()
    for i in range(300001,300550):
        #logging.info('process  SZ{}'.format(i))
        print('process  SZ{}'.format(i))
        splider.process('http://web.juhe.cn:8080/finance/stock/hs?gid=sz{}&key=907887c7ebd7383100d46702370390e5'.format(i), str(i))
    for i in range(600000,602001):
        #logging.info('process  SZ{}'.format(i))
        print('process  SH{}'.format(i))
        splider.process('http://web.juhe.cn:8080/finance/stock/hs?gid=sh{}&key=907887c7ebd7383100d46702370390e5'.format(i),str(i))
    for i in range(2001,2756):
        #logging.info('process  SZ{}'.format(i))
        print('process  SZ{}'.format(i))
        splider.process('http://web.juhe.cn:8080/finance/stock/hs?gid=sz00{}&key=907887c7ebd7383100d46702370390e5'.format(i),str(i))
    for i in range(1,1000):
        #logging.info('process  SZ{}'.format(i))
        print('process  SZ{}'.format(i))
        splider.process('http://web.juhe.cn:8080/finance/stock/hs?gid=sz{:0>6}&key=907887c7ebd7383100d46702370390e5'.format(i),str(i))
    logging.info('[success] crawler end .')
    
    update_db()    

if __name__=='__main__':
    
    _logging_conf()
    while True:
        s_time = time.time()
        update()
        update_dl_stocks()
        logging.info('[success] update db .')
        e_time = time.time()
        logging.info('use time {}s'.format(e_time - s_time))
        time.sleep(24*3600)
      
