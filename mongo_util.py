#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
    @author : xiaohe
    @date   : 2016/10/10
'''
import pymongo
from pymongo import MongoClient

class Mongo(object):
    
    def __init__(self, host, port=None, db=None, user=None, password=None):
        self.__conn = MongoClient(host,port)
        if db != None:
            self.__db = self.__conn[db]
            self.__db.authenticate(user,password)
    
    def get_db(self, db):
        self.__db = self._conn[db]
        return self.__db

    def get_collection(self, coll):
        return self.__db[coll]
    def close(self):
        self.__conn.close() 
