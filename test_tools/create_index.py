__author__ = 'multiangle'
from pymongo import MongoClient
import time
import sys
import File_Interface as FI
import matplotlib.pyplot as plt
import pymongo

def create_index_asTable(table_list):
    client=MongoClient('localhost',27017)
    db=client['microblog_spider']
    for t in table_list:
        collec = eval('db.{x}'.format(x=t))
        collec.create_index([('user_id',pymongo.DESCENDING)])
        collec.create_index([('id',pymongo.DESCENDING)])
        print('{t} done'.format(t=t))

def create_index_all():
    client=MongoClient('localhost',27017)
    db=client['microblog_spider']
    res=db.collection_names()
    collec_list=[]
    for x in res:
        if 'user' in x:
            collec_list.append(x)
    for item in collec_list:
        collec=eval('db.{x}'.format(x=item))
        collec.create_index([('user_id',pymongo.DESCENDING)])
        print(item+' is done')

def auto_index():
    client = MongoClient('localhost',27017)
    db = client['microblog_spider']
    collec_list = []
    res=db.collection_names()
    for x in res:
        if 'user' in x:
            collec_list.append(x)
    print('****** start to check the index station of collections in mongodb ******')
    for name in collec_list:
        collec = db.get_collection(name)
        indexs = [x for x in collec.list_indexes()]
        if indexs.__len__()<3: # 此时没有索引
            print('{n} do not have indexes yet, ready to craete'.format(n=name))
            collec.create_index([('user_id',pymongo.DESCENDING)])
            collec.create_index([('id',pymongo.DESCENDING)])
        else:
            # print('{n} has 3 indexs, done'.format(n=name))
            pass
    print('****** all indexes is created ******')

auto_index()