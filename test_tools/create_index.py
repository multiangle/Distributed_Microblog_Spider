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

t = ['user_2016_05','user_2016_06','user_2016_07','user_2016_08']
create_index_asTable(t)
