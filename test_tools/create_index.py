__author__ = 'multiangle'
from pymongo import MongoClient
import time
import sys
import File_Interface as FI
import matplotlib.pyplot as plt
import pymongo

client=MongoClient('localhost',27017)
db=client['microblog_spider']
res=db.collection_names()
collec_list=[]
for x in res:
    if 'user' in x:
        collec_list.append(x)
for item in collec_list:
    collec=eval('db.{x}'.format(x=item))
    collec.create_index([('id',pymongo.DESCENDING)])
    print(item+' is done')