__author__ = 'multiangle'
from pymongo import MongoClient

client=MongoClient('localhost',27017)
db=client['microblog_spider']
table_name='user_2016_03'
table=eval('db.{tname}'.format(tname=table_name))
