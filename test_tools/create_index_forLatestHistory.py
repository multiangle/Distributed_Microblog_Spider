__author__ = 'multiangle'

from pymongo import MongoClient
import time
import sys
import File_Interface as FI
import matplotlib.pyplot as plt
import pymongo


client=MongoClient('localhost',27017)
db=client['microblog_spider']
collec = db.latest_history
collec.create_index([('user_id',pymongo.ASCENDING)])