__author__ = 'multiangle'

import pymongo
from DB_Interface import MySQL_Interface
from pymongo import MongoClient

def read_content_in_mongo(select={},field=[],sort='',sort_type='up'):
    client=MongoClient('localhost',27017)
    db=client['microblog_spider']
    collection=db.formal
    res=None
    if field.__len__()==0:
        if sort=='':
            res=[pop_id(x) for x in collection.find(select)]
        else:
            if sort_type=='up':
                res=[pop_id(x) for x in collection.find(select).sort(sort,pymongo.ASCENDING)]
            else:
                res=[pop_id(x) for x in collection.find(select).sort(sort,pymongo.DESCENDING)]
    else:
        f={}
        for item in field:
            f[item]=1

        if sort=='':
            res=[pop_id(x) for x in collection.find(select,f)]
        else:
            if sort_type=='up':
                res=[pop_id(x) for x in collection.find(select,f).sort(sort,pymongo.ASCENDING)]
            else:
                res=[pop_id(x) for x in collection.find(select,f).sort(sort,pymongo.DESCENDING)]
    return res

def pop_id(data):
    data.pop('_id')
    return data

# client=MongoClient('localhost',27017)
# db=client['microblog_spider']
# collection=db.formal
# res=collection.find({'user_name':'TFBOYS-王俊凯'})
res=read_content_in_mongo({'user_name':'任志强'},['dealed_text.left_content','created_at'],'created_timestamp','down')

for line in res:
    print(line)
