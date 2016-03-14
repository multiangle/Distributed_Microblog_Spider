__author__ = 'multiangle'

import pymongo
from DB_Interface import MySQL_Interface
from pymongo import MongoClient

def read_content_in_mongo(select={},field=[],limit=-1,sort='',sort_type='up'):
    client=MongoClient('localhost',27017)
    db=client['microblog_spider']
    collection=db.formal
    res=None
    if limit==-1:
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
    else:
        if field.__len__()==0:
            if sort=='':
                res=[pop_id(x) for x in collection.find(select).limit(limit)]
            else:
                if sort_type=='up':
                    res=[pop_id(x) for x in collection.find(select).limit(limit).sort(sort,pymongo.ASCENDING)]
                else:
                    res=[pop_id(x) for x in collection.find(select).limit(limit).sort(sort,pymongo.DESCENDING)]
        else:
            f={}
            for item in field:
                f[item]=1

            if sort=='':
                res=[pop_id(x) for x in collection.find(select,f).limit(limit)]
            else:
                if sort_type=='up':
                    res=[pop_id(x) for x in collection.find(select,f).limit(limit).sort(sort,pymongo.ASCENDING)]
                else:
                    res=[pop_id(x) for x in collection.find(select,f).limit(limit).sort(sort,pymongo.DESCENDING)]
    return res

def pop_id(data):
    data.pop('_id')
    return data

client=MongoClient('localhost',27017)
db=client['microblog_spider']
collec=db.user_2016_02
res=collec.find({},{'dealed_text.left_content':1}).sort('created_timestamp',pymongo.DESCENDING)

for line in res:
    print(line)

