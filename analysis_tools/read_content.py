__author__ = 'multiangle'

import pymongo
from DB_Interface import MySQL_Interface
from pymongo import MongoClient
import File_Interface as FI

def read_content_in_mongo(table_name,select={},field=[],limit=-1,sort='',sort_type='up'):
    client=MongoClient('localhost',27017)
    db=client['microblog_spider']
    collection=eval('db.{name}'.format(name=table_name))
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

res=read_content_in_mongo('latest_history',{'user_id':'1681029540'},['dealed_text.left_content','created_at','user_name'],-1,'id','down')

data = FI.load_pickle('demo.pkl')
data = data + res
FI.save_pickle(data,'demo.pkl')
for line in res:
    print(line)
print(res.__len__())

