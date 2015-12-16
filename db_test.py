__author__ = 'multiangle'
import File_Interface as FI
from DB_Interface import MySQL_Interface
import json
import re
import urllib.request as request
import urllib.parse as parse
import time
import datetime
import redis
import random

class SimpleHash():
    def __init__(self,cap,seed):
        self.cap=cap
        self.seed=seed
    def hash(self,value):
        ret=0
        for i in range(value.__len__()):
            ret+=self.seed*ret+ord(value[i])
        return ((self.cap-1) & ret)

class BloomFilter():
    def __init__(self):
        self.bit_size=1<<25
        self.seeds=[5,7,11,13,31,37,61]
        self.r=redis.StrictRedis(host='127.0.0.1',port=6379,db=0)
        self.hashFunc=[]
        for i in range(self.seeds.__len__()):
            self.hashFunc.append(SimpleHash(self.bit_size,self.seeds[i]))

    def isContains(self,str_input,name):
        if str_input==None:
            return False
        if str_input.__len__()==0:
            return False
        ret=True
        for f in self.hashFunc:
            loc=f.hash(str_input)
            ret=ret & self.r.getbit(name,loc)
        return ret

    def insert(self,str_input,name):
        for f in self.hashFunc:
            loc=f.hash(str_input)
            self.r.setbit(name,loc,1)

        # dbi=MySQL_Interface(dbname='test')

        # insert_str="insert into hehe(b,time) values (\'{v_value}\',\'{time_stick}\') ;"
        # query='select * from hehe where time is not null'
        # print(dbi.select_asQuery(query))

        # res=dbi.select_asQuery("select * from ready_to_get where is_fetching is not null ;")
        # for i in res:
        #     s=i[-1]
        #     x=s.timestamp()
        #     # x=datetime.datetime.strftime('%Y-%m-%d %H:%M:%S',s)
        #     print(x)



        # data=FI.load_pickle('F:\\multiangle\\Coding!\\python\\Distributed_Microblog_Spider\\data.pkl')
        # data=parse.urlencode(data).encode('utf8')
        # url='http://127.0.0.1:8000/info_return'
        # req=request.Request(url,data)
        # opener=request.build_opener()
        # res=opener.open(req)
        # print(res.read().decode('utf8'))

        # print(dbi.is_empty('cache_user_info'))

        # data=data['user_attends']
        # col=dbi.get_col_name('cache_attends')
        # res=[[line[i] if i in col else '' for i in col] for line in data]
        # dbi.insert_asList('cache_attends',res,unique=True)

        # res=dbi.select_asQuery('select * from ready_to_get where is_fetching is null order by fans_num desc limit 1;')
        # res=res[0]
        # col_info=dbi.get_col_name('ready_to_get')
        # uid=res[col_info.index('uid')]
        # print(uid)

        #
        # data=data['user_attends']
        # col_info=dbi.get_col_name('ready_to_get')
        # keys=data[0].keys()
        # data= [[line[i] if i in keys else None if i=='is_fetching' else '' for i in col_info] for line in data]
        # table_name='ready_to_get'
        # data_list=data

        # d1=data[0:300]
        # d2=data[100:]
        # d2[-1][-1]+=1
        # print(d2[-1])
        #
        # dbi.insert_asList(table_name,data,unique=True)



        # for i in range(data.__len__()):
        #     data[i][2]=''
        # data[i][5]=''

        # dbi.insert_asList('cache_attends',data)
        # invalid_data=[]
        # for i in range(data.__len__()):
        #     try:
        #         dbi.insert_asList('ready_to_get',[data[i]])
        #     except Exception as e:
        #         print(data[i])
        #         invalid_data.append(data[i])
        # print(e)


dbi=MySQL_Interface(dbname='microblog_spider')
r=redis.StrictRedis(host='127.0.0.1',port=6379,db=0)
query='select uid from user_info_table ;'
uid=dbi.select_asQuery(query)
uid=[x[0] for x in uid]
bf=BloomFilter()
for id in uid:
    bf.insert(id,'user_info_table')
# print(invalid_data)
# FI.save_pickle(invalid_data,'test.pkl')

# data=FI.load_pickle('test.pkl')
# print(data)

