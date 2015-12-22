__author__ = 'multiangle'

"""
    NAME:       server_database
    PY_VERSION: python3.4
    FUNCTION:--------------------------------------------------------------
    This file deal with the event of databases.
    The several main task is:
    1. Check cahce_attends, and check if the user exists in table of ready_to_get
    and user_info_table. If not , store it into ready_to_get table
    2. Check the data stored in cache_user_info, and store then into user_info_table.
    Be sure that the user in user_info_table should be unique. And delet the user in
    ready_to_get table
    3. Check the data stored in cache_user_get , and store then into atten_web table.
    Also, the attend connection of two people should be unique
    4. Check the ready_to_get table . if some uid is fetching for too much time, set
    the value if is_fetching to null
    -----------------------------------------------------------------------
    VERSION:    _0.1.1_
    UPDATE_HISTORY:
        _0.1.1: add redis and bloom filter as the cache of mysql
        _0.1_:  The 1st edition
"""
# TODO 第三个功能还未实现 。
#======================================================================
#----------------import package--------------------------
# import python package
import urllib.request as request
import urllib.parse as parse
from multiprocessing import Process
import threading
import time
import redis
import os
import json
import http.cookiejar
import re
import random

# import from this folder
import client_config as config
import File_Interface as FI
from DB_Interface import MySQL_Interface
#=======================================================================

#=======================================================================
#---code session
class deal_cache_attends(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        dbi=MySQL_Interface()
        self.dbi=dbi
        self.bf=BloomFilter()


    def  run(self):
        bag=[]
        uid_bag=[]              #与bag类似，只不过存储uid
        bag_size=100             #100次插入一次
        ready_to_get_col=self.dbi.get_col_name('ready_to_get')
        cache_attends_col=self.dbi.get_col_name('cache_attends')
        while True:
            query='select * from cache_attends limit 1000'
            res=self.dbi.select_asQuery(query)
            if res.__len__()==0:
                if bag.__len__()>0:
                    self.dbi.insert_asList('ready_to_get',bag,unique=True)
                    bag=[]
                    # self.bf.insert_asList(uid_bag,'ready_to_get')
                    uid_bag=[]
                time.sleep(1)
                self.dbi=MySQL_Interface()  #更新dbi
                continue

            print('thread cache attends is working')

            for line in res:
                raw_id=line[cache_attends_col.index('uid')]
                in_user_info=self.bf.isContains(raw_id,'user_info_table')   #此处可优化
                if not in_user_info:
                    data=[line[cache_attends_col.index(col)] if col in cache_attends_col else None for col in ready_to_get_col]
                    bag.append(data)
                    uid_bag.append(raw_id)
                    if bag.__len__()>bag_size:
                        self.dbi.insert_asList('ready_to_get',bag,unique=True)
                        # self.bf.insert_asList(uid_bag,'ready_to_get')
                        print('insert once')
                        bag=[]
                        uid_bag=[]
                self.dbi.delete_line('cache_attends','uid',raw_id) # 此处可优化

    def isInUserInfo(self,in_uid):
        col_user_info=self.dbi.get_col_name('user_info_table')
        query='select * from user_info_table where uid={uid}'.format(uid=in_uid)
        res=self.dbi.select_asQuery(query)
        if res.__len__()==0:
            return False
        else:
            return True

class deal_cache_user_info(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.dbi=MySQL_Interface()
        self.bf=BloomFilter()

    def run(self):
        while True:
            if self.dbi.is_empty('cache_user_info'):
                time.sleep(2)
                self.dbi=MySQL_Interface()
                continue
            [res,cache_user_info_col]=self.dbi.select_all('cache_user_info')

            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))      # insert into user info table
            user_info_table_col=self.dbi.get_col_name('user_info_table')
            data= [[line[cache_user_info_col.index(col)] if col in cache_user_info_col else time_stick if col=='insert_time' else '' for col in user_info_table_col ] for line in res]
            uid_list=[line[user_info_table_col.index('uid')] for line in data]
            self.dbi.insert_asList('user_info_table',data,unique=True)          # 插入 user info table
            self.bf.insert_asList(uid_list,'user_info_table')
            print('insert {num} users into user info table'.format(num=data.__len__()))

            uid_list=[line[cache_user_info_col.index('uid')] for line in res]
            q1="delete from {table_name} where uid in ( {id_str_list} ) ;"   # 从cache user info 中删除
            id_str_list=''
            for i in uid_list:
                id_str_list=id_str_list+'\''+str(i)+'\''+','
            id_str_list=id_str_list[:-1]

            query=q1.format(id_str_list=id_str_list,table_name='cache_user_info')
            self.dbi.cur.execute(query)
            self.dbi.conn.commit()

            query=q1.format(id_str_list=id_str_list,table_name='ready_to_get')
            self.dbi.cur.execute(query)
            self.dbi.conn.commit()

class deal_fetching_user(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.dbi=MySQL_Interface()

    def run(self):
        while True:
            self.dbi=MySQL_Interface()
            t=time.time()
            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t-3600))
            query="update ready_to_get set is_fetching=null where is_fetching < \'{time}\' ;".format(time=time_stick)
            # print(query)
            # query='select * from ready_to_get where is_fetching < {time}'.format(time=time_stick)
            self.dbi.update_asQuery(query)
            time.sleep(1)

class control_ready_table(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.dbi=MySQL_Interface()
    def run(self):
        while True:
            self.dbi=MySQL_Interface()
            num=self.dbi.get_line_num('ready_to_get')
            if num>150*1000:
                query='select m.fans_num from (' \
                      'select fans_num from ready_to_get ' \
                      'ORDER BY fans_num limit 50000' \
                      ') as m order by fans_num desc limit 1'
                res=self.dbi.select_asQuery(query)[0][0]
                query='delete from ready_to_get where fans_num<{num}'\
                    .format(num=res)
                self.dbi.update_asQuery(query)
            else:
                time.sleep(600)

class DB_manager(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.p1=deal_cache_attends()
        self.p2=deal_cache_user_info()
        self.p3=deal_fetching_user()
        self.p4=control_ready_table()

    def run(self):
        self.p1.start()
        self.p2.start()
        self.p3.start()
        self.p4.start()
        print('Process: deal_cache_attends is started ')
        print('Process: deal_cache_user_info is started ')
        print('Process: deal_fetching_user is started')
        print('Process: control_ready_table is started')
        while True:
            time.sleep(5)
            if not self.p1.is_alive():
                self.p1=deal_cache_attends()
                self.p1.start()
                print('Process: deal_cache_attends is restarted ')
            if not self.p2.is_alive():
                self.p2=deal_cache_user_info()
                self.p2.start()
                print('Process: deal_cache_user_info is restarted ')
            if not self.p3.is_alive():
                self.p3=deal_fetching_user()
                self.p3.start()
                print('Process: deal_fetching_user is restarted')
            if not self.p4.is_alive():
                self.p4=control_ready_table()
                self.p4.start()
                print('Process: control_ready_table is restarted')

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
        self.bit_size=1<<15
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

    def insert_asList(self,list_input,name):
        for line in list_input:
            self.insert(line,name)

if __name__=='__main__':
    db_thread=DB_manager()              # database thread
    db_thread.start()