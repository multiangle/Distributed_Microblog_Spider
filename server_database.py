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
    VERSION:    _0.1_
    UPDATE_HISTORY:
        _0.1_:  The 1st edition
"""
# TODO 第一个和第二个功能已经实现，第三个和第四个还未实现 。其中 关系表 可以加上粉丝数，博客数
#======================================================================
#----------------import package--------------------------
# import python package
import urllib.request as request
import urllib.parse as parse
from multiprocessing import Process
import threading
import time
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
class deal_cache_attends():
    def __init__(self):
        dbi=MySQL_Interface()
        self.dbi=dbi
        self.run()

    def  run(self):
        bag=[]
        bag_size=100             #十次插入一次
        while True:
            query='select * from cache_attends limit 1'
            res=self.dbi.select_asQuery(query)
            if res.__len__()==0:
                if bag.__len__()>0:
                    self.dbi.insert_asList('ready_to_get',bag,unique=True)
                    bag=[]
                time.sleep(2)
                continue

            cache_attends_col=self.dbi.get_col_name('cache_attends')
            uid=res[0][cache_attends_col.index('uid')]

            in_user_info=self.isInUserInfo(uid)
            if not in_user_info:
                ready_to_get_col=self.dbi.get_col_name('ready_to_get')
                data= [[line[cache_attends_col.index(col)] if col in cache_attends_col else None for col in ready_to_get_col]for line in res]
                self.dbi.delete_line('cache_attends','uid',uid)
                bag+=data
                if bag.__len__()>bag_size:
                    self.dbi.insert_asList('ready_to_get',bag,unique=True)
                    bag=[]
                    print('insert once')

    def isInUserInfo(self,in_uid):
        col_user_info=self.dbi.get_col_name('user_info_table')
        query='select * from user_info_table where uid={uid}'.format(uid=in_uid)
        res=self.dbi.select_asQuery(query)
        if res.__len__()==0:
            return False
        else:
            return True

class deal_cache_user_info():
    def __init__(self):
        self.dbi=MySQL_Interface()
        self.run()

    def run(self):
        while True:
            if self.dbi.is_empty('cache_user_info'):
                time.sleep(1)
                continue
            [res,cache_user_info_col]=self.dbi.select_all('cache_user_info')

            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))      # insert into user info table
            user_info_table_col=self.dbi.get_col_name('user_info_table')
            data= [[line[cache_user_info_col.index(col)] if col in cache_user_info_col else time_stick if col=='insert_time' else '' for col in user_info_table_col ] for line in res]
            self.dbi.insert_asList('user_info_table',data,unique=True)
            print('insert {num} users into user info table'.format(num=data.__len__()))

            uid_list=[line[cache_user_info_col.index('uid')] for line in res]
            query="delete from cache_user_info where uid=\'{uid}\' ;"
            for i in uid_list:
                q=query.format(uid=i)
                self.dbi.cur.execute(q)
            self.dbi.conn.commit()

class DB_manager(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.p1=Process(target=deal_cache_attends,args=())
        self.p2=Process(target=deal_cache_user_info,args=())

    def run(self):
        self.p1.start()
        self.p2.start()
        print('Process: deal_cache_attends is started ')
        print('Process: deal_cache_user_info is started ')
        while True:
            time.sleep(5)
            if not self.p1.is_alive():
                self.p1=Process(target=deal_cache_attends,args=())
                self.p1.start()
                ('Process: deal_cache_attends is restarted ')
            if not self.p2.is_alive():
                self.p2=Process(target=deal_cache_user_info,args=())
                self.p2.start()

