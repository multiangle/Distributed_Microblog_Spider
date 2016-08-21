__author__ = 'multiangle'
"""
    NAME:       client_asy_update.py
    PY_VERSION: python3.5
    FUNCTION:   This client part of distrubuted microblog spider.
                Can execute update mission through asynchronize ways
    VERSION:    _0.1_

"""
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
from random import Random
import math
import aiohttp
import asyncio

# import from this folder
import client_config as config
import File_Interface as FI
from data_transport import upload_list
#=======================================================================

class clientAsyUpdate():
    def __init__(self, uuid=None):
        self.task_uid = uuid
        self.task_type = None
        check_server()
        self.get_task()
        self.proxy_pool=[]
        self.get_proxy_pool(self.proxy_pool,config.PROXY_POOL_SIZE)
        self.run()

    def get_task(self):

        """
        get task user id from server
        """
        if not self.task_uid:
            self.task_uid = config.UUID
        url='{url}/task/?uuid={uuid}'.format(url=config.SERVER_URL,uuid=self.task_uid)

        try:
            res=request.urlopen(url,timeout=10).read()
            res=str(res,encoding='utf8')
        except Exception as e:
            check_server()     # sleep until server is available
            try:
                res=request.urlopen(url,timeout=10).read()
                res=str(res,encoding='utf8')
            except:
                err_str='error: client -> get_task : ' \
                        'unable to connect to server, exit process'
                info_manager(err_str,type='KEY')
                os._exit(0)

        if 'no task' in res:       # if server have no task uid ,return 'no task uid'
            err_str= 'error: client -> get_task : ' \
                     'unable to get task, sleep for 1 min and exit process'
            info_manager(err_str,type='KEY')
            time.sleep(60)
            os._exit(0)

        try:        # try to parse task str
            res=res.split(',')
            self.task_uid=res[0]
            self.task_type=res[1]
        except:
            err_str='error: client -> get_task : ' \
                    'unable to split task str,exit process'
            info_manager(err_str,type='KEY')
            os._exit(0)

    def get_proxy_pool(self,proxy_pool,num):

        """
        request certain number of proxy from server
        :param num:
        :return: None, but a list of proxy as formation of [[proxy(str),timeout(float)]...[]]
                    will be added to self.proxy_pool
        """

        url='{url}/proxy/?num={num}'.format(url=config.SERVER_URL,num=num)

        try:
            res=request.urlopen(url,timeout=10).read()
            res=str(res,encoding='utf8')
        except:
            time.sleep(5)
            check_server()     # sleep until server is available
            try:
                res=request.urlopen(url,timeout=10).read()
                res=str(res,encoding='utf8')
            except Exception as e:
                err_str='error: client -> get_proxy_pool : unable to ' \
                        'connect to proxy server '
                info_manager(err_str,type='KEY')
                if config.KEY_INFO_PRINT:
                    print('Error from client.get_proxy_pool,reason:',e)
                return

        if 'no valid proxy' in res:     # if server return no valid proxy, means server
            # cannot provide proxy to this client
            err_str='error: client -> get_proxy_pool : fail to ' \
                    'get proxy from server'
            info_manager(err_str,type='KEY')
            time.sleep(1)
            return

        try:
            data=res.split(';')             # 'url,timedelay;url,timedelay;.....'
            data=[proxy_object(x) for x in data]
        except Exception as e:
            err_str='error: client -> get_proxy_pool : fail to ' \
                    'parse proxy str info:\r\n'+res
            info_manager(err_str,type='KEY')
            return

        proxy_pool[:]=proxy_pool[:]+data

    def return_proxy(self):

        """
        return useful or unused proxy to server
        """

        check_server()
        url='{url}/proxy_return'.format(url=config.SERVER_URL)
        proxy_ret= [x.raw_data for x in self.proxy_pool]
        proxy_str=''

        for item in proxy_ret:
            proxy_str=proxy_str+item
        data={
            'data':proxy_str
        }

        data=parse.urlencode(data).encode('utf-8')

        try:
            opener=request.build_opener()
            req=request.Request(url,data)
            res=opener.open(req).read().decode('utf-8')
        except:
            try:
                opener=request.build_opener()
                req=request.Request(url,data)
                res=opener.open(req).read().decode('utf-8')
            except:
                err_str='error:client->return_proxy:unable to ' \
                        'connect to server'
                info_manager(err_str,type='KEY')
                return

        if 'return success' in res:
            print('Success: return proxy to server')
            return
        else:
            err_str='error:client->return_proxy:'+res
            info_manager(err_str,type='KEY')
            # raise ConnectionError('Unable to return proxy')
            return

    def run(self):
        pass


def info_manager(info_str,type='NORMAL'):
    time_stick=time.strftime('%Y/%m/%d %H:%M:%S ||', time.localtime(time.time()))
    str=time_stick+info_str
    if type=='NORMAL':
        if config.NOMAL_INFO_PRINT:
            print(str)
    elif type=='KEY':
        if config.KEY_INFO_PRINT:
            print(str)
    elif type=='DEBUG':
        if config.DEBUG_INFO_PRINT:
            print(str)
    else:
        print('Error from info_manager : unknown type')

def check_server():

    """
    check if server can provide service
    if server is valid, will return 'connection valid'
    """

    url='{url}/auth'.format(url=config.SERVER_URL)
    while True:

        # # todo 异常危险！！！ 把checkserver这一步跳过了
        # break

        try:
            res=request.urlopen(url,timeout=10).read()
            res=str(res,encoding='utf8')
            #--------------------------------------------------
            if 'connection valid' in res:
                break
            else:
                error_str='error: client-> check_server :' \
                          'no auth to connect to server,exit process'
                info_manager(error_str,type='KEY')
                os._exit(0)
        except Exception as e:
            err_str='error:client->check_server:cannot ' \
                    'connect to server; process sleeping'
            info_manager(err_str,type='NORMAL')
            print('Error from check_server',e,' url is',url)
            time.sleep(5)       # sleep for 1 seconds

if __name__=='__main__':
    p_pool = []
    # uuid = int(input('client id : '))
    uuid = 4

