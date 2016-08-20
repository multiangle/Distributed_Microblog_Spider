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
    def __init__(self, uuid):
        self.task_uid = uuid
        self.task_type = None

def info_manager(info_str,type='NORMAL'):
    time_stick=time.strftime('%Y/%m/%d %H:%M:%S ||', time.localtime(time.time()))
    str=time_stick+info_str
    if type=='NORMAL':
        if config.NOMAL_INFO_PRINT:
            print(str)
    if type=='KEY':
        if config.KEY_INFO_PRINT:
            print(str)
    if type=='KEY':
        if config.DEBUG_INFO_PRINT:
            print(str)

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

