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
from client import parseMicroblogPage
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
        # 监控proxy pool,建议get_proxy_pool单独开一个线程，
        # 如果server立即返回则马上关闭，否则设为长连接
        if self.task_type=='update':
            sub_thread = AsyUpdateHistory(self.proxy_pool,self.task_uid)
        sub_thread.start()
        inner_count = 0
        while True:
            inner_count += 1
            time.sleep(0.1)  #每隔0.1秒检查情况

            if inner_count==20:     # print the size of proxy pool
                print('client->run: the size of proxy pool is ',
                      self.proxy_pool.__len__())
                inner_count=0

            if self.proxy_pool.__len__()<int(config.PROXY_POOL_SIZE*2/3):
                err_str='client->run : request proxy from server'
                info_manager(err_str)
                self.get_proxy_pool(self.proxy_pool,config.PROXY_POOL_SIZE)

            if not sub_thread.is_alive():
                # self.return_proxy()
                break

class AsyUpdateHistory():
    def __init__(self,proxy_pool,task):
        self.task = task
        self.proxy_pool = proxy_pool

    def run(self):
        # ori_task_list是一个数组，里面每个元素格式：1005051003716184-1457818009-1446862845
        ori_task_list=self.task.split(';')
        self.mission_id=ori_task_list[-1]
        ori_task_list=ori_task_list[0:-1]
        def trans_task_dict(data):
            ret = {}
            tmp = data.split('-')
            # 在下述字段中， reconn_limit表示单个代理下最多重连次数。
            ret['container_id']     = tmp[0]        # container id
            ret['update_time']      = tmp[1]        # update time
            ret['latest_blog']      = tmp[2]        # latest blog
            ret['reconn_limit']     = 3             # 最多重连次数(在使用一个ip情况下)
            ret['proxy_limit']      = 3             # 最多更换的proxy数目
            ret['retry_left']      = config.LARGEST_TRY_TIMES  # 最多重新尝试次数
            return ret
        task_dict_list = [trans_task_dict(x) for x in ori_task_list]
        random.shuffle(task_dict_list)
        contents = []

    @asyncio.coroutine
    async def updateHistory_asyMethod(self, task_dict, ret_content):

        # 初始化变量
        container_id    = task_dict['container_id']
        update_time     = task_dict['update_time']
        latest_blog     = task_dict['latest_blog']
        reconn_limit    = task_dict['reconn_limit']
        proxy_limit     = task_dict['proxy_limit']
        retry_left      = task_dict['retry_left']

        url_model='http://m.weibo.cn/page/json?containerid={cid}_-_WEIBO_SECOND_PROFILE_WEIBO&page={page}'
        aconn = AsyConnector(self.proxy_pool)

        page_undealed_list = []
        page = 1
        # this func exec the normal seq, and there will be another func to deal with unsuccess page
        while True:
            try:
                res = await self.getPageContent()

    @asyncio.coroutine
    async def getPageContent(self, url, proxy_limit,
                             reconn_limit, timeout=10):
        aconn = AsyConnector(self.proxy_pool)

        # get page
        try:
            page = await aconn.getPage(url,
                                          proxy_limit,
                                          reconn_limit,
                                          timeout=timeout)
        except Exception as e:
            raise IOError("Unable to get page , url:{u}"
                               .format(u=url))
        # parse page
        try:
            pmp = parseMicroblogPage()
            res = pmp.parse_blog_page(page)
            return res
        except Exception as e:
            raise ValueError("Unable to parse page, page:\n{p}".format(p=page))


class AsyConnector():
    def __init__(self, proxy_pool, if_proxy=True):
        self.proxy_pool = proxy_pool
        self.if_proxy   = if_proxy

    @asyncio.coroutine
    async def getPage(self, url, proxy_limit, reconn_limit,
                      proxy_used=0, timeout=10):
        proxy = self.proxy_pool.get(1)
        try:
            page = await self.__single_connect(url,
                                               proxy,
                                               reconn_limit,
                                               timeout=timeout)
            print("success to get page after try {t} proxies"
                  .format(t=proxy_used))
            return page
        except Exception as e:
            print("Error from AsyConnector.getPage : reason:\n{e}"
                  .format(e=e))
            if proxy_used < proxy_limit:
                print('this proxy seems invalid, ready to change one, '
                      'the {i}th proxy'.format(i=proxy_used+1))
                return await self.getPage(url,
                                          proxy_limit,
                                          reconn_limit,
                                          proxy_used+1,
                                          timeout=timeout)
            else:
                raise RuntimeError("** warning: can not get page, "
                                   "boooooooooooom")

    @asyncio.coroutine
    async def __single_connect(self,url, proxy, reconn_limit,  # 处理单个proxy的任务
                               reconn_times=0, timeout=10):
        headers = {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 8_0 like Mac OS X) '
                                 'AppleWebKit/600.1.3 (KHTML, like Gecko) Version/8.0 Mobile'
                                 '/12A4345d Safari/600.1.4'}
        conn = aiohttp.ProxyConnector(proxy=proxy, conn_timeout=timeout)
        async with aiohttp.ClientSession(connector=conn) as session:
            try:
                async with session.get(url, headers=headers) as resp:
                    content = await resp.read()
                    content = content.decode('utf8')
                print("success to get page after reconn {t} times".format(t=reconn_times))
                return content
            except Exception as e:
                print("Error from AsyConnector.__single_connect: \nreason :{x}".format(x=e))
                if reconn_times < reconn_limit:
                    print("reconn again, the {i} times".format(i=reconn_times+1))
                    return await self.__single_connect(url, proxy, reconn_limit,
                                                       reconn_times+1, timeout=timeout)
                else:
                    raise RuntimeError("** warning: can not get page, ready to change proxy and retry")


def info_manager(info_str,type='NORMAL'):
    time_stick=time.strftime('%Y/%m/%d %H:%M:%S ||',
                             time.localtime(time.time()))
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

