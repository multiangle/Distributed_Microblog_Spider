__author__ = 'multiangle'
"""
    NAME:       client.py
    PY_VERSION: python3.4
    FUNCTION:   This client part of distrubuted microblog spider.
                The client request uid from server, and server
                return the list of uid whose info should be searched.
                After searching data, client will return data to
                server by POST method. If the data wanted to post
                is too large, it will be seperated into severl parts
                and transport individually
    VERSION:    _0.1_

    UPDATE_HISTORY:
        _0.1_:  The 1st edition
"""
#======================================================================
#----------------import package--------------------------
# import python package
import urllib.request as request
from multiprocessing import Process
import threading
from bs4 import BeautifulSoup
import time

# import from this folder
import client_config as config
#=======================================================================

#=======================================================================
#------------------code session--------------------------

class client():          # the main process of client
    def __init__(self):
        self.check_server()
        self.task_uid=self.get_task_uid()
        self.run()

    def check_server(self):
        """
        check if server can provide service
        if server is valid, will return 'connection valid'
        """
        url='{url}/auth'.format(url=config.SERVER_URL)
        while True:
            try:
                res=request.urlopen(url,timeout=5).read()
                res=str(res,encoding='utf8')
                if 'connection valid' in res:
                    break
                else:
                    raise ConnectionError('error: client-> check_server :'
                                          'cannot connect to server')
            except Exception as e:
                print(e)
                time.sleep(1)       # sleep for 1 seconds

    def get_task_uid(self):
        """
        get task user id from server
        """
        url='{url}/task_uid'.format(config.SERVER_URL)
        try:
            res=request.urlopen(url,timeout=10).read()
            res=str(res,encoding='utf8')
        except Exception as e:
            self.check_server()
            res=request.urlopen(url,timeout=10).read()
            res=str(res,encoding='utf8')
        if 'no task uid' in res:       # if server have no task uid ,return 'no task uid'
            raise TypeError('error: client -> get_task_uid : unable to get task uid')
        return res

    def get_basic_info(self,uid):
        """
        get user's basic information,
        :param uid:
        :return:basic_info(dict)
        """
        #TODO
        pass

    def get_proxy_pool(self,num):
        """
        request certain number of proxy from server
        :param num:
        :return: a list of proxy as formation of [[proxy(str),timeout(float)]...[]]
        """
        #TODO
        pass

    def run(self):
        self.basic_info=self.get_basic_info(self.task_uid)
        self.proxy_pool=self.get_proxy_pool(config.PROXY_POOL_SIZE)
        self.atten_list=[]
        self.url_task_list=[]
        #TODO
        # 创建任务队列，建立爬网页线程
        # 监控proxy pool,建议get_proxy_pool单独开一个线程，如果server立即返回则马上关闭，否则设为长连接






if __name__=='__main__':
    p=Process(target=client,args=())
    p.start()

