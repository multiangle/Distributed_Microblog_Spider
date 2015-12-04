__author__ = 'multiangle'
"""
    NAME:       server_proxy.py
    VERSION:    _0.1_
    FUNCTION:   This part is used to get large number of proxy
                from certain http proxy website, then verify if
                they are useful. Useful proxy is saved in cache
                and provided to client to get info from website
    UPDATE HISTORY:
        _0.1_:  the first edition
"""
#======================================================================
#----------------import package--------------------------
# import python package
from multiprocessing import Process
import os
import time
import threading
import urllib.request as request

# import from this folder
from server_config import GET_PROXY_URL,PROXY_POOL_SIZE,PROXY_PATH     #about proxy
import File_Interface as FI
#=======================================================================

class proxy_manager():
    """
    Core Data:      proxy_pool ,formation as [[],[],[]]
    Method:         get(num)
                    add(data)
    """
    def __init__(self,proxy_pool_size=PROXY_POOL_SIZE):
        # self.raw_proxy_pool=[]
        # self.raw_proxy_pool_size=10*proxy_pool_size     #一般情况下，rawproxy池大小是proxy池大小的10倍
        proxy_pool=[]
        self.proxy_pool=proxy_pool
        self.proxy_pool_size=proxy_pool_size
        self.start_up()

    def start_up(self):
        """
        function:   used to recover info when start up this process
                    for example, read stored proxy list
        """
        pass
        #TODO

    def run(self):
        """
        function:   The main circle of this process.
                    Monitor the state of proxy pool
                    Receive and transmit signal of event between threads processes.
                    send valid proxy to client terminal
        """
        #TODO
        pass
    def get(self,num):
        #TODO
        pass

class find_valid_proxy(threading.Thread):
    """
    function:   Get raw proxy list,check them ,and find valide proxy list
    """
    #TODO
    def __init__(self,proxy_pool):
        threading.Thread.__init__(self)
        self.proxy_pool=proxy_pool      #proxy pool
        self.raw_proxy=[]
        self.get_raw_proxy()

    def run(self):
        pass

    def get_raw_proxy(self):
        RAW_PROXY_RATIO=10      # the ratio of raw and valid proxy
        current_proxy_num=self.proxy_pool.__len__()
        fetch_size=max(0,PROXY_POOL_SIZE-current_proxy_num)*RAW_PROXY_RATIO+1
        url=GET_PROXY_URL.format(NUM=fetch_size)
        try:
            res=request.urlopen(url).read()
            res=str(res,encoding='utf-8')
            self.raw_proxy=res.split('\r\n')
            if self.raw_proxy.__len__()<fetch_size:
                print('*** warning: find_valid_proxy -> get_raw_proxy: '
                      'the proxy num got from web is not enough')
        except Exception as e:
            print('error: find_valid_proxy -> get_raw_proxy: ',e)
            # if can't get proxy ,sleep for 1 sec , then try again
            try:
                time.sleep(1)
                res=request.urlopen(url).read()
                res=str(res,encoding='utf-8')
                self.raw_proxy=res.split('\r\n')
                if self.raw_proxy.__len__()<fetch_size:
                    print('*** warning: find_valid_proxy -> get_raw_proxy: '
                          'the proxy num got from web is not enough')
            except Exception as e:
                print('error: find_valid_proxy -> get_raw_proxy: ',e)
                raise IOError('Unable to get raw proxy from website')





class check_proxy(threading.Thread):
    pass
    #TODO

class proxy_pool():
    def __init__(self):

        self.proxy_list=[]

    def get(self,num):
        pass
        #TODO

    def add(self,data):
        """
        Data Formation: each item be formation of list[[],[],...,[]]
                        [[ip:port(str),timedelay(float)],[ip:port(str),timedelay(float)]]
                        and so on
        """
        pass
        #TODO


if __name__=='__main__':
    proc=Process(target=proxy_manager,args=())
    proc.start()


