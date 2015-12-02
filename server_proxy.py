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

# import from this folder
from server_config import GET_PROXY_URL,PROXY_POOL_SIZE
#=======================================================================

class proxy_manager():
    def __init__(self,proxy_pool_size=PROXY_POOL_SIZE):
        self.proxy_pool=[]
        self.proxy_pool_size=proxy_pool_size

    def get_proxy(self):
        pass

class check_proxy(threading.Thread):
    pass


if __name__=='__main__':
    s=proxy_manager()

