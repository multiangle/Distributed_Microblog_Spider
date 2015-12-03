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
        # self.raw_proxy_pool=[]
        # self.raw_proxy_pool_size=10*proxy_pool_size     #一般情况下，rawproxy池大小是proxy池大小的10倍
        global proxy_pool
        proxy_pool=[]
        self.proxy_pool_size=proxy_pool_size
        self.start_up()
        self.run()

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

class find_valide_proxy(threading.Thread):
    """
    function:   Get raw proxy list,check them ,and find valide proxy list
    """
    #TODO
    pass

class check_proxy(threading.Thread):
    pass
    #TODO


if __name__=='__main__':
    pass


