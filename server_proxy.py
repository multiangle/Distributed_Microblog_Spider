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
import re,json
import time
import threading
import urllib.request as request

# import from this folder
from server_config import GET_PROXY_URL,PROXY_POOL_SIZE,PROXY_PATH     #about proxy
from server_config import VERIFY_PROXY_THREAD_NUM
import File_Interface as FI
#=======================================================================

class proxy_manager(threading.Thread):

    def __init__(self,proxy_pool,proxy_pool_size=PROXY_POOL_SIZE):
        threading.Thread.__init__(self)
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
                    Receive and transmit signal of event between threads processes.b
                    send valid proxy to client terminal
        """
        #TODO
        pass


class find_valid_proxy(threading.Thread):
    """
    function:   Get raw proxy list,check them ,and find valide proxy list
    """
    #TODO
    def __init__(self,proxy_pool,proxy_lock):
        threading.Thread.__init__(self)
        self.proxy_pool=proxy_pool      #proxy pool
        self.proxy_lock=proxy_lock
        self.raw_proxy=[]
        self.get_raw_proxy()
        self.raw_proxy_lock=threading.Lock()

    def run(self):
        self.threads=[]
        for i in range(VERIFY_PROXY_THREAD_NUM):
            t=check_proxy(self.raw_proxy,self.proxy_pool,self.raw_proxy_lock,self.proxy_lock)
            self.threads.append(t)
        for t in self.threads:
            t.start()

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
    def __init__(self,raw_proxy,proxy_pool,raw_proxy_lock,proxy_lock):
        threading.Thread.__init__(self)
        self.raw_proxy=raw_proxy
        self.proxy_pool=proxy_pool
        self.raw_proxy_lock=raw_proxy_lock
        self.proxy_lock=proxy_lock
    def run(self):
        while(True):
            if not self.raw_proxy:      # if raw_proxy is empty ,end this threading
                break
            self.raw_proxy_lock.acquire()
            current_raw_proxy=self.raw_proxy.pop(0)
            self.raw_proxy_lock.release()

            handler=request.ProxyHandler({'http':'http://%s'%(current_raw_proxy)})
            self.opener=request.build_opener(handler)
            testurl='http://m.weibo.cn/page/tpl?containerid=1005051221171697_-_FOLLOWERS&page=3'
            t1=time.time()
            try:
                page=self.getData(testurl,timeout=10)
                page=re.findall(r'"card_group":.+?]}]',page)[0]
                page='{'+page[:page.__len__()-1]
                page=json.loads(page)
                temp_list=[self.card_group_item_parse(x) for x in page['card_group']]
                usetime=time.time()-t1
                self.proxy_lock.acquire()
                self.proxy_pool.add([[current_raw_proxy,usetime]])
                self.proxy_lock.release()
            except Exception as e:
                pass

    def getData(self,url,timeout=10):
        headers= {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 8_0 like Mac OS X) '
                                'AppleWebKit/600.1.3 (KHTML, like Gecko) Version/8.0 Mobile/12A4345d Safari/600.1.4'}
        req=request.Request(url,headers=headers)
        result=self.opener.open(req,timeout=timeout)
        return result.read().decode('utf-8')
    def card_group_item_parse(self,sub_block):
        """
        :param user_block   : json type
        :return:  user      : dict type
        """
        user_block=sub_block['user']
        user_block_keys=user_block.keys()
        user={}

        if 'profile_url' in user_block_keys:
            user['basic_page']=user_block['profile_url']

        if 'screen_name' in user_block_keys:
            user['name']=user_block['screen_name']

        if 'desc2' in user_block_keys:
            user['recent_update_time']=user_block['desc2']

        if 'desc1' in user_block_keys:
            user['recent_update_content']=user_block['desc1']

        if 'gender' in user_block_keys:
            user['gender']=('male' if user_block['gender']=='m' else 'female')

        if 'verified_reason' in user_block_keys:
            user['verified_reason']=user_block['verified_reason']

        if 'profile_image_url' in user_block_keys:
            user['profile']=user_block['profile_image_url']

        if 'statuses_count' in user_block_keys:
            temp=user_block['statuses_count']
            if isinstance(temp,str):
                temp=int(temp.replace('万','0000'))
            user['blog_num']=temp

        if 'description' in user_block_keys:
            user['description']=user_block['description']

        if 'follow_me' in user_block_keys:
            user['follow_me']=user_block['follow_me']

        if 'id' in user_block_keys:
            user['uid']=user_block['id']

        if 'fansNum' in user_block_keys:
            temp=user_block['fansNum']
            if isinstance(temp,str):
                temp=int(temp.replace('万','0000'))
            user['fans_num']=temp

        return user

class proxy_pool():
    """
    Core Data:      proxy_pool ,formation as [[],[],[]]
    Method:         get(num)
                    add(data)
    """
    def __init__(self):
        self.proxy=[]

    def get(self,num):
        if self.proxy.__len__()==0:
            return []
        if self.proxy.__len__()<num:
            num=self.proxy.__len__()
        res=[x[0] for x in self.proxy[0:num]]
        self.proxy=self.proxy[num:]
        return res

    def add(self,data):
        """
        Data Formation: each item be formation of list[[],[],...,[]]
                        [[ip:port(str),timedelay(float)],[ip:port(str),timedelay(float)]]
                        and so on
        """
        self.proxy=self.proxy+data

    def size(self):
        return self.proxy.__len__()

    def sort(self):         # sort according to the timedelay
        pass
        #TODO

if __name__=='__main__':
    pass


