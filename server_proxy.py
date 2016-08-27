__author__ = 'multiangle'
"""
    NAME:       server_proxy.py
    PY_VERSION: python3.4
    FUNCTION:   This part is used to get large number of proxy
                from certain http proxy website, then verify if
                they are useful. Useful proxy is saved in cache
                and provided to client to get info from website
    VERSION:    _0.2_

    UPDATE HISTORY:
        _0.2_:  add the partition to monitor the state of proxy pool
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
import random

# import from this folder
from server_config import GET_PROXY_URL,PROXY_POOL_SIZE     #about proxy
from server_config import VERIFY_PROXY_THREAD_NUM,MAX_VALID_PROXY_THREAD_NUM
import server_config
import File_Interface as FI
from DB_Interface import MySQL_Interface
#=======================================================================

class proxy_manager(threading.Thread):

    def __init__(self,proxy_pool,proxy_lock,proxy_pool_size=PROXY_POOL_SIZE):
        threading.Thread.__init__(self)
        self.proxy_pool=proxy_pool
        self.proxy_pool_size=proxy_pool_size
        self.proxy_lock=proxy_lock
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
        """
        thread_pool=[]
        run_value=[int(self.proxy_pool_size/1),int(self.proxy_pool_size/2)]
        MAX_VALID_PROXY_THREAD_NUM=2                        # maximum num of thread of find valid proxy
        for i in range(MAX_VALID_PROXY_THREAD_NUM):        # initialize of thread pool
            temp_t=find_valid_proxy(self.proxy_pool,self.proxy_lock)
            thread_pool.append(temp_t)

        if run_value.__len__()!=MAX_VALID_PROXY_THREAD_NUM: # check data formation
            raise ValueError('the length of run_value is not equal to '
                             'MAX_VALID_PROXY_THREAD_NUM')

        maintain_proxy_thread=keep_proxy_valid(self.proxy_pool)
        maintain_proxy_thread.start()

        state_persistance_thread=state_persistance(self.proxy_pool)
        state_persistance_thread.start()

        while (True):
            time.sleep(0.1)
            for i in range(thread_pool.__len__()):
                if not thread_pool[i].is_alive():
                    if self.proxy_pool.size()<=run_value[i]:
                        thread_pool[i]=find_valid_proxy(self.proxy_pool,self.proxy_lock)
                        thread_pool[i].start()
            if not maintain_proxy_thread.is_alive():
                maintain_proxy_thread=keep_proxy_valid(self.proxy_pool)
                maintain_proxy_thread.start()
            if not state_persistance_thread.is_alive():
                state_persistance_thread=state_persistance(self.proxy_pool)
                state_persistance_thread.start()

class state_persistance(threading.Thread):
    """
    function: monitor and note the state of proxy pool,including the current
    size of proxy pool, the input speed of new proxy , and the output speed.
    and manage the average size oj of proxy_pool class
    """
    def __init__(self,proxy_pool):
        threading.Thread.__init__(self)
        self.proxy_pool=proxy_pool
        self.dbi=MySQL_Interface()

    def run(self):
        while True:
            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            current_size=self.proxy_pool.size()
            [input,output]=self.proxy_pool.update_proxy_state()
            insert_value=[[current_size,time_stick,input,output]]
            self.dbi.insert_asList('proxy_table',insert_value,unique=True)
            time.sleep(server_config.PROXY_MONITOR_GAP)

class find_valid_proxy(threading.Thread):
    """
    function:   Get raw proxy list,check them ,and find valide proxy list
    """
    def __init__(self,proxy_pool,proxy_lock):
        threading.Thread.__init__(self)
        self.proxy_pool=proxy_pool      #proxy pool
        self.proxy_lock=proxy_lock
        self.raw_proxy=[]
        self.raw_proxy_lock=threading.Lock()

    def run(self):
        self.get_raw_proxy()
        self.threads=[]
        for i in range(VERIFY_PROXY_THREAD_NUM):
            t=check_proxy(self.raw_proxy,self.proxy_pool,self.raw_proxy_lock,self.proxy_lock)
            self.threads.append(t)
        for t in self.threads:
            t.start()

    def get_raw_proxy(self):
        RAW_PROXY_RATIO=5      # the ratio of raw and valid proxy
        current_proxy_num=self.proxy_pool.size()
        fetch_size=max(0,PROXY_POOL_SIZE-current_proxy_num)*RAW_PROXY_RATIO+1
        url=GET_PROXY_URL.format(NUM=fetch_size)
        try:
            time.sleep(random.randint(2,2*MAX_VALID_PROXY_THREAD_NUM))
            res=request.urlopen(url)
            res=res.read()
            res=str(res,encoding='utf-8')
            self.raw_proxy=res.split('\r\n')
            if self.raw_proxy.__len__()<fetch_size:
                # model = '*** warning: find_valid_proxy -> get_raw_proxy: '\
                #         'the proxy num got from web is not enough \n '\
                #         'the wanted size is {want_size}, the gotten size is {gotten_size}'
                model = '*** warning: find_valid_proxy -> get_raw_proxy: ' \
                        'the wanted size is {want_size}, the gotten size is {gotten_size}'
                print(model.format(want_size=fetch_size,
                                   gotten_size=str(self.raw_proxy.__len__())))

            else:
                print('get {num} proxy from website'.format(num=fetch_size))
        except Exception as e:
            print('error: find_valid_proxy -> get_raw_proxy: ',e)
            # if can't get proxy ,sleep for 1 sec , then try again
            try:
                time.sleep(random.randint(2,2*MAX_VALID_PROXY_THREAD_NUM))
                res=request.urlopen(url).read()
                res=str(res,encoding='utf-8')
                self.raw_proxy=res.split('\r\n')
                if self.raw_proxy.__len__()<fetch_size:
                    print('*** warning: find_valid_proxy -> get_raw_proxy: '
                          'the proxy num got from web is not enough \n '
                          'the wanted size is {want_size}, the gotten size is {gotten_size}'
                          .format(want_size=fetch_size,gotten_size=str(self.raw_proxy.__len__())))
            except Exception as e:
                print('error: find_valid_proxy -> get_raw_proxy: ',e)
                # raise IOError('Unable to get raw proxy from website')
                print('Unable to get raw proxy from website')

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
            try:
                current_raw_proxy=self.raw_proxy.pop(0)
            except:
                break
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
                                'AppleWebKit/600.1.3 (KHTML, like Gecko) '
                                'Version/8.0 Mobile/12A4345d Safari/600.1.4'}
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

        self.ave_proxy_size=0   # used to monitor the state of proxy pool
        self.proxy_size_list=[]
        self.input_speed=0
        self.output_speed=0

    def add(self,data):
        """
        Data Formation: each item be formation of list[[],[],...,[]]
                        [[ip:port(str),timedelay(float)],[ip:port(str),timedelay(float)]]
                        and so on
        """
        self.proxy=data+self.proxy
        if isinstance(data,list) and data.__len__()>0:
            self.input_speed+=data.__len__()

    def insert(self,single_data):
        """
        Data Formation: each item be formation of
                [ip:port(str),timedelay(float)] and so on
        """
        self.proxy.insert(0,single_data)
        self.input_speed+=1

    def sort(self):         # sort according to the timedelay
        pass
        #TODO

    def empty(self):        #清空proxy列表
        self.output_speed+=self.proxy.__len__()
        self.proxy=[]

    def get(self,num):      # return [[]...[]]
        if self.proxy.__len__()==0:
            return []
        if self.proxy.__len__()<num:
            num=self.proxy.__len__()
        res=[x for x in self.proxy[0:num]]
        self.proxy=self.proxy[num:]
        self.output_speed+=num
        return res

    def pop(self):
        if self.proxy.__len__()==0:
            return []
        self.output_speed+=1
        return self.proxy.pop()

    def size(self):

        return self.proxy.__len__()

    def get_ave_proxy_size(self):
        return self.ave_proxy_size

    def update_proxy_state(self):      # return the value of in&output speed
        if self.proxy_size_list.__len__()>server_config.PROXY_SIZE_STATE_LIST_LEN:
            self.proxy_size_list.pop()
            self.proxy_size_list.insert(0,self.proxy.__len__())
        else:
            self.proxy_size_list.insert(0,self.proxy.__len__())
        self.ave_proxy_size=int(sum(self.proxy_size_list)/self.proxy_size_list.__len__())

        a=self.input_speed              # and reset these two values as 0
        b=self.output_speed
        self.input_speed=0
        self.output_speed=0
        return [a,b]

class keep_proxy_valid(threading.Thread):
    def __init__(self,proxy_pool):
        threading.Thread.__init__(self)
        self.proxy_pool=proxy_pool

    def run(self):
        while True:
            if self.proxy_pool.size()==0:
                time.sleep(0.5)
                continue
            try:
                c_proxy=self.proxy_pool.pop()[0]
            except:
                time.sleep(0.5)
                continue
            # url='http://m.sina.cn/'
            url='http://m.weibo.cn/page/tpl?containerid=1005051221171697_-_FOLLOWERS&page=3'
            handler=request.ProxyHandler({'http':'http://%s'%(c_proxy)})
            t_start=time.time()
            try:
                page=self.getData(url,handler,timeout=5)
                page=re.findall(r'"card_group":.+?]}]',page)[0]
                page='{'+page[:page.__len__()-1]
                page=json.loads(page)
                temp_list=[self.card_group_item_parse(x) for x in page['card_group']]
                usetime=time.time()-t_start
                self.proxy_pool.insert([c_proxy,usetime])
                # print('proxy {proxy} is valid, insert it'.format(proxy=c_proxy))
            except Exception as e:
                pass
                # print(e)
                # print('proxy {proxy} is invalid ,drop it'.format(proxy=c_proxy))

    def getData(self,url,handler,timeout=10):
        headers= {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 8_0 like Mac OS X) '
                                'AppleWebKit/600.1.3 (KHTML, like Gecko) '
                                'Version/8.0 Mobile/12A4345d Safari/600.1.4'}
        req=request.Request(url,headers=headers)
        opener=request.build_opener(handler)
        page=opener.open(req,timeout=timeout)
        return page.read().decode('utf-8')

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

def proxy_info_print(str_info,type='NORMAL'):     # decide if normal of key infomation should be print
    from server_config import PROXY_NORMAL_INFO_PRINT
    if type=='NORMAL':
        if PROXY_NORMAL_INFO_PRINT:
            print(str_info)

if __name__=='__main__':
    proxy_lock=threading.Lock()
    proxy=proxy_pool()
    t=proxy_manager(proxy,proxy_lock)
    t.start()
    while True:
        time.sleep(0.1)
        print(proxy.size())



