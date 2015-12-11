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
#=======================================================================

#=======================================================================
#------------------code session--------------------------

class client():          # the main process of client
    def __init__(self):

        self.task_uid=None      #任务id
        self.task_type=None     #任务类型
        check_server()     #检查是否能连上服务器
        self.get_task()         #获取任务
        self.proxy_pool=[]
        self.get_proxy_pool(self.proxy_pool,config.PROXY_POOL_SIZE)
        self.run()

    def run(self):      # main code of the process
        # 监控proxy pool,建议get_proxy_pool单独开一个线程，
        # 如果server立即返回则马上关闭，否则设为长连接

        getinfo_thread=getInfo(self.proxy_pool,self.task_uid)
        getinfo_thread.start()
        inner_count=0

        while True:
            inner_count+=1
            time.sleep(0.1)       #每隔0.1秒检查情况

            if inner_count==20:     # print the size of proxy pool
                print('client->run: the size of proxy pool is ',
                      self.proxy_pool.__len__())
                inner_count=0

            proxy_thread=get_proxy_pool_thread(self.proxy_pool,config.PROXY_POOL_SIZE)

            # if not proxy_thread.is_alive():         # maintain proxy pool
            #     if self.proxy_pool.__len__()<int(config.PROXY_POOL_SIZE/2):
            #         err_str='client->run : request proxy from server'
            #         info_manager(err_str)
            #         # proxy_thread=get_proxy_pool_thread(self.proxy_pool,config.PROXY_POOL_SIZE)
            #         proxy_thread.start()

            if self.proxy_pool.__len__()<int(config.PROXY_POOL_SIZE*2/3):
                err_str='client->run : request proxy from server'
                info_manager(err_str)
                self.get_proxy_pool(self.proxy_pool,config.PROXY_POOL_SIZE)

            if not getinfo_thread.is_alive():
                self.return_proxy()
                break
                #TODO 此处有待斟酌，就是关于如何判断执行完线程方面

    def get_task(self):

        """
        get task user id from server
        """

        url='{url}/task'.format(url=config.SERVER_URL)

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
                     'unable to get task, exit process'
            info_manager(err_str,type='KEY')
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
            res=request.urlopen(url,timeout=5).read()
            res=str(res,encoding='utf8')
        except:
            time.sleep(5)
            check_server()     # sleep until server is available
            try:
                res=request.urlopen(url,timeout=5).read()
                res=str(res,encoding='utf8')
            except Exception as e:
                err_str='error: client -> get_proxy_pool : unable to ' \
                        'connect to proxy server '
                info_manager(err_str,type='KEY')
                if config.KEY_INFO_PRINT:
                    print(e)
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
            return
        else:
            err_str='error:client->return_proxy:'+res
            info_manager(err_str,type='KEY')
            # raise ConnectionError('Unable to return proxy')
            return

class get_proxy_pool_thread(threading.Thread):
    def __init__(self,proxy_pool,num):
        threading.Thread.__init__(self)
        self.proxy_pool=proxy_pool
        self.num=num

    def run(self):
        url='{url}/proxy/?num={num}'.format(url=config.SERVER_URL,num=self.num)

        try:
            res=request.urlopen(url,timeout=5).read()
            res=str(res,encoding='utf8')
        except:
            time.sleep(5)
            check_server()     # sleep until server is available
            try:
                res=request.urlopen(url,timeout=5).read()
                res=str(res,encoding='utf8')
            except Exception as e:
                err_str='error: get_proxy_pool_thread -> run : ' \
                        'unable to connect to proxy server '
                info_manager(err_str,type='KEY')
                if config.KEY_INFO_PRINT:
                    print(e)
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

        self.proxy_pool[:]=self.proxy_pool[:]+data

class getInfo(threading.Thread):       # 用来处理第一类任务，获取用户信息和关注列表

    def __init__(self,proxy_pool,uid):
        threading.Thread.__init__(self)
        self.conn=Connector(proxy_pool,if_proxy=False)      #请求用户基本信息的时候会暂时关掉proxy
        self.uid=uid
        self.proxy_pool=proxy_pool

    def run(self):
        time.sleep(max(0.1,random.gauss(1,0.2)))
        self.user_basic_info=self.getBasicInfo()
        self.attends=self.getAttends(self.user_basic_info['container_id'],
                                     self.proxy_pool)
        userInfo={
            'user_basic_info':self.user_basic_info,
            'user_attends':self.attends
        }

        try:
            data=parse.urlencode(userInfo).encode('utf8')
        except:
            err_str='error:getInfo->run: ' \
                    'unable to parse uesr info data'
            info_manager(err_str,type='KEY')
            raise TypeError('Unable to parse user info')

        url='{url}/info_return'.format(url=config.SERVER_URL)
        req=request.Request(url,data)
        opener=request.build_opener()

        try:
            res=opener.open(req)
        except:
            times=0

            while times<5:
                times+=1
                time.sleep(3)
                try:
                    res=opener.open(req)
                    break
                except:
                    warn_str='warn:getInfo->run:' \
                             'unable to return info to server ' \
                             'try {num} times'.format(num=times)
                    info_manager(warn_str,type='NORMAL')
            if times==5:
                FI.save_pickle(self.attends,'data.pkl')
                string='warn: getInfo->run: ' \
                        'get attends list, but unable to connect server,' \
                        'stored in data.pkl'
                info_manager(string,type='KEY')

        res=res.read().decode('utf8')
        if 'success to return user info' in res:
            suc_str='Success:getInfo->run:' \
                    'Success to return user info to server'
            info_manager(suc_str,type='KEY')
        else:
            FI.save_pickle(userInfo,'data.pkl')
            string='warn: getInfo->run: ' \
                   'get attends list, but unable to connect server,' \
                   'stored in data.pkl'
            info_manager(string,type='KEY')

        #TODO 注意是否要将信息分开发送

    def getBasicInfo(self):
        """
        get user's basic information,
        :param uid:
        :return:basic_info(dict)
        """
        homepage_url = 'http://m.weibo.cn/u/' + str(self.uid)

        try:
            homepage_str = self.conn.getData(homepage_url)
        except :
            raise ConnectionError('Unable to get basic info')

        user_basic_info={}
        info_str = re.findall(r'{(.+?)};', homepage_str)[1].replace("'", "\"")
        info_str = '{'+ info_str +'}'
        info_json = json.loads(info_str)

        user_basic_info['container_id'] = info_json['common']['containerid']     #containerid
        info = json.loads(info_str)['stage']['page'][1]
        user_basic_info['uid'] = info['id']                                         #uid
        user_basic_info['name'] = info['name']                                     #name
        user_basic_info['description'] = info['description']                     #description
        user_basic_info['gender'] = ('male' if info['ta'] == '他' else 'female')   #sex
        user_basic_info['verified'] = info['verified']
        user_basic_info['verified_type'] = info['verified_type']
        user_basic_info['native_place'] = info['nativePlace']

        user_basic_info['fans_num'] = info['fansNum']
        if isinstance(info['fansNum'],str):
            temp=info['fansNum'].replace('万','0000')
            temp=int(temp)
            user_basic_info['fans_num']=temp

        user_basic_info['blog_num'] = info['mblogNum']
        if isinstance(info['mblogNum'],str):
            temp=info['mblogNum'].replace('万','0000')
            temp=int(temp)
            user_basic_info['blog_num']=temp

        user_basic_info['attends_num'] = info['attNum']
        if isinstance(info['attNum'],str):
            temp=info['attNum'].replace('万','0000')
            temp=int(temp)
            user_basic_info['attends_num']=temp

        user_basic_info['detail_page']="http://m.weibo.cn/users/"+str(user_basic_info['uid'])
        user_basic_info['basic_page']='http://m.weibo.cn/u/'+str(user_basic_info['uid'])
        print('\n','CURRENT USER INFO ','\n','Name:',user_basic_info['name'],'\t','Fans Num:',user_basic_info['fans_num'],'\t',
              'Attens Num:',user_basic_info['attends_num'],'\t','Blog Num:',user_basic_info['blog_num'],'\n',
              'Atten Page Num:',int(user_basic_info['attends_num']/10),'\n',
              'description:',user_basic_info['description']
        )
        return user_basic_info

    def getAttends(self,container_id,proxy_pool):

        attends_num=self.user_basic_info['attends_num']
        model_url='http://m.weibo.cn/page/tpl?containerid='\
                  +str(container_id)+'_-_FOLLOWERS&page={page}'
        page_num=int(attends_num/10)
        task_url=[model_url.format(page=(i+1)) for i in range(page_num)]
        random.shuffle(task_url)            # 对任务列表进行随机排序
        attends=[]
        threads_pool=[]                     #线程池
        for i in range(config.THREAD_NUM):  # thread initialization
            t=self.getAttends_subThread(task_url,proxy_pool,attends)
            threads_pool.append(t)
        for t in threads_pool:              # thread_list
            t.start()

        while True:
            time.sleep(0.2)
            if task_url :   # 如果 task_url 不为空，则检查是否有进程异常停止
                for i in range(config.THREAD_NUM):
                    if not threads_pool[i].is_alive() :
                        threads_pool[i]=self.getAttends_subThread(task_url,proxy_pool,attends)
                        threads_pool[i].start()
            else:           #如果 task_url 为空，则当所有线程停止时跳出
                all_stoped=True
                for t in threads_pool:
                    if t.is_alive():
                        all_stoped=False
                if all_stoped:
                    break
        attends_unique=[]
        attends_uid=[]
        for i in range(attends.__len__()):
            if attends[i]['uid'] not in attends_uid:
                attends_uid.append(attends[i]['uid'])
                attends_unique.append(attends[i])
            else:
                pass
        return attends_unique

    class getAttends_subThread(threading.Thread):

        def __init__(self,task_url,proxy_pool,attends):
            threading.Thread.__init__(self)
            self.task_url=task_url
            self.conn=Connector(proxy_pool)
            self.attends=attends

        def run(self):
            while True:
                if not self.task_url:
                    break
                url=self.task_url.pop(0)
                time.sleep(max(random.gauss(0.5,0.1),0.05))
                try:
                    page=self.conn.getData(url,
                                           timeout=5,
                                           reconn_num=1,
                                           proxy_num=30)
                except Exception as e:
                    print('error:getAttends_subThread->run: '
                          'fail to get page'+url)
                    print('return this page')
                    self.task_url.append(url)
                    continue
                page='{\"data\":['+page[1:]+'}'
                try:
                    page=json.loads(page)
                    page=page['data'][1]
                    temp_list=[card_group_item_parse(x) for x in page['card_group']]
                    self.attends[:]=self.attends[:]+temp_list
                    info_str='Success: Page {url} is done'.format(url=url)
                    info_manager(info_str,type='NORMAL')
                except Exception as e:
                    try:                #分析是否是因为 “没有内容” 出错，如果是，当前的应对措施是休眠5秒，再次请求。
                        # page=page.replace(' ','')
                        data=json.loads(page)
                        if data['test'][1]['msg']=='没有内容':
                            time.sleep(5)
                            print('--- fail to get valid page, sleep for 5 seconds ---')
                            try:
                                page = self.conn.getData(url,
                                                         timeout=5,
                                                         reconn_num=1,
                                                         proxy_num=30)
                            except Exception as e:
                                print('error:getAttends_subThread->run: '
                                      'fail to get page twice:'+url)
                                print('skip this page')
                                continue
                            try:
                                page='{\"data\":['+page[1:]+'}'
                                page=json.loads(page)
                                page=page['data'][1]
                                # page=re.findall(r'"card_group":.+?]}]',page)[0]
                                # page='{'+page[:page.__len__()-1]
                                # page=json.loads(page)
                                temp_list=[card_group_item_parse(x) for x in page['card_group']]
                                self.attends[:]=self.attends[:]+temp_list
                                info_str='Success: Page {url} is done'.format(url=url)
                                info_manager(info_str,type='KEY')
                            except:
                                print(e)
                                pass    #如果再次失败，当前措施是直接跳过
                    except Exception as e:  #如果不是因为 “没有内容出错” 则出错原因不明，直接跳过
                        if config.KEY_INFO_PRINT: print(e)
                        err_str='error:getAttends_subThread->run:Unknown page type, ' \
                                'fail to parse {url}'.format(url=url)
                        info_manager(err_str,type='NORMAL')
                        if config.NOMAL_INFO_PRINT: print(page)
                        if config.NOMAL_INFO_PRINT: print('--- skip this page ---')
                        pass

def card_group_item_parse(sub_block):

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

def check_server():

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
                error_str='error: client-> check_server :' \
                          'no auth to connect to server,exit process'
                info_manager(error_str,type='KEY')
                os._exit(0)
        except Exception as e:
            err_str='error:client->check_server:cannot ' \
                    'connect to server; process sleeping'
            info_manager(err_str,type='NORMAL')
            time.sleep(5)       # sleep for 1 seconds

class Connector():
    def __init__(self,proxy_pool,if_proxy=True):      #从proxy_pool队列中取出一个
        self.headers = {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 8_0 like Mac OS X) '
                                       'AppleWebKit/600.1.3 (KHTML, like Gecko) Version/8.0 Mobile'
                                       '/12A4345d Safari/600.1.4'}
        self.proxy_pool=proxy_pool
        self.cj=http.cookiejar.CookieJar()
        self.if_proxy=if_proxy

        if if_proxy :
            counting=0

            while True:

                try:
                    self.current_proxy_oj=proxy_pool.pop(0)
                    break
                except:
                    time.sleep(1)
                    counting+=1
                    if counting>60:
                        raise ConnectionError('unable to get proxy')

            self.proxy_handler=request.ProxyHandler({'http':self.current_proxy_oj.url})
            # self.opener=request.build_opener(request.HTTPCookieProcessor(self.cj),self.proxy_handler)
            self.opener=request.build_opener(self.proxy_handler)
        else:
            self.current_proxy_oj=None
            self.opener=request.build_opener(request.HTTPCookieProcessor(self.cj))
        request.install_opener(self.opener)

    def getData(self,url,timeout=10,reconn_num=2,proxy_num=30):
        try:
            res=self.getData_inner(url,timeout=timeout)
            return res
        except Exception as e:
            err_str='warn: Connector->getData : connect fail,ready to connect'
            info_manager(err_str,type='NORMAL')
            if config.NOMAL_INFO_PRINT: print(e)
            proxy_count=1
            while(proxy_count<=proxy_num):
                reconn_count=1
                while(reconn_count<=reconn_num):
                    time.sleep(max(random.gauss(2,0.5),0.5))
                    err_str='warn: Connector->getData:the {num}th reconnect  '.format(num=reconn_count)
                    info_manager(err_str,type='NORMAL')
                    try:
                        res=self.getData_inner(url,timeout=timeout)
                        return res
                    except:
                        reconn_count+=1
                info_manager('warn: Connector->getData:reconnect fail, ready to change proxy',type='NORMAL')
                self.change_proxy()
                err_str='warn: Connector->getData:change proxy for {num} times'.format(num=proxy_count)
                info_manager(err_str,type='NORMAL')
                proxy_count+=1
            raise ConnectionError('run out of reconn and proxy times')

    def getData_inner(self,url,timeout=10):
        # if self.if_proxy:
        #     opener=request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
        #                                 request.ProxyHandler({'http':self.current_proxy_oj.url}))
        # else:
        #     opener=request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()))

        req=request.Request(url,headers=self.headers)
        # request.install_opener(opener)
        result=self.opener.open(req,timeout=timeout)
        # result=opener.open(req,timeout=timeout)
        return result.read().decode('utf-8')

    def change_proxy(self,retry_time=50):
        try:
            self.current_proxy_oj=self.proxy_pool.pop(0)
        except Exception as e:
            print(e)
            re_try=1
            while(re_try<retry_time):
                time.sleep(5)
                try:
                    self.current_proxy_oj=self.proxy_pool.pop(0)
                    break
                except:
                    err_str='warn: Connector->change_proxy:' \
                            're_try fail,ready to try again' \
                            ' size of proxy pool is {num}'\
                        .format(num=self.proxy_pool.size())
                    info_manager(err_str,type='NORMAL')
                    re_try+=1
            if re_try==retry_time: raise ConnectionError('Unable to get proxy from proxy_pool')
        self.cj=http.cookiejar.CookieJar()
        self.proxy_handler=request.ProxyHandler({'http':self.current_proxy_oj.url})
        self.opener=request.build_opener(request.HTTPCookieProcessor(self.cj),
                                         self.proxy_handler)
        request.install_opener(self.opener)

class proxy_object():
    def __init__(self,data):    # in this version ,data is in formation of [str(proxy),int(timedelay)]
        self.raw_data=data
        res=data.split(',')
        self.url=res[0]
        self.timedelay=res[1]
    def getUrl(self):
        return self.url
    def getRawType(self):       #返回原来格式
        return self.raw_data

def info_manager(info_str,type='NORMAL'):
    time_stick=time.strftime('%Y/%m/%d %H:%M:%S ||', time.localtime(time.time()))
    str=time_stick+info_str
    if type=='NORMAL':
        if config.NOMAL_INFO_PRINT:
            print(str)
    if type=='KEY':
        if config.KEY_INFO_PRINT:
            print(str)

if __name__=='__main__':
    p=Process(target=client,args=())
    p.start()
    while True:
        time.sleep(5)
        if not p.is_alive():
            p=Process(target=client,args=())
            p.start()


