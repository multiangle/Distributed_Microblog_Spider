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

class clientAsy():
    def __init__(self, uuid=None):
        self.pm = PrintManager()
        self.task_uid = uuid
        self.task_type = None
        check_server()
        self.get_task()
        self.proxy_pool=[]
        self.get_proxy_pool(self.proxy_pool,config.PROXY_POOL_SIZE)
        print(self.pm.gen_block_with_time("SUCCESS TO GET PROXY\nTHE SIZE IS {x}\nSTART TO RUN PROGRAM"
                                          .format(x=self.proxy_pool.__len__())))
        self.run()

    def get_task(self):

        """
        get task user id from server
        """
        print(self.pm.gen_block_with_time("MISSION START\nREADY TO GET TASK"))
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
            info_manager(self.pm.gen_block_with_time("UNABLE TO GET TASK\nSLEEP FOR 1 MIN AND EXIT"),
                         type="KEY",with_time=False)
            time.sleep(60)
            os._exit(0)


        try:        # try to parse task str
            res=res.split(',')
            self.task_uid=res[0]
            self.task_type=res[1]
        except:
            info_manager(self.pm.gen_block_with_time("UNABLE TO GET TASK\nSLEEP FOR 1 MIN AND EXIT"),
                         type="KEY",with_time=False)
            os._exit(0)

        info_manager(self.pm.gen_block_with_time("GOTTEN TASK\nREADY TO GET PROXY"),
                     type='KEY',with_time=False)

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

        proxy_thread = proxy_keep_thread(self.proxy_pool)
        proxy_thread.start()

        if self.task_type=='update':
            asyupdate = AsyUpdateHistory(self.proxy_pool,self.task_uid)
            asyupdate.run()

class proxy_keep_thread(threading.Thread):
    def __init__(self,proxy_pool):
        self.proxy_pool = proxy_pool
        threading.Thread.__init__(self)

    def run(self):
        inner_count = 0
        while True:
            inner_count += 1
            time.sleep(0.1)

            if inner_count==20:
                # print('proxy_keep_thread.run: ths size of pool is {x}'
                #       .format(x=self.proxy_pool.__len__()))
                inner_count = 0
            if self.proxy_pool.__len__()<int(config.PROXY_POOL_SIZE*2/3):
                info_manager('proxy_keep_thread.run: request proxy from server')
                self.get_proxy_pool(self.proxy_pool,config.PROXY_POOL_SIZE)

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

class AsyUpdateHistory():
    def __init__(self,proxy_pool,task):
        self.task = task
        self.proxy_pool = proxy_pool
        self.finished_user = []
        self.exec_res = self.exec_status()
        self.pm = PrintManager()

    def run(self):
        # ori_task_list是一个数组，里面每个元素格式：1005051003716184-1457818009-1446862845
        ori_task_list=self.task.split(';')
        self.mission_id=ori_task_list[-1]
        ori_task_list=ori_task_list[0:-1]
        info_manager(self.pm.gen_block_with_time("START AsyUpdateHistory\n"
                                                 "The mission id:{i}".format(i=self.mission_id)),
                     type='KEY',with_time=False)
        def trans_task_dict(data):
            ret = {}
            tmp = data.split('-')
            # 在下述字段中， reconn_limit表示单个代理下最多重连次数。
            ret['container_id']     = tmp[0]        # container id
            ret['update_time']      = tmp[1]        # update time
            ret['latest_blog']      = tmp[2]        # latest blog
            ret['reconn_limit']     = 2             # 最多重连次数(在使用一个ip情况下)
            ret['proxy_limit']      = 3             # 最多更换的proxy数目
            ret['retry_left']       = 1  # 最多重新尝试次数
            return ret
        task_dict_list = [trans_task_dict(x) for x in ori_task_list]
        if config.NOMAL_INFO_PRINT:
            cids = [x['container_id'] for x in task_dict_list]
            print(cids)
            self.exec_res.set_container_ids(cids)

        # 对获取网页结果的监督进程，可以定时报告任务进度
        self.exec_res.set_total_user_num(task_dict_list.__len__())
        exec_super_msgqueue = []
        exec_supervisor_thread = self.exec_supervisor(self.exec_res, self.pm, exec_super_msgqueue)
        exec_supervisor_thread.start()

        # 打包分发任务，获取页面数据
        random.shuffle(task_dict_list)
        ret_content = []
        page_undealed = []

        info_manager(self.pm.gen_block_with_time("TASK ASSIGNED COMPLETE\n"
                                                 "START TO HANDLE USER TASK"),
                     type='KEY',with_time=False)
        t0 = int(time.time())
        loop = asyncio.get_event_loop()
        user_tasks = [self.asyUpdateHistory_user(x,ret_content,page_undealed,timeout=10)
                     for x in task_dict_list]
        loop.run_until_complete(asyncio.wait(user_tasks))

        t1 = int(time.time())
        info_manager(self.pm.gen_block_with_time("TASK OF SINGLE USER IS FINISHED\n"
                                                 "READY TO HANDLE FAILED TASKS\n"
                                                 "USE {t} SECONDS".format(t=t1-t0)),
                     type='KEY',with_time=False)

        exec_super_msgqueue.append('hehe')

        undealed_tasks = [self.asyUpdateHistory_undealed(x,ret_content,timeout=10)
                          for x in page_undealed]
        loop.run_until_complete(asyncio.wait(undealed_tasks))
        loop.close()

        t2 = int(time.time())
        info_manager(self.pm.gen_block_with_time("TASK OF UNDEALED USER IS FINISHED\n"
                                                 "THIS TASK USE {t} SECONDS\n"
                                                 "TOTAL {t2} SECONDS").format(t=t2-t1,t2=t2-t0),
                     type='KEY',with_time=False)

        # 页面获取完毕，开始去除重复元素
        contents = ret_content
        content_unique=[]      # pick out the repeated content
        content_msgid=[]
        for i in range(contents.__len__()):
            if contents[i]['idstr'] not in content_msgid:
                content_msgid.append(contents[i]['idstr'])
                content_unique.append(contents[i])
            else:
                pass

        # transport the data to data server
        start_time=int(time.time())
        url='{url}/history_data'.format(url=config.DATA_SERVER_URL)
        upload=upload_history(content_unique,url,15,10,self.mission_id)
        upload.run()
        end_time=int(time.time())
        time_gap=end_time-start_time
        info_manager(self.pm.gen_block_with_time("Success to upload data\n"
                                                 "of {cid} to data server\n"
                                                 "use {gap} secs"
                                                 .format(cid=self.mission_id,gap=time_gap)),
                     type="KEY",with_time=False)

        updateHistory={
            'mission_id':self.mission_id
        }

        try:
            data=parse.urlencode(updateHistory).encode('utf8')
        except:
            err_str='error:updateHistory->run: ' \
                    'unable to parse update history data'
            info_manager(err_str,type='KEY')
            raise TypeError('Unable to parse update history')

        url='{url}/update_report'.format(url=config.SERVER_URL)
        req=request.Request(url,data)
        opener=request.build_opener()

        try:
            res=opener.open(req,timeout=10)
        except:
            times=0
            while True:
                times+=1
                try:
                    opener=request.build_opener()
                    res=opener.open(req,timeout=10)
                    break
                except:
                    warn_str='warn:updateHistory->run:' \
                             'unable to return history to server ' \
                             'try {num} times'.format(num=times)
                    info_manager(warn_str,type='KEY')
                if times>50:
                    print('ERROR:unable to convey success info to server')
                    os._exit(0)

        res=res.read().decode('utf8')
        if 'success' in res:
            suc_str='Success:updateHistory->run:\n'\
                    'Success to return update to server'
            info_manager(self.pm.gen_block_with_time(suc_str),type="KEY",with_time=False)
        else:
            string='warn: updateHistory->run: \n' \
                   'get user update, \n' \
                   'but report was denied by server'
            info_manager(self.pm.gen_block_with_time(string),type='KEY',with_time=False)

        self.return_proxy()
        os._exit(0)

    def return_proxy(self):

        """
        return useful or unused proxy to server
        """

        # check_server()
        url='{url}/proxy_return'.format(url=config.SERVER_URL)
        proxy_ret= [x.raw_data for x in self.proxy_pool]
        proxy_str=''

        for item in proxy_ret:
            proxy_str=proxy_str+item+';'
        proxy_str=proxy_str[0:-1]
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



    @asyncio.coroutine
    async def asyUpdateHistory_user(self, task_dict, ret_content, page_undealed_list, timeout=10):

        # 初始化变量
        container_id    = task_dict['container_id']
        update_time     = task_dict['update_time']
        latest_blog     = task_dict['latest_blog']
        reconn_limit    = task_dict['reconn_limit']
        proxy_limit     = task_dict['proxy_limit']
        retry_left      = task_dict['retry_left']

        self.url_model='http://m.weibo.cn/page/json?containerid={cid}_-_WEIBO_SECOND_PROFILE_WEIBO&page={page}'
        aconn = AsyConnector(self.proxy_pool)

        page = 1
        # this func exec the normal seq, and there will be another func to deal with unsuccess page
        continue_err_page_count = 0
        batch = 1 # 每批获取页面的个数

        ave_time_per_page = None
        while True:
            if continue_err_page_count>5:
                print('warning: the continue_err_page_count come up to 5, up to {p}, finish {c} task'
                      .format(c=container_id,p=page))
                self.exec_res.add_user_finish(container_id)
                break
            # for i in range(batch):
            try:
                url = self.url_model.format(cid=container_id,page=page)
                if self.exec_res.unfinished_size()<10:
                    # print(self.exec_res.report_unfinished_tasks())
                    print('user execution report: '+self.exec_res.get_action_times(container_id))

                self.exec_res.add_user_action(container_id)         # 对运行结果进行监控
                self.exec_res.add_page_action(container_id,page)
                time_start = time.time()

                res = await self.getPageContent(url,proxy_limit,reconn_limit,timeout=timeout)
                continue_err_page_count = 0

                self.exec_res.add_page_success(container_id,page)   # 对运行结果进行监控
                time_end = time.time()
                self.exec_res.add_exec_time(time_end-time_start)

                if page>=5 and not ave_time_per_page:       # 经过前5页以后，开始估计要多少同步获取才能够在5页内完成
                    target_gaps =  min((time.time() - latest_blog + 86400*10), 86400*80)
                    current_gap = time.time()-res[-1]['created_timestamp']
                    ave_gap_per_page = current_gap/page
                    target_gaps -= current_gap
                    if target_gaps>0:
                        tmp = int(target_gaps/(ave_gap_per_page*5))
                        if tmp>1:
                            batch = tmp

                valid_res = self.pick_out_valid_res(res,latest_blog,update_time)
                ret_content += valid_res
                if valid_res.__len__()<res.__len__():
                    self.finished_user.append(container_id)
                    info_str = "Success: user {cid} is done".format(cid=container_id)
                    info_manager(info_str,type="NORMAL")
                    self.exec_res.add_user_success(container_id)
                    break
            except:
                continue_err_page_count += 1
                print('{i} continue_err_page_count : {c}, current page: {p}'
                      .format(c=continue_err_page_count,i=container_id,p=page))
                undealed_task = dict(
                    container_id    = container_id,
                    page_id         = page,
                    update_time     = update_time,
                    latest_blog     = latest_blog,
                    reconn_limit    = reconn_limit,
                    proxy_limit     = proxy_limit,
                    retry_left      = retry_left,
                )
                page_undealed_list.append(undealed_task)

            page += 1

    @asyncio.coroutine
    async def asyUpdateHistory_undealed(self,task,ret_content,timeout=10):
        try:
            page_id = task['page_id']
            container_id = task['container_id']
            url = self.url_model.format(cid=container_id,page=page_id)
            res = await self.getPageContent(url,
                                      task['proxy_limit'],
                                      task['reconn_limit'],
                                      timeout=timeout
                                      )
            valid_res = self.pick_out_valid_res(res,task['latest_blog'],task['update_time'])
            ret_content += valid_res
            print(' UNDEALED: Success {cid}-page {i} is done'.format(cid=container_id,i=page_id))
        except:
            if task['retry_left'] > 0:
                task['retry_left'] -= 1
                await self.asyUpdateHistory_undealed(task,ret_content,timeout=timeout)
            else:
                pass
                print('sorry about that {c} {i}'.format(c=container_id,i=page_id))

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

    def pick_out_valid_res(self,init_res,latest_blog,update_time):
        valid_res = []
        for r in init_res:
            if int(r['created_timestamp'])>int(latest_blog)-60*60*24*10 \
                    and int(r['created_timestamp'])>time.time()-60*60*24*80:  # 追踪到最后一条微博的前10天，或者是最近的80天
            # if int(r['created_timestamp'])>time.time()-60*60*24*80:
                valid_res.append(r)
        return valid_res

    class exec_supervisor(threading.Thread):
        def __init__(self, exec_status, print_manager, msg_queue):
            threading.Thread.__init__(self)
            self.exec_status = exec_status
            self.pm = print_manager
            self.msg_queue = msg_queue

        def run(self):
            while self.msg_queue.__len__()==0:
                time.sleep(10)
                if self.msg_queue.__len__()==0:
                    info_manager(self.pm.gen_block_with_time(self.exec_status.anz_res()),
                                 type="KEY",with_time=False)
                # if self.exec_status.unfinished_size()<5:
                #     print(self.exec_status.report_unfinished_tasks())


    class exec_status():
        def __init__(self):
            self._mission_start_time        = time.time()
            self._mission_end_time          = None
            self._total_user_num            = None
            self._action_user_set           = {}
            self._finished_user_set         = {}
            self._success_user_set          = {}
            self._action_page_set           = {}
            self._success_page_set          = {}
            self._exec_time_list            = []

            self._action_user_count         = 0
            self._finished_user_count       = 0
            self._success_user_count        = 0
            self._action_page_count         = 0
            self._success_page_count        = 0

            self._container_ids             = []
            self._unfinished_ids            = []

        def set_total_user_num(self,total_user_num):
            self._total_user_num = total_user_num

        def set_container_ids(self,ids):
            self._container_ids[:] = ids[:]
            self._unfinished_ids[:] = ids[:]

        def add_user_action(self, container_id):
            gotten = self._action_user_set.get(container_id,0)
            if gotten==0:
                self._action_user_count += 1
            self._action_user_set[container_id] = gotten + 1

        def add_user_success(self, container_id):
            gotten = self._success_user_set.get(container_id,0)
            if gotten==0:
                self._success_user_count += 1
            self._success_user_set[container_id] = gotten + 1
            self.add_user_finish(container_id)

        def add_user_finish(self, container_id):
            gotten = self._finished_user_set.get(container_id,0)
            if gotten==0:
                self._finished_user_count += 1
            self._finished_user_set[container_id] = gotten + 1
            self._unfinished_ids.pop(self._unfinished_ids.index(container_id))

        def add_page_action(self, container_id, page_id):
            key = '{c}-{p}'.format(c=container_id, p=page_id)
            gotten = self._action_page_set.get(key,0)
            if gotten==0:
                self._action_page_count += 1
            self._action_page_set[key] = gotten + 1

        def add_page_success(self, container_id, page_id):
            key = '{c}-{p}'.format(c=container_id, p=page_id)
            gotten = self._success_page_set.get(key,0)
            if gotten==0:
                self._success_page_count += 1
            self._success_page_set[key] = gotten + 1

        def add_exec_time(self,time_sec):
            self._exec_time_list.append(time_sec)

        def anz_res(self):
            ret = ""
            ret += "The user status is {a} / {b} / {c}\n".format(
                a = self._success_user_count,
                b = self._finished_user_count,
                c = self._total_user_num
            )

            bar_size = 30
            valid_size = int((self._finished_user_count/self._total_user_num)*bar_size)
            valid_size = valid_size if valid_size<=bar_size else bar_size
            invalid_size = bar_size-valid_size
            tmpstr = '◆'*valid_size + '-'*invalid_size
            ret += tmpstr + '\n'

            ret += "user success ratio: {p}\n".format(
                p = self._success_user_count/self._finished_user_count if self._finished_user_count>0 else 0)
            ret += "The page status is {a} / {b}\n".format(
                a = self._success_page_count,
                b = self._action_page_count
            )
            ret += "page success ratio: {p}\n".format(
                p = str(self._success_page_count/self._action_page_count)[:5]
            )
            ret += "this task lasted for {t} secs".format(
                t = int (time.time() - self._mission_start_time))
            return ret

        def report_unfinished_tasks(self):
            ret = ''
            for id in self._unfinished_ids:
                tmp = 'id:{i}\taction pages:{p}'.format(i=id,p=self._action_user_set[id])
                ret += tmp + '\n'
            return ret[:-1]

        def unfinished_size(self):
            return self._total_user_num-self._finished_user_count

        def get_action_times(self,container_id):
            return self._action_user_set[container_id]

        def tmp(self):
            return self._action_user_set

class AsyConnector():
    def __init__(self, proxy_pool, if_proxy=True):
        self.proxy_pool = proxy_pool
        self.if_proxy   = if_proxy

    @asyncio.coroutine
    async def getPage(self, url, proxy_limit, reconn_limit,
                      proxy_used=0, timeout=10):
        while True:
            if self.proxy_pool.__len__()>0:
                proxy = 'http://'+self.proxy_pool.pop(0).url
                break
            else:
                info_manager("AsyConnector.getPage.getproxy->"
                             "unable to get proxy, sleep for 3 sec",type="NORMAL")
                await asyncio.sleep(3)
        try:
            ret_data = await self.__single_connect(url,
                                                   proxy,
                                                   reconn_limit,
                                                   timeout=timeout)
            reconn_times    = ret_data['reconn_times']
            page            = ret_data['content']
            print("success to get page {u} \n\t\tafter try {p} proxies and {r} reconn"
                  .format(u=url,p=proxy_used,r=reconn_times))
            return page
        except Exception as e:
            print("Error from AsyConnector.getPage {u}\n\t\treason:{e}"
                  .format(e=e,u=url))
            if proxy_used < proxy_limit:
                print('\t\tthis proxy seems invalid, ready to change one, '
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
                # print("success to get page after reconn {t} times".format(t=reconn_times))
                ret_data = dict(
                    content = content,
                    reconn_times = reconn_times
                )
                return ret_data
            except Exception as e:
                print("Error from AsyConnector.__single_connect: \n\t\treason :{x}".format(x=e))
                if reconn_times < reconn_limit:
                    print("\t\treconn again, the {i} times".format(i=reconn_times+1))
                    return await self.__single_connect(url, proxy, reconn_limit,
                                                       reconn_times+1, timeout=timeout)
                else:
                    raise RuntimeError("** warning: can not get page, ready to change proxy and retry")

def info_manager(info_str,type='NORMAL',with_time=True):
    time_stick=time.strftime('%Y/%m/%d %H:%M:%S ||',
                             time.localtime(time.time()))
    if with_time:
        str=time_stick+info_str
    else:
        str = info_str
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

def generate_timestr():
    tstr = time.strftime('%Y/%m/%d %H:%M:%S',time.localtime(time.time()))
    return tstr

class PrintManager():
    def gen_timestr(self):
        tstr = time.strftime('%Y/%m/%d %H:%M:%S',time.localtime(time.time()))
        return tstr

    def gen_center_str(self, content, len=42, frame="|||"):
        if type(content)==str:
            content = content.split("\n")
            # content = [content]
        ret = ""
        for s in content:
            left = len-frame.__len__()*2-s.__len__()
            margin_left = left>>1
            margin_right = left-margin_left
            line = "{fr}{ml}{s}{mr}{fr}".format(
                ml = " "*margin_left,
                s = s,
                mr = " "*margin_right,
                fr = frame
            )
            ret += line+'\n'
        return ret

    def gen_block(self, content, len=42, frame="|||"):
        ret = "="*len + '\n'
        ret += self.gen_center_str(content,len,frame=frame)
        ret += "="*len + '\n'
        return ret


    def gen_block_with_time(self, content, len=42, frame="|||"):
        ret = "="*len+'\n'
        time_s = self.gen_timestr()
        timeline = "TIME: "+time_s
        ret += self.gen_center_str(timeline,len,frame=frame)
        return ret+self.gen_block(content,len,frame=frame)



class upload_history(upload_list):
    def __init__(self,data,url,pack_len,thread_num,container_id):
        self.container_id=container_id
        setting=dict(
            batch_size=pack_len,
            thread_adjust=False,
            thread_num=thread_num
        )
        upload_list.__init__(self,data,url,setting)

    def pack_block(self,main_data,pack_id,pack_num):
        data={
            'data':          main_data,
            'current_id':   pack_id,
            'total_num':    pack_num,
            'len':           main_data.__len__(),
            'container_id':self.container_id
        }
        data=parse.urlencode(data).encode('utf8')
        return data

if __name__=='__main__':
    p_pool = []
    uuid = 100
    for i in range(1):
        p = Process(target=clientAsy,args=(uuid,))
        p_pool.append(p)
    for p in p_pool:
        p.start()
    # while True:
    #     for i in range(p_pool.__len__()):
    #         if not p_pool[i].is_alive():
    #             p_pool[i] = Process(target=clientAsy,args=(uuid,))
    #             x=random.randint(1,10)
    #             time.sleep(x)
    #             p_pool[i].start()

