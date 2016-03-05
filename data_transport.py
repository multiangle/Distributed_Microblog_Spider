__author__ = 'multiangle'
import math
import threading
import urllib.parse as parse
import urllib.request as request
import time

# todo 线程调节那块需要补充 ， 当数据小时，自动调节线程，避免线程空转

class upload_list():
    def __init__(self,data,url,setting):
        # data : the formation of data should be a list
        # url :  target url the data should be uploaded
        # setting:  keys:   batch_size
        #                   thread_adjust
        #                   thread_num
        self.url=url
        self.data_list=data
        self.list_len=self.data_list.__len__()

        self.batch_size=1
        self.thread_adjust=True
        self.thread_num=10
        self.setting=setting
        self.seting_check()

        self.task_list=[]
        self.task_num=0
        self.build_task_list()

    def run(self):
        stat_ret=[]
        if self.thread_adjust:
            alive_id=[]
            thread_pool=[]
            stat_his=[]

            for i in range(self.thread_num):
                t=upload_sub(self.task_list,self.url,i,stat_ret,alive_id)
                thread_pool.append(t)
            for t in thread_pool:
                t.start()

            while self.task_list:
                for i in range(self.thread_num):
                    if not thread_pool[i].is_alive():
                        t=upload_sub(self.task_list,self.url,i,stat_ret,alive_id)
                        thread_pool[i]=t
                        thread_pool[i].start()

                task_left_num=self.task_list.__len__()
                task_done_num=max(self.task_num-task_left_num,0)
                show_block=40
                task_left_show=min(max(int(show_block*task_left_num/(task_left_num+task_done_num)),0),show_block)
                task_done_show=show_block-task_left_show
                print(task_done_show*'★'+task_left_show*'☆')

                time.sleep(1)

            while True:
                all_dead=True
                for t in thread_pool:
                    if t.is_alive():
                        all_dead=False
                if all_dead:
                    break
                time.sleep(1)


        else:
            alive_id=[x for x in range(self.thread_num)]
            thread_pool=[]
            for i in range(self.thread_num):
                t=upload_sub(self.task_list,self.url,i,stat_ret,alive_id)
                thread_pool.append(t)
            for t in thread_pool:
                t.start()

            while self.task_list:
                for i in range(self.thread_num):
                    if not thread_pool[i].is_alive():
                        t=upload_sub(self.task_list,self.url,i,stat_ret,alive_id)
                        thread_pool[i]=t
                        thread_pool[i].start()

                task_left_num=self.task_list.__len__()
                task_done_num=max(self.task_num-task_left_num,0)
                show_block=40
                task_left_show=min(max(int(show_block*task_left_num/(task_left_num+task_done_num)),0),show_block)
                task_done_show=show_block-task_left_show
                print(task_done_show*'◆'+task_left_show*'―')

                time.sleep(1)

            while True:
                all_dead=True
                for t in thread_pool:
                    if t.is_alive():
                        all_dead=False
                if all_dead:
                    break
                time.sleep(1)

    def seting_check(self):
        keys=self.setting.keys()

        if 'batch_size' in keys:
            self.batch_size=self.setting['batch_size']
        else:
            raise ValueError('Unknown data of batch size')

        if 'thread_adjust' in keys:
            self.thread_adjust=self.setting['thread_adjust']
            if not isinstance(self.thread_adjust,bool):
                raise ValueError('The type of thread_adjust should be boolean')
        else:
            self.thread_adjust=True

        if 'thread_num' in keys:
            self.thread_num=self.setting['thread_num']
        else:
            if not self.thread_adjust:
                raise ValueError('thread num should be provided '
                                 'if auto adjust is turn off')

    def build_task_list(self):
        batch_num=math.ceil(self.list_len/self.batch_size)
        for i in range(batch_num):
            ori_block=self.data_list[i*self.batch_size:min((i+1)*self.batch_size,self.list_len)]
            self.task_list.append(self.pack_block(ori_block,i,batch_num))
        self.task_num=self.task_list.__len__()
        self.data_list=None     # 注释掉data_list,留下 task_list formation:[{}{}{}]

    def pack_block(self,main_data,pack_id,pack_num):
        data={
            'data':main_data
        }
        data=parse.urlencode(data).encode('utf8')
        return data

    # def judge_tread_num(self):

class upload_sub(threading.Thread):
    def __init__(self,task_list,url,thread_id,stat_ret,alive_id):
        threading.Thread.__init__(self)
        self.task_list=task_list
        self.url=url
        self.stat_ret=stat_ret
        self.thread_id=thread_id
        self.alive_id=alive_id

    def run(self):
        while self.task_list:
            task=self.task_list.pop(0)

            req=request.Request(self.url,task)
            opener=request.build_opener()

            stat={}
            stat['id']=self.thread_id
            stat['start']=time.time()
            try:
                res=opener.open(req)
                res=res.read().decode('utf8')
                if 'success' in res:
                    stat['result']='success'
                else:
                    stat['result']='denied'
            except:
                self.task_list.append(task)
                stat['result']='timeout'
            stat['end']=time.time()
            stat['gap']=stat['start']-stat['end']
            self.stat_ret.append(stat)

            if self.thread_id not in self.alive_id:
                break









    
