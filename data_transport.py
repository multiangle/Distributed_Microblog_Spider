__author__ = 'multiangle'
import math

class upload_list():
    def __init__(self,data,url,setting={}):
        # data : the formation of data should be a list
        # url :  target url the data should be uploaded
        # setting:  keys:   batch_size
        #                   thread_adjust
        self.target_url=url
        self.data_list=data
        self.list_len=self.data_list.__len__()
        self.seting_check()
        self.pack_data()

    def pack_data(self):
        self.packed_data=[]
        batch_num=math.ceil(self.list_len/self.batch_size)
        for i in range(batch_num):
            temp=self.data_list[i*self.batch_size:min((i+1)*self.batch_size,self.list_len)]
            self.packed_data.append(self.pack_subdata(temp))
        self.data_list=None     # 注释掉data_list,留下 packed_data formation:[{}{}{}]



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

    def pack_subdata(self,main_data):
        data={
            'data':main_data
        }
        return data





    
