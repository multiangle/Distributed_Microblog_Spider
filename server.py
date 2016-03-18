__author__ = 'multiangle'
"""
    NAME:       server.py
    PY_VERSION: python3.4
    FUNCTION:
    This server part of distrubuted microblog spider.
    The function of server can be divided into 3 parts.
    1.  proxy manager.  Scratch web page in high speed need a lot of http proxy ip.
    Server should maintain a proxy pool which should provide proxy to client.
    2.  task manager.   Client will require task from server. task list should fetched
    from sqlserver and stored in memory
    3.  store return info.  When client finished searching user information from sina,
    client will return this info to server. if the length of data is too lang, client
    will seperate it into several parts and send then individually. Server should combine
    these data package together.
        Besides, server should check whether the received user is already exist in database.
    Server has to assure that no repeating data exists in database. It a heavy task for
    server to connect with databases.

    VERSION:
       _0.5_
"""
#======================================================================
#----------------import package--------------------------
# import python package
import threading
import time
import sys
from random import Random

# import from outer package
from pymongo import MongoClient

import tornado.web
import tornado.ioloop
import tornado.options
from tornado.options import define,options

# import from this folder
from server_proxy import proxy_pool,proxy_manager
import server_config as config
from server_database import DB_manager,deal_cache_user_info,deal_cache_attends
import File_Interface as FI
from DB_Interface import MySQL_Interface
from server_data import DataServer
#=======================================================================
define('port',default=8000,help='run on the given port',type=int)

class Application(tornado.web.Application):
    def __init__(self):
        handlers=[
            (r'/auth',AuthHandler),
            (r'/proxy/',ProxyHandler),
            (r'/task/',TaskHandler),
            (r'/proxy_size',ProxySize),
            (r'/proxy_empty',ProxyEmpty),
            (r'/proxy_return',ProxyReturn),
            (r'/info_return',InfoReturn),
            (r'/history_report',HistoryReport),
            (r'/update_report',UpdateReport)
        ]
        settings=dict(
            debug=True
        )
        tornado.web.Application.__init__(self,handlers,**settings)

class AuthHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('connection valid')
        self.finish()

class ProxyHandler(tornado.web.RequestHandler):
    def get(self):
        global proxy
        num=int(self.get_argument('num'))
        if num>proxy.size():
            self.write('no valid proxy')
            self.finish()
        else:
            proxy_list=proxy.get(num)
            proxy_list=['{url},{timedelay};'.format(url=x[0],timedelay=x[1]) for x in proxy_list]
            res=''
            for i in proxy_list: res+=i
            res=res[0:-1]       # 'url,timedelay;url,timedelay;...,'
            self.write(res)
            self.finish()

class TaskHandler(tornado.web.RequestHandler):

    def get(self):
        global proxy
        uuid=str(self.get_argument('uuid'))
        task_id=self.task_assign(uuid)

        if proxy.get_ave_proxy_size()<30:   # check the size of current proxy size
            self.write('no task')
            self.finish()
            return

        if task_id==-1:       # checi if this uuid is valid
            self.write('no task')
            self.finish()
            return

        if task_id==1:         # get the social web of certain user
            dbi=MySQL_Interface()
            query='select * from ready_to_get where is_fetching is null order by fans_num desc limit 1;'
            res=dbi.select_asQuery(query)
            if res.__len__()==0:
                self.write('no task')
                self.finish()
                return
            res=res[0]
            col_info=dbi.get_col_name('ready_to_get')
            uid=res[col_info.index('uid')]

            self.write('{uid},connect'.format(uid=uid))
            self.finish()

            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            query="update ready_to_get set is_fetching=\'{t_time}\' where uid={uid} ;"\
                .format(t_time=time_stick,uid=uid)
            dbi.update_asQuery(query)


        if task_id==2:      # get the history microblog of a certain user
            dbi=MySQL_Interface()
            query='select container_id,blog_num from user_info_table ' \
                  'where (isGettingBlog is null and update_time is null and blog_num<{valve} and blog_num>100)' \
                  'order by fans_num desc limit 1 ;'.format(valve=config.HISTORY_TASK_VALVE)
            # query='select container_id,blog_num from user_info_table ' \
            #       'order by rand() limit 1 ;'
            [container_id,blog_num]=dbi.select_asQuery(query)[0]
            self.write('{c_id};{blog},history'
                       .format(c_id=container_id,blog=blog_num))
            self.finish()
            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            query="update user_info_table set isGettingBlog=\'{t_time}\' where container_id={cid} ;"\
                .format(t_time=time_stick,cid=container_id)
            dbi.update_asQuery(query)

        if task_id==3:      # get the history microblog of a certain user
            dbi=MySQL_Interface()
            query='select container_id,blog_num from user_info_table ' \
                  'where (isGettingBlog is null and update_time is null and blog_num>={valve}  and blog_num>100)' \
                  'order by fans_num desc limit 1 ;'.format(valve=config.HISTORY_TASK_VALVE)
            # query='select container_id,blog_num from user_info_table ' \
            #       'order by rand() limit 1 ;'
            [container_id,blog_num]=dbi.select_asQuery(query)[0]
            self.write('{c_id};{blog},history'
                       .format(c_id=container_id,blog=blog_num))
            self.finish()
            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            query="update user_info_table set isGettingBlog=\'{t_time}\' where container_id={cid} ;" \
                .format(t_time=time_stick,cid=container_id)
            dbi.update_asQuery(query)

        if task_id==4:   # this part is in test
            dbi=MySQL_Interface()
            current_time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            target_time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()-60*60*24)) #提早一天
            query='select container_id,update_time,latest_blog from user_info_table ' \
                  'where update_time<\'{target_time}\' and isGettingBlog is null limit {batch}' \
                .format(target_time=target_time_stick,batch=100)
            res=dbi.select_asQuery(query)

            # 将从mysql中取得的用户列表加上必要的变量以后发送给客户端
            res=[[line[0],int(time.mktime(line[1].timetuple())),int(time.mktime(line[2].timetuple()))] for line in res]
            res_cp=res
            res=[line[0]+'-'+str(line[1])+'-'+str(line[2]) for line in res]
            inn=''
            for item in res:
                inn+=item+';'
            inn=inn[0:-1]
            # uid-stamp;uid-timestamp;...;,update  (the formation of order)
            mission_id=random_str(15)
            commend='{list};{task_id},update'.format(list=inn,task_id=mission_id)
            # 传送给客户端的指令格式： ContainerId-UpdateTime-LatestBlog;...;...;...,update
            self.write(commend)
            self.finish()

            # 将用户列表，任务id,以及任务开始时间存入mongodb
            u_list=[dict(container_id=x[0],update_time=x[1],latest_blog=x[2]) for x in res_cp]
            data_toMongo=dict(
                mission_id  =   mission_id,
                user_list   =   u_list,
                mission_start=  int(time.time())
            )
            client=MongoClient('localhost',27017)
            db=client['microblog_spider']
            collec=db.update_mission
            collec.insert(data_toMongo)

            # 将相关内容从mysql中设置isGettingBlog
            user_list_str=''
            for line in res_cp:
                user_list_str+='\'{cid}\','.format(cid=line[0])
            user_list_str=user_list_str[:-1]
            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            query='update user_info_table set isGettingBlog=\'{time}\' where container_id in ( {ulist} )'\
                .format(time=time_stick,ulist=user_list_str)
            dbi.update_asQuery(query)

    def task_assign(self,uuid):
        t_1=['1']         # get social web
        t_2=['2']         # get history weibo , get counter which blog_num<=15000 ,connect with server through www
        t_3=['3']         # get history weibo , get counter which blog_num>15000 ,connect with server through localhost
        t_4=['4']         # update weibo
        if uuid in t_1:
            return 1
        elif uuid in t_2:
            return 2
        elif uuid in t_3:
            return 3
        elif uuid in t_4:
            return 4
        else:
            return -1

class ProxySize(tornado.web.RequestHandler):
    global proxy
    def get(self):
        self.write(str(proxy.size()))
        self.finish()

class ProxyEmpty(tornado.web.RequestHandler):
    global proxy
    def get(self):
        proxy.empty()
        if proxy.size()<2:
            self.write('empty proxy success')
            self.finish()

class ProxyReturn(tornado.web.RequestHandler):
    def post(self):
        global  proxy
        data=self.get_argument('data')
        print('proxy data:',data)
        proxy_list=data.split(';')
        in_data=[x.split(',') for x in proxy_list]
        if in_data.__len__()>0:
            proxy.add(in_data)
            print('Success to receive returned proxy')
            for i in in_data:
                print(i)
        self.write('return success')
        self.finish()

class InfoReturn(tornado.web.RequestHandler):
    def post(self):

        try:
            user_basic_info=self.get_argument('user_basic_info')
            attends=self.get_argument('user_attends')
            user_basic_info=eval(user_basic_info)
            attends=eval(attends)
            self.write('success to return user info')
            self.finish()
        except:
            self.write('fail to return user info')
            self.finish()
            return

        try:
            dbi=MySQL_Interface()
        except:
            print('unable to connect to MySql DB')

        try:
            if attends.__len__()>0:           #store attends info
                table_name='cache_attends'
                attends_col_info=dbi.get_col_name(table_name)
                keys=attends[0].keys()
                attends= [[line[i] if i in keys else '' for i in attends_col_info] for line in attends]
                fans_col_pos=attends_col_info.index('fans_num')
                insert_attends=[]
                for line in attends:
                    if line[fans_col_pos]>1000:
                        insert_attends.append(line)
                dbi.insert_asList(table_name,insert_attends,unique=True)
                print('Success : attends of {uid} is stored in {tname}'
                      .format(uid=user_basic_info['uid'],tname=table_name))
            else:
                pass
        except Exception as e:
            print(e)
            path="temp\\{uid}_attends.pkl".format(uid=user_basic_info['uid'])
            print('unable to store attends of {uid}, it will be stored '
                  .format(uid=user_basic_info['uid']))
            FI.save_pickle(attends,path)

        try:
            atten_num_real=user_basic_info['attends_num']
            atten_num_get=attends.__len__()
            user_basic_info['accuracy']=atten_num_get       # 实际获取到的关注数目
            col_info=dbi.get_col_name('cache_user_info')    # store user basic info
            keys=user_basic_info.keys()
            data=[user_basic_info[i] if i in keys else '' for i in col_info]
            dbi.insert_asList('cache_user_info',[data],unique=True)
            print('Success : basic info of {uid} is stored in cache_user_info'
                  .format(uid=user_basic_info['uid']))
        except Exception as e:
            print(e)
            path='temp\\{uid}_basic_info.pkl'.format(uid=user_basic_info['uid'])
            print('unable to store basic info of {uid} , it will be stored'
                  .format(uid=user_basic_info['uid']))
            FI.save_pickle(user_basic_info,path)

        try:
            if attends.__len__()>0:            # store atten connection web
                from_uid=user_basic_info['uid']
                from_fans_num=user_basic_info['fans_num']
                from_blog_num=user_basic_info['blog_num']
                data=[[from_uid,from_fans_num,from_blog_num,str(x[attends_col_info.index('uid')]),str(x[attends_col_info.index('fans_num')]),str(x[attends_col_info.index('blog_num')])]for x in attends]
                dbi.insert_asList('cache_atten_web',data)
                print('Success : conn web of {uid} is stored in cache_atten_web'
                      .format(uid=user_basic_info['uid']))
            else:
                pass
        except Exception as e:
            print(e)
            path='{uid}_atten_web.pkl'.format(uid=user_basic_info['uid'])
            print('unable to store atten web of {uid} , it will be stored'
                  .format(uid=user_basic_info['uid']))
            FI.save_pickle(data,path)

class HistoryReport(tornado.web.RequestHandler):
    def post(self):

        # 从客户端获取信息
        try:
            latest_time=self.get_argument('latest_time')
            latest_timestamp=self.get_argument('latest_timestamp')
            container_id=self.get_argument('container_id')
            self.write('success')
            self.finish()
            print('Success: to get data from web')
        except Exception as e:
            self.write('fail to return user history')
            self.finish()
            print('Error:server-HistoryReturn:'
                  'Unable to get value from http package,Reason:')
            print(e)
            return

        dbi=MySQL_Interface()
        checkin_timestamp=int(time.time())
        col_info=dbi.get_col_name('cache_history')
        data=dict(
            latest_time=latest_time,
            latest_timestamp=latest_timestamp,
            container_id=container_id,
            checkin_timestamp=checkin_timestamp
        )
        keys=data.keys()
        insert_data=[[data[item] if item in keys else None for item in col_info]]
        dbi.insert_asList('cache_history',insert_data)

class UpdateReport(tornado.web.RequestHandler):
    def post(self):
        # 从客户端获取信息
        try:
            mission_id=self.get_argument('mission_id')
            self.write('success')
            self.finish()
            print('Success: to get update report from web')
        except Exception as e:
            self.write('fail to return user update')
            self.finish()
            print('Error:server-UpdateReturn:'
                  'Unable to get value from http package,Reason:')
            print(e)
            return

        # 将该任务在mongodb中设置为组装状态
        client=MongoClient('localhost',27017)
        db=client['microblog_spider']
        collec=db.update_mission
        collec.update({'mission_id':mission_id},{'$set':{'isReported':int(time.time())}})

def random_str(randomlength=8):
    str = ''
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
    length = len(chars) - 1
    random = Random()
    for i in range(randomlength):
        str+=chars[random.randint(0, length)]
    return str

if __name__=='__main__':
    proxy_lock=threading.Lock()         # proxy thread
    global proxy
    proxy=proxy_pool()
    pm=proxy_manager(proxy,proxy_lock)
    pm.start()

    db_thread=DB_manager()              # database thread
    db_thread.start()

    tornado.options.parse_command_line()    # tornado thread
    Application().listen(options.port)
    # nginx 使用8001接口，分别链接到8002,8003,8004等若干个数据服务器
    DataServer().listen(8002)
    DataServer().listen(8003)
    DataServer().listen(8004)
    tornado.ioloop.IOLoop.instance().start()