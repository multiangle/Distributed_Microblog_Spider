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
from pymongo import MongoClient

# import from outer package
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
            (r'/history_return',HistoryReturn)
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

        if task_id==2:      # this part is in test
            dbi=MySQL_Interface()
            query='select container_id,blog_num from user_info_table ' \
                  'where (isGettingBlog is null and update_time is null) ' \
                  'order by fans_num desc limit 1 ;'
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

        if task_id==3:
            pass
            #TODO update content

    def task_assign(self,uuid):
        t_1=['1']         # get social web
        t_2=['2']         # get history weibo
        t_3=[]          # update the weibo info
        if uuid in t_1:
            return 1
        elif uuid in t_2:
            return 2
        elif uuid in t_3:
            return 3
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

class HistoryReturn(tornado.web.RequestHandler):
    def post(self):

        # 从客户端获取信息
        try:
            user_history=self.get_argument('user_history')
            latest_time=self.get_argument('latest_time')
            latest_timestamp=self.get_argument('latest_timestamp')
            container_id=self.get_argument('container_id')
            user_history=eval(user_history)
            self.write('success to return user history')
            self.finish()
            print('Success: to get data from web')
        except Exception as e:
            self.write('fail to return user history')
            self.finish()
            print('Error:server-HistoryReturn:'
                  'Unable to get value from http package,Reason:')
            print(e)
            return

        # 连接
        try:
            dbi=MySQL_Interface()
        except:
            print('Error:server-HistoryReturn:'
                  'Unable to connect to MySQL')

        # 从MYSQL获取该用户相关信息
        try:
            query='select * from user_info_table where container_id=\'{cid}\''\
                .format(cid=container_id)
            user_info=dbi.select_asQuery(query)[0]
            col_name=dbi.get_col_name('user_info_table')
        except Exception as e:
            print('Error:server-HistoryReturn:'
                  'No such user in MySQL.user_info_table,Reason:')
            print(e)

        # 将数据存入Mongodb以后将相关信息存入mysql，并将isGettingBlog字段设为空
        try:
            blog_len=user_history.__len__()
            wanted_blog_len=user_info[col_name.index('blog_num')]
            blog_accuracy=blog_len/wanted_blog_len
            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            if not user_info[col_name.index('update_time')]:
                save_data_inMongo(user_history)
                # query='update user_info_table set ' \
                #       'update_time=\'{up_time}\',' \
                #       'latest_blog=\'{latest_blog}\',' \
                #       'isGettingBlog=null ' \
                #       'where container_id=\'{cid}\';'\
                #     .format(up_time=time_stick,latest_blog=latest_time,cid=container_id)
                query='update user_info_table set ' \
                      'update_time=\'{up_time}\',' \
                      'latest_blog=\'{latest_blog}\'' \
                      'where container_id=\'{cid}\';' \
                    .format(up_time=time_stick,latest_blog=latest_time,cid=container_id)
                dbi.update_asQuery(query)
            else:
                query='update user_info_table set isGettingBlog=null where container_id=\'{cid}\''\
                    .format(cid=container_id)
                dbi.update_asQuery(query)

            query='insert into accuracy_table values ({acc},\'{t_s}\') ;'\
                .format(acc=blog_accuracy,t_s=time_stick)
            dbi.insert_asQuery(query)

            print('Success: insert user into MongoDB, the num of data is {len}'
                  .format(len=blog_len))
        except Exception as e:
            print('Error:server-HistoryReturn:'
                  'Unable to update data in MySQL.user_info_tabe,Reason:')
            print(e)

def save_data_inMongo(dict_data):
    client=MongoClient('localhost',27017)
    db=client['microblog_spider']
    collection=db.test3
    result=collection.insert_many(dict_data)

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
    tornado.ioloop.IOLoop.instance().start()