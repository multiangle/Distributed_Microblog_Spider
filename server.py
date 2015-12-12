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

    VERSION:    _0.1_

    UPDATE_HISTORY:
        _0.1_:  The 1st edition
"""
#======================================================================
#----------------import package--------------------------
# import python package
import threading
import time

# import from outer package
import tornado.web
import tornado.ioloop
import tornado.options
from tornado.options import define,options

# import from this folder
from server_proxy import proxy_pool,proxy_manager
import server_config as config
from server_database import DB_manager
import File_Interface as FI
from DB_Interface import MySQL_Interface

#=======================================================================
define('port',default=8000,help='run on the given port',type=int)

class Application(tornado.web.Application):
    def __init__(self):
        handlers=[
            (r'/auth',AuthHandler),
            (r'/proxy/',ProxyHandler),
            (r'/task',TaskHandler),
            (r'/proxy_size',ProxySize),
            (r'/proxy_empty',ProxyEmpty),
            (r'/proxy_return',ProxyReturn),
            (r'/info_return',InfoReturn)
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
        proxy_list=data.split(';')
        in_data=[x.split(',') for x in proxy_list]
        proxy.add(in_data)
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
                dbi.insert_asList(table_name,attends,unique=True)
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
                user_uid=user_basic_info['uid']
                data=[[user_uid,str(x[attends_col_info.index('uid')])]for x in attends]
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