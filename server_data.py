__author__ = 'multiangle'

import tornado.web
import tornado.ioloop
import tornado.options
from tornado.options import define,options

class Application(tornado.web.Application):
    def __init__(self):
        handlers=[
            (r'/history_data',HistoryDataReturn)
        ]
        setting=dict(
            debug=True
        )
        tornado.web.Application.__init__(self,handlers,**setting)

class HistoryDataReturn(tornado.web.RequestHandler):
    def post(self):
        try:
            data=self.get_argument('data')
            current_id=self.get_argument('current_id')
            total_num=self.get_argument('total_num')
            len=self.get_argument('len')
            container_id=self.get_argument('container_id')
            self.write('success')
            self.finish()
            print('Success: to get data from web')
            #todo 将收到的子包放入装配车间
        except Exception as e:
            self.write('fail to return user history')
            self.finish()
            print('Error:server-HistoryReturn:'
                'Unable to get value from http package,Reason:')
            print(e)
            return

if __name__=='__main__':
    Application().listen(8001)
    tornado.ioloop.IOLoop.instance().start()