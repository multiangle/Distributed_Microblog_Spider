__author__ = 'multiangle'

#======================================================================
#----------------import package--------------------------
# import python package
import tornado.web
import tornado.ioloop
import tornado.options
from tornado.options import define,options
from pymongo import MongoClient

# import from this folder
#======================================================================
class DataServer(tornado.web.Application):
    def __init__(self):
        handlers=[
            (r'/history_data',HistoryDataReturn),
            (r'/auth',DataAuth)
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

            client=MongoClient('localhost',27017)
            db=client['microblog_spider']
            collection=db.assemble_factory

            # store sub pack to assemble factory
            mongo_data=dict(
                data        =data,
                current_id  =current_id,
                total_num   =total_num,
                len         =len,
                container_id=container_id,
                type        ='history'
            )
            result=collection.insert_many(mongo_data)
            print('ServerData->HistoryDataReturn: Success to get data from web')

        except Exception as e:
            self.write('fail to return user history')
            self.finish()
            print('Error:server-HistoryReturn:'
                'Unable to get value from http package,Reason:')
            print(e)
            return

class DataAuth(tornado.web.RequestHandler):
    def get(self):
        self.write('connection success')
        self.finish()

if __name__=='__main__':
    DataServer().listen(8001)
    tornado.ioloop.IOLoop.instance().start()