__author__ = 'multiangle'
#--------------------------------------------------------
#
#    This client part of distrubuted microblog spider.
#    Version_0.1_
#    In this version, the client part is mianly achieved by tornado
#
#--------------------------------------------------------

#--------------------------------------------------------
#----------------import package--------------------------

# import python package

# import tornado
import tornado.web
import tornado.ioloop
import tornado.options
from tornado.options import  define,options

# import from this folder
import client_config as config
#--------------------------------------------------------

#------------------code session--------------------------
define('port',default=7000,help='run on the given port',type=int)

class Application(tornado.web.Application):

    def __init__(self):
        handlers=[
            (r'/add_user_id',add_user_id),
        ]
        settings=dict(
            debug=config.TORNADO_DEBUG
        )
        tornado.web.Application.__init__(self,handlers,**settings)

if __name__=='__main__':
    tornado.options.parse_command_line()
