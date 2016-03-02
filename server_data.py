__author__ = 'multiangle'

import tornado.web
import tornado.ioloop
import tornado.options
from tornado.options import define,options
define('port',default=8001,help='run on the given port',type=int)

class Application(tornado.web.Application):
    def __init__(self):
        pass

