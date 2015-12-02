__author__ = 'multiangle'
#======================================================================
#
#    This program run in a certain server to verify if a proxy is useful
#    Version_0.1_
#    if a http get request is sent to here, it means the http proxy is
#       useful
#
#======================================================================
import tornado.web
import tornado.ioloop
import tornado.options

from tornado.options import define,options
define('port',default=7001,help='run on the given port',type=int)

class Application(tornado.web.Application):
    def __init__(self):
        handlers=[
            (r'/verify_proxy',verify_proxy),
            ]
        settings=dict(
            debug=False
        )
        tornado.web.Application.__init__(self,handlers)

class verify_proxy(tornado.web.RequestHandler):
    def get(self):
        self.write('valid proxy')

if __name__=='__main__':
    tornado.options.parse_command_line()
    Application().listen(options.port)
    tornado.ioloop.IOLoop.instance().start()