import tornado.web
import tornado.ioloop
import tornado.options
from tornado.options import define,options

import time

define('port',default=8080,help='run on the given port',type=int)

class Application(tornado.web.Application):
    def __init__(self):
        handlers=[
            (r'/test',TestHandler),
            ]
        settings=dict(
            debug=True
        )
        tornado.web.Application.__init__(self,handlers,**settings)

class TestHandler(tornado.web.RequestHandler):
    def get(self):
        # time.sleep(10)
        self.write('233333')
        self.finish()

if __name__=='__main__':
    tornado.options.parse_command_line()    # tornado thread
    Application().listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
    print('ok')

