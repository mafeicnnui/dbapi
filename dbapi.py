#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/9/16 9:32
# @Autho r : ma.fei
# @File    : dbapi.py
# @Func    : dbops_api Server
# @Software: PyCharm

import sys
import tornado.ioloop
import tornado.web
import tornado.options
import tornado.httpserver
import tornado.locale
from   tornado.options  import define
from   utils.urls  import urls

define("port", default=sys.argv[1], help="run on the given port", type=int)
class Application(tornado.web.Application):
    def __init__(self):
        handlers = urls
        tornado.web.Application.__init__(self, handlers)

if __name__ == '__main__':
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(sys.argv[1])
    print('Dbapi Api Server running {0} port ...'.format(sys.argv[1]))
    tornado.ioloop.IOLoop.instance().start()