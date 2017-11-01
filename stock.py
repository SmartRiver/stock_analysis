# -*- coding: utf-8 -*-
__author__ = 'johnson'

import tornado.ioloop
import tornado.web
from tornado.web import MissingArgumentError
import sys
import tornado.gen
import tornado.httpserver
import tornado.concurrent
import json
import tornado.options
import os
import time
import analysis
from analysis import update_dl_stocks, get_format_stocks,get_stocks, html_dl_stocks, html_single_stock

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            ('/stock/(.*)', StockHandler),
            ('/test/(.*)', TestHandler),
        ]
        super(Application, self).__init__(handlers)

class StockHandler(tornado.web.RequestHandler):
    def get(self, request_type):
        self.set_header('Access-Control-Allow-Origin','*')
        symbol = self.get_argument('symbol')
        if symbol == 'all':
            res = get_stocks()
            res = '<html><head><title>分析结果</title></head><body>{}</body></html>'.format(html_dl_stocks(res))
        elif symbol == 'dl_stocks':
            res = get_format_stocks()
            res = json.dumps(res,ensure_ascii=False, indent=4)
        elif symbol == 'update':
            update_dl_stocks()
        else:      
            res = html_single_stock(int(symbol))
            res = '<html><head><title>分析结果</title></head><body>{}</body></html>'.format(res)
        try:
            self.finish(res)
        except Exception as e:
            print(e)

class TestHandler(tornado.web.RequestHandler):
    def get(self,request_type):
        self.finish('return '+str(time.time()))

if __name__ == "__main__":
    try:
        print('stock analysis service starting.')
        
        tornado.options.define("port", default=9704, help="run on the given port", type=int)
        tornado.options.parse_command_line()
        http_server = tornado.httpserver.HTTPServer(Application())
        http_server.listen(tornado.options.options.port)
        tornado.ioloop.IOLoop.current().start()
        print('stock analysis service starts success.')
    except Exception as e:
        print(e)
        print('stock analysis service starts failed.')
