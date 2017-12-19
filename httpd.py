# -*- coding: utf-8 -*-
import json
import BaseHTTPServer
from os import stat
from sys import argv
from time import asctime, sleep
from datetime import datetime

from ip import ip2int, ip_query, country_range
from maxmind.sync import main as maxmind_sync
from qqwry.sync import main as qqwry_sync

# http server
class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def version_string(self):
        return 'apache'
    def do_GET(self):
        d = main(path_format(self.path))
        self.send_response(200)
        self.send_header('content-type', d[0])
        self.end_headers()
        self.wfile.write(d[1])

def is_int(s):
    try:
        int(s)
        return True
    except:
        return False

# path移除不可視字元
def path_format(path):
    path = ''.join(filter(lambda i:ord(i) > 32 and ord(i) < 127, path)).split('/')
    path = filter(lambda i:i not in ['', '.', '..'], path)
    return path

def db_date():
    return ', '.join(map(lambda i:i+' update: '+datetime.fromtimestamp(stat(i+'/geoip.db').st_atime).strftime('%Y-%m-%d'), ['maxmind', 'qqwry']))

# server 主程式
def main(path):
    if len(path):
        if path[0].startswith('favicon.ico'):
            return 'image/x-icon', open('favicon.ico', 'rb').read()
        elif path[0] == 'ip' and len(path) > 1 and ip2int(path[1]) > -1:
            return 'application/json', json.dumps(ip_query(path[1]))
        elif path[0] == 'country' and len(path) > 1:
            return 'application/json', json.dumps(country_range(filter(lambda i:i in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', path[1])))
        elif path[0] == 'update':
            maxmind_sync()
            qqwry_sync()
            return 'text/html', 'maxmind, qqwry sync done'
    return 'text/html', open('index.html', 'rb').read()%(db_date(), )

HOST_NAME = '0.0.0.0'
PORT_NUMBER = int(argv[1]) if len(argv) > 1 and is_int(argv[1]) else 80

# 跑 server
if __name__ == '__main__':
    httpd = BaseHTTPServer.HTTPServer((HOST_NAME, PORT_NUMBER), MyHandler)
    print asctime(), "Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print asctime(), "Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER)

