#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv, urllib2, sqlite3
from os import chdir, remove, rename, getcwd
from os.path import abspath, dirname
from time import time, sleep
from shutil import rmtree
from zipfile import ZipFile
from StringIO import StringIO
from traceback import format_exc

# 顯示錯誤資訊
def errmess(n = 1):
    log = format_exc()
    a = log.rfind('\n')
    for i in range(n):
        a = log.rfind('\n', 0, a)
    return log[a+1:-1]

def try_remove(f):
    try:
        remove(f)
    except:
        pass

def wget(url):
    req = urllib2.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0')
    while True:
        try:
            return urllib2.urlopen(req).read()
        except KeyboardInterrupt:
            return ''
        except Exception, e:
            print e
            sleep(1)

def is_float(s):
    try:
        return True if float(s) < 9223372036854775808 and float(s) >= -9223372036854775808 else False
    except ValueError:
        return False

def sqlite_font(a):
    if len(a) == 0:
        return "''"
    if (a[0] == "'" or a[0] == '"') and a[0] == a[-1]:
        if is_float(a[1:-1]):
            return a[1:-1]
        else:
            if a[1:-1].find("'") > -1:
                return "'%s'"%(a[1:-1].replace("'", "''"), )
            return a
    else:
        if is_float(a):
            return a
        else:
            if a.find("'") > -1:
                a = a.replace("'", "''")
            return "'%s'"%(a, )

def line_split(s, mod=0):
    if mod == 0:
        return s[:-1].split(',')
    elif mod == 1:
        return s[1:-2].split('", "')
    elif mod == 2:
        s = s.split(', ')
        if len(s) > 4:
            return [', '.join(s[:-4]), s[-3], s[-2], s[-1]]
        return s

def csv2sqlite(csv_file, db, table, data_start_line=0, split_type=0):
    #
    f = open(csv_file)
    l = line_split(f.readline())
    if data_start_line < 1:
        fields = range(len(l))
        for i in range(len(fields)):
            fields[i] = str(fields[i])
    else:
        for i in range(data_start_line-1):
            l = line_split(f.readline())
        fields = l
        l = line_split(f.readline())
    f.close()
    #
    conn = sqlite3.connect(db)
    c = conn.cursor()
    try:
        s = 'CREATE TABLE '+table+" ('"+"','".join(fields)+"')"
        c.execute(s)
    except:
        print errmess()
        print s
        exit()
    #
    n = 0
    for i in csv.reader(open(csv_file), dialect='excel'):
        if n < data_start_line:
            n += 1
            continue
        for j in range(len(i)):
            i[j] = sqlite_font(i[j])
        try:
            s = 'INSERT INTO '+table+' VALUES ('+','.join(i)+')'
            c.execute(s)
        except:
            print errmess()
            print s
            exit()
        n += 1
        if n%100000 == 0:
            print table, n
    #
    conn.commit()
    conn.close()

def main():
    t = time()
    cwd = getcwd()
    chdir(dirname(abspath(__file__)))
    map(try_remove, ['geoip.db', 'GeoIPCountryWhois.csv', 'GeoLiteCity-Blocks.csv', 'GeoLiteCity-Location.csv'])
    url = ['http://geolite.maxmind.com/download/geoip/database/GeoIPCountryCSV.zip', 'http://geolite.maxmind.com/download/geoip/database/GeoLiteCity_CSV/GeoLiteCity-latest.zip']
    f = []
    for u in url:
        print u
        z = ZipFile(StringIO(wget(u)))
        z.extractall()
        f += z.namelist()
        print 'downloaded'
    #f = ['GeoIPCountryWhois.csv', 'GeoLiteCity_20141104/GeoLiteCity-Blocks.csv', 'GeoLiteCity_20141104/GeoLiteCity-Location.csv']
    ds = []
    for i in range(len(f)):
        if f[i].endswith('/'):
            if f[i] not in ds:
                ds.append(f[i])
            f[i] = ''
        elif '/' in f[i]:
            rename(f[i], f[i].split('/')[-1])
            d = '/'.join(f[i].split('/')[:-1])
            if d not in ds:
                ds.append(d)
            f[i] = f[i].split('/')[-1]
    for d in ds:
        rmtree(d, True)
    csv2sqlite('GeoIPCountryWhois.csv', 'geoip.db', 'Geo_IP_Country_Whois', 0)#7m
    print 'country finish'
    csv2sqlite('GeoLiteCity-Blocks.csv', 'geoip.db', 'GeoLite_City_Blocks', 2)#65m
    print 'block finish'
    csv2sqlite('GeoLiteCity-Location.csv', 'geoip.db', 'GeoLite_City_Location', 2)#29m
    print 'location finish'
    if '' in f:
        f.remove('')
    map(try_remove, f)
    chdir(cwd)
    print int(time()-t), 'sec'

if __name__ == '__main__':
    main()
