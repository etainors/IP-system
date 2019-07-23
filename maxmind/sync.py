#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, csv, urllib2, sqlite3
from time import time, sleep
from shutil import rmtree
from zipfile import ZipFile
from StringIO import StringIO

# 顯示錯誤資訊
def errmess(n=1):
    from traceback import format_exc
    return '\n'.join(format_exc().split('\n')[-n-1:-1])

def try_remove(f):
    try:
        os.remove(f)
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

def is_int(s):
    try:
        return int(s) == float(s)
    except ValueError:
        return False

def is_float(s):
    try:
        return True if float(s) < 9223372036854775808 and float(s) >= -9223372036854775808 else False
    except ValueError:
        return False

def ip2int(ip):
    try:
        ip = map(int, ip.split('.'))
        return -1 if len(ip) != 4 or any(map(lambda i:i < 0 or i > 255, ip)) else sum(map(lambda i:ip[i]<<(3-i<<3), (0, 1, 2, 3)))
    except:
        return -1

int_mask = [0, 2147483648, 3221225472, 3758096384, 4026531840, 4160749568, 4227858432, 4261412864, 4278190080, 4286578688, 4290772992, 4292870144, 4293918720, 4294443008, 4294705152, 4294836224, 4294901760, 4294934528, 4294950912, 4294959104, 4294963200, 4294965248, 4294966272, 4294966784, 4294967040, 4294967168, 4294967232, 4294967264, 4294967280, 4294967288, 4294967292, 4294967294, 4294967295]
int_mask_size = [4294967295, 2147483647, 1073741823, 536870911, 268435455, 134217727, 67108863, 33554431, 16777215, 8388607, 4194303, 2097151, 1048575, 524287, 262143, 131071, 65535, 32767, 16383, 8191, 4095, 2047, 1023, 511, 255, 127, 63, 31, 15, 7, 3, 1, 0]

def ip_int_range(network):
    ip, s = network.split('/')
    n = ip2int(ip)
    m = int(s)
    r = n&int_mask[m]
    return [r, r+int_mask_size[m]]

def sqlite_font(a):
    if len(a) == 0:
        return ''
    if (a[0] == "'" or a[0] == '"') and a[0] == a[-1]:
        a = a[1:-1]
    return int(a) if is_int(a) else float(a) if is_float(a) else a.decode('utf8')

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
    l = line_split(f.readline().decode('utf8'))
    if data_start_line < 1:
        fields = range(len(l))
        for i in range(len(fields)):
            fields[i] = str(fields[i])
    else:
        for i in range(data_start_line-1):
            l = line_split(f.readline().decode('utf8'))
        fields = l
        l = line_split(f.readline().decode('utf8'))
    f.close()
    if 'Blocks_IPv4' in table:
        fields.append('startIpNum')
        fields.append('endIpNum')
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
        i = map(sqlite_font, i)
        if 'Blocks_IPv4' in table:
            i += ip_int_range(i[0])
        try:
            s = 'INSERT INTO %s VALUES (%s)'%(table, ','.join(['?']*len(i)))
            c.execute(s, tuple(i))
        except:
            print errmess()
            print s, i
            exit()
        n += 1
        if n%100000 == 0:
            print table, n
    #
    conn.commit()
    conn.close()

def main():
    t = time()
    cwd = os.getcwd()
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    map(try_remove, ['geoip.db', 'GeoIPCountryWhois.csv', 'GeoLiteCity-Blocks.csv', 'GeoLiteCity-Location.csv'])
    #url = ['http://geolite.maxmind.com/download/geoip/database/GeoIPCountryCSV.zip', 'http://geolite.maxmind.com/download/geoip/database/GeoLiteCity_CSV/GeoLiteCity-latest.zip']
    url = ['https://geolite.maxmind.com/download/geoip/database/GeoLite2-Country-CSV.zip', 'https://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip']
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
            os.rename(f[i], f[i].split('/')[-1])
            d = '/'.join(f[i].split('/')[:-1])
            if d not in ds:
                ds.append(d)
            f[i] = f[i].split('/')[-1]
    for d in ds:
        rmtree(d, True)
    #csv2sqlite('GeoIPCountryWhois.csv', 'geoip.db', 'Geo_IP_Country_Whois', 0)#7m
    #print 'country finish'
    #csv2sqlite('GeoLiteCity-Blocks.csv', 'geoip.db', 'GeoLite_City_Blocks', 2)#65m
    #print 'block finish'
    #csv2sqlite('GeoLiteCity-Location.csv', 'geoip.db', 'GeoLite_City_Location', 2)#29m
    #print 'location finish'
    for i in sorted(filter(lambda i:i.endswith('.csv'), os.listdir(u'.'))):
        csv2sqlite(i, 'geoip.db', i[:-4].replace('-', '_'), 1)
        print i[:-4].replace('-', '_'), 'finish'
    if '' in f:
        f.remove('')
    map(try_remove, f)
    os.chdir(cwd)
    print int(time()-t), 'sec'

if __name__ == '__main__':
    main()
