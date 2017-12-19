#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3
import ipaddress

def ip2int(ip):
    try:
        ip = map(int, ip.split('.'))
        if len(ip) != 4 or any(map(lambda i:i < 0 or i > 255, ip)):
            return -1
        n = sum(map(lambda i:ip[i]<<8*(3-i), range(4)))
        return -1 if n > 0xffffffff else n
    except:
        return -1

def int2ip(n):
    try:
        n = int(n)
        return '.'.join(map(lambda i:str((n&255*2**i)>>i), [24, 16, 8, 0]))
    except:
        return ''

def ip_query(ip):
    conn = sqlite3.connect('maxmind/geoip.db')
    c = conn.cursor()
    n = ip2int(ip)
    locId = 0
    country = ''
    country2 = ''
    describe = ''
    for i in c.execute('SELECT `4` FROM Geo_IP_Country_Whois WHERE `2`<=? AND `3`>=?', (n, n)):
        country2 = i[0]
    for i in c.execute('SELECT locId FROM GeoLite_City_Blocks WHERE startIpNum<=? AND endIpNum>=?', (n, n)):
        locId = i[0]
    try:
        for i in c.execute('SELECT * FROM GeoLite_City_Location WHERE locId=?', (locId, )):
            locId, country, region, city, postalCode, latitude, longitude, mertoCode, areaCode = i
    except sqlite3.OperationalError:
        for i in c.execute('SELECT locId,country,region,postalCode,latitude,longitude,areaCode FROM GeoLite_City_Location WHERE locId=?', (locId, )):
            locId, country, region, postalCode, latitude, longitude, areaCode = i
            city, mertoCode = '', ''
    conn.close()
    conn = sqlite3.connect('qqwry/geoip.db')
    c = conn.cursor()
    for i in c.execute('SELECT describe FROM qqwry WHERE startIpNum<=? AND endIpNum>=?', (n, n)):
        describe = i[0].replace('CZ88.NET', '').replace(u'\u81fa\u7063\u7701', u'\u81fa\u7063')
    conn.close()
    if country2 == country:
        del country2
    del conn, c, i
    return locals()

def country_range(country):
    conn = sqlite3.connect('maxmind/geoip.db')
    c = conn.cursor()
    r = []
    for i in c.execute('SELECT `0`,`1` FROM Geo_IP_Country_Whois WHERE `4`=?', (country, )):
        r += map(str, map(lambda i:i, ipaddress.summarize_address_range(*map(ipaddress.ip_address, i))))
    return r

if __name__ == '__main__':
    from maxmind import sync as maxmind
    from qqwry import sync as qqwry
    print '\033[31mupdate local ip info db.\033[m'
    maxmind.main()
    qqwry.main()

'''
exit()
python
from ip import *
a = ip_query('8.8.8.8')
a.keys()
'''
