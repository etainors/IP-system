#!/usr/bin/python
# -*- coding: utf-8 -*-
import sqlite3

def ip2int(ip):
    try:
        ip = map(int, ip.split('.'))
        return -1 if len(ip) != 4 or any(map(lambda i:i < 0 or i > 255, ip)) else sum(map(lambda i:ip[i]<<(3-i<<3), range(4)))
    except:
        return -1

def int2ip(n):
    try:
        n = int(n)
        return '' if n < 0 or n > 0xffffffff else '.'.join(map(lambda i:str((n&255<<i)>>i), [24, 16, 8, 0]))
    except:
        return ''

def ip_query(ip):
    conn = sqlite3.connect('maxmind/geoip.db')
    c = conn.cursor()
    n = ip2int(ip)
    geoname_id = ''
    geoname_id2 = ''
    describe = ''
    for i in c.execute('SELECT geoname_id FROM GeoLite2_Country_Blocks_IPv4 WHERE startIpNum<=? AND endIpNum>=?', (n, n)):
        geoname_id = i[0]
    for i in c.execute('SELECT * FROM GeoLite2_Country_Locations_en WHERE geoname_id=?', (geoname_id, )):
        geoname_id, locale_code, continent_code, continent_name, country_iso_code, country_name, is_in_european_union = i
    for i in c.execute('SELECT geoname_id,is_anonymous_proxy,is_satellite_provider,postal_code,latitude,longitude,accuracy_radius FROM GeoLite2_City_Blocks_IPv4 WHERE startIpNum<=? AND endIpNum>=?', (n, n)):
        geoname_id2, is_anonymous_proxy, is_satellite_provider, postal_code, latitude, longitude, accuracy_radius = i
    for i in c.execute('SELECT * FROM GeoLite2_City_Locations_en WHERE geoname_id=?', (geoname_id2, )):
        geoname_id2, locale_code, continent_code, continent_name, country_iso_code, country_name, subdivision_1_iso_code, subdivision_1_name, subdivision_2_iso_code, subdivision_2_name, city_name, metro_code, time_zone, is_in_european_union = i
    if geoname_id2 == geoname_id:
        del geoname_id2
    conn.close()
    conn = sqlite3.connect('qqwry/geoip.db')
    c = conn.cursor()
    for i in c.execute('SELECT describe FROM qqwry WHERE startIpNum<=? AND endIpNum>=?', (n, n)):
        describe = i[0].replace('CZ88.NET', '').replace(u'\u81fa\u7063\u7701', u'\u81fa\u7063')
    conn.close()
    del conn, c, i
    return locals()

def country_range(country):
    conn = sqlite3.connect('maxmind/geoip.db')
    c = conn.cursor()
    geoname_id = map(lambda i:i[0], c.execute('SELECT geoname_id FROM GeoLite2_Country_Locations_en WHERE country_iso_code=?', (country, )))
    return map(lambda i:i[0], c.execute('SELECT network FROM GeoLite2_Country_Blocks_IPv4 WHERE '+' or '.join(['geoname_id=?']*len(geoname_id)), tuple(geoname_id))) if geoname_id else []

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
