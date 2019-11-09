#!/usr/bin python
# -*- coding:utf-8 -*-

# v1.0 2017.11.07
# B站的番剧评分只能通过手机客户端查看太蠢了，后来发现其实网页也有只是没有显示。
# 本来想写个UserScript，但是后来顺手就写了个爬虫。
# v1.1 2017.12.11
# 添加了日期方便每个月进行收集统计，稍微优化了一下代码结构。
# v1.2 2018.01.27
# 将程序从单线程改为多线程。
# 2019.11.08
# update api
# honor to https://github.com/guyigenius

import requests
import json
import sqlite3
import csv
import time
import threading


def bilibili_rating(bangumi_id):
    response = requests.get('https://api.bilibili.com/pgc/view/web/media?media_id={}'.format(bangumi_id))
    data = json.loads(response.text)
    response.close()
    try:
        is_finish = int(data['result']['copyright']['is_finish'])
        count = int(data['result']['rating']['count'])
        score = float(data['result']['rating']['score'])
        season_id = int(data['result']['season_id'])
        title = '\"{0}\"'.format(data['result']['title'])
        publish_year = '\"{0}\"'.format(data['result']['publish']['pub_date'])
        print(season_id, title, score, count, is_finish, publish_year)
        try:
            lock.acquire(True)
            cursor.execute('insert into bangumi_{0} values ({1}, {2}, {3}, {4}, {5}, {6})'
                           .format(date, season_id, title, score, count, is_finish, publish_year))
            conn.commit()
        except sqlite3.IntegrityError:
            pass
        finally:
            lock.release()
    except KeyError:
        return None


def bilibili_rating_thread(bangumi_id):
    # 每个线程负责1个列表
    for i in bangumi_id:
        print('bangumi_id:{0}, thread_id:{1}'.format(i, threading.current_thread().name))
        bilibili_rating(i)
        time.sleep(1)


def fetch_bangumi_ids_of_publish_year(year, endyear):
    print('fetching year {} ~ {}'.format(year, endyear))
    api = "https://api.bilibili.com/pgc/season/index/result"
    param = {
        'season_version': 1,
        'area': -1,
        'is_finish':-1,
        'copyright':-1,
        'season_status':-1,
        'season_month':-1,
        'year':'[{},{})'.format(year, endyear),
        'style_id':-1,
        'order':5,
        'st':1,
        'sort':1,
        'page':1,
        'season_type':1,
        'pagesize':20,
        'type':1,
    }
    response = requests.get(api, params=param)
    data = json.loads(response.text)
    res = [int(data['data']['list'][i]['media_id']) for i in range(0, len(data['data']['list']))]
    while int(data['data']['has_next']) == 1:
        param['page'] = param['page'] + 1
        response = requests.get(api, params=param)
        data = json.loads(response.text)
        tmpres = [int(data['data']['list'][i]['media_id']) for i in range(0, len(data['data']['list']))]
        res.extend(tmpres)
    return res


def crawler():
    # Fetch bilibili rating data and write into database
    bangumi_ids = []
    bangumi_ids.extend(fetch_bangumi_ids_of_publish_year(2005, 2010))
    bangumi_ids.extend(fetch_bangumi_ids_of_publish_year(2010, 2015))
    for x in range(2015,2020):
        bangumi_ids.extend(fetch_bangumi_ids_of_publish_year(x, x+1))
    thread = []
    groups = [bangumi_ids[i:i+1000] for i in range(0, len(bangumi_ids), 1000)]
    for g in groups:
        t = threading.Thread(target=bilibili_rating_thread, args=(g,))
        thread.append(t)
    for i in range(0, len(thread)):
        thread[i].start()
    for i in range(0, len(thread)):
        thread[i].join()


if __name__ == '__main__':
    date = time.strftime('%Y%m%d')
    conn = sqlite3.connect('bilibili_bangumi.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('create table if not exists bangumi_{0} (season_id int primary key, '
                   'title varchar(40), score float, count int, is_finish int, publish_year varchar(40))'.format(date))
    lock = threading.Lock()
    crawler()
    # Write into output file.
    cursor.execute('select * from bangumi_{0}'.format(date))
    values = cursor.fetchall()
    print(values)
    with open('bilibili_score_{0}.csv'.format(date), 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow(['season_id', 'title', 'score', 'count', 'is_finish', 'publish_year'])
        writer.writerows(values)
    cursor.close()
    conn.close()
