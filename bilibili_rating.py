# v1.0 2017.11.07
# B站的番剧评分只能通过手机客户端查看太蠢了，后来发现其实网页也有只是没有显示。
# 本来想写个UserScript，但是后来顺手就写了个爬虫。
# v1.1 2017.12.11
# 添加了日期方便每个月进行收集统计，稍微优化了一下代码结构。
# v1.2 2018.01.27
# 将程序从单线程改为多线程。


import requests
import json
import sqlite3
import csv
import time
import threading


def bilibili_rating(bangumi_id):
    payload = {'callback': 'seasonListCallback'}
    response = requests.get('https://bangumi.bilibili.com/jsonp/seasoninfo/{0}.ver'.format(bangumi_id), params=payload)
    data = json.loads(response.text[19:-2])
    response.close()
    try:
        is_finish = int(data['result']['is_finish'])
        count = int(data['result']['media']['rating']['count'])
        score = float(data['result']['media']['rating']['score'])
        season_id = int(data['result']['season_id'])
        title = '\"{0}\"'.format(data['result']['media']['title'])
        print(season_id, title, score, count, is_finish)
        try:
            lock.acquire(True)
            cursor.execute('insert into bangumi_{0} values ({1}, {2}, {3}, {4}, {5})'
                           .format(date, season_id, title, score, count, is_finish))
            conn.commit()
        except sqlite3.IntegrityError:
            pass
        finally:
            lock.release()
    except KeyError:
        return None


def bilibili_rating_thread(bangumi_id):
    i = bangumi_id
    # 每个线程负责1000个
    while i < bangumi_id + 1000:
        print('bangumi_id:{0}, thread_id:{1}'.format(i, threading.current_thread().name))
        bilibili_rating(i)
        i += 1
        time.sleep(1)


def crawler():
    # Fetch bilibili rating data and write into database.
    # 2018.01开始新番的bangumi_id都变成了20000开始的，很诡异。所以现在的区间为：0-7000，20000-25000。
    thread = []
    for i in range(0, 7000, 1000):
        t = threading.Thread(target=bilibili_rating_thread, args=(i,))
        thread.append(t)
    for i in range(20000, 25000, 1000):
        t = threading.Thread(target=bilibili_rating_thread, args=(i,))
        thread.append(t)
    for i in range(0, len(thread)):
        thread[i].start()
    for i in range(0, len(thread)):
        thread[i].join()


if __name__ == '__main__':
    date = time.strftime('%Y%m%d')
    conn = sqlite3.connect('bilibili_bangumi.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('create table bangumi_{0} (season_id int primary key, '
                   'title varchar(40), score float, count int, is_finish int)'.format(date))
    lock = threading.Lock()
    crawler()
    # Write into output file.
    cursor.execute('select * from bangumi_{0}'.format(date))
    values = cursor.fetchall()
    print(values)
    with open('bilibili_score_{0}.csv'.format(date), 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file, delimiter=';')
        writer.writerows(values)
    cursor.close()
    conn.close()
