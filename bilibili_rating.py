# v1.0 2017.11.07
# B站的番剧评分只能通过手机客户端查看太蠢了，后来发现其实网页也有只是没有显示。
# 本来想写个UserScript，但是后来顺手就写了个爬虫。


import requests
import json
import sqlite3
import csv


conn = sqlite3.connect('bilibili_bangumi.db')
cursor = conn.cursor()
cursor.execute('create table bangumi_20171107 (season_id int primary key, '
               'title varchar(40), score float, count int, is_finish int)')

session = requests.session()
url = 'https://bangumi.bilibili.com/jsonp/seasoninfo/{0}.ver'


def bilibili_rating(bangumi_id):
    payload = {'callback': 'seasonListCallback'}
    response = requests.get(url.format(bangumi_id), params=payload)
    data = json.loads(response.text[19:-2])
    try:
        is_finish = int(data['result']['is_finish'])
        count = int(data['result']['media']['rating']['count'])
        score = float(data['result']['media']['rating']['score'])
        season_id = int(data['result']['season_id'])
        title = '\"{0}\"'.format(data['result']['media']['title'])
        print(season_id, title, score, count, is_finish)
        try:
            cursor.execute('insert into bangumi_20171107 values ({0}, {1}, {2}, {3}, {4})'
                           .format(season_id, title, score, count, is_finish))
            conn.commit()
        except sqlite3.IntegrityError:
            pass
    except KeyError:
        return None


for i in range(1, 7000):
    bilibili_rating(i)
    print(i)

cursor.execute('select * from bangumi_20171107')
values = cursor.fetchall()
print(values)
with open('bangumi_20171107.csv', 'w', newline='', encoding='utf-8') as csv_file:
    writer = csv.writer(csv_file, delimiter=';')
    writer.writerows(values)
cursor.close()
conn.close()
