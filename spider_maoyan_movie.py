import requests
from lxml import etree
import pymysql
import time


class MaoyanSpider(object):
    def __init__(self):
        self.start_url = 'http://maoyan.com/films'
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Cookie': 'uuid=1A6E888B4A4B29B16FBA1299108DBE9C1A8251A364BCB1DEA175CC3A24715319; ci=1; __mta=155472772.1505783294508.1505783391674.1505783395557.6; _lxsdk_s=46396f0b9fd7f229bae18c40b684%7C%7C18',
            'Host': 'maoyan.com',
            'Referer': 'http://maoyan.com/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36'
        }
        self.moive_list = []
        self.MYSQL_CONFIG = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'admin2016',
            'db': 'local_db',
            'charset': 'utf8'
        }
        self.conn = pymysql.connect(**self.MYSQL_CONFIG)


    # 解析猫眼电影 请求的url组合 分类 地区 年代
    def parse_urls(self):
        html = requests.get(self.start_url, headers=self.headers).text
        selector = etree.HTML(html)
        infos = selector.xpath('//div[@class="movies-channel"]/div[@class="tags-panel"]/ul[@class="tags-lines"]/li/ul[@class="tags"]/li[position()>1]')
        item = {}
        item['分类'] = []
        item['地区'] = []
        item['年代'] = []
        big_dic = {}
        for type_info in infos:
            href = type_info.xpath('a/@href')[0]
            text = type_info.xpath('a/text()')[0]
            dic = {href: text}
            big_dic[href] = text
            if 'cat' in href:
                type = '分类'
                item[type].append(dic)
            elif 'sourceId' in href:
                type = '地区'
                item[type].append(dic)
            else:
                type = '年代'
                item[type].append(dic)
        all_url = []
        cats = []
        for cat in item['分类']:
            cats.append(list(cat.keys())[0])
        areas = []
        for area in item['地区']:
            areas.append(list(area.keys())[0])
        years = []
        for year in item['年代']:
            years.append(list(year.keys())[0])
        offsets=['0','30','60','90']
        for cat in cats:
            for area in areas:
                for year in years:
                    for offset in offsets:
                        url = self.start_url + cat + '&' + area.lstrip('?') + '&' + year.lstrip('?') + '&offset=' + offset
                        tags = big_dic[cat] + "," + big_dic[area] + "," + big_dic[year]
                        all_url.append({'url': url, 'tags': tags})
        return all_url

    # 根据不同请求 解析出电影的url以及该电影的属性
    def parse_moive(self, item):
        html = requests.get(item['url'], headers=self.headers).text
        selector = etree.HTML(html)
        infos = selector.xpath('//div[@class="movies-list"]/dl[@class="movie-list"]//div[@class="channel-detail movie-item-title"]/a')
        if len(infos) > 0:
            for info in infos:
                movie_url = 'http://maoyan.com' + info.xpath('@href')[0]
                movie_name = info.xpath('text()')[0]
                print(movie_name, movie_url, item['tags'])
                movie_item = {}
                movie_item['movie_name'] = movie_name
                movie_item['movie_url'] = movie_url
                movie_item['tags'] = item['tags']
                movie_item['req_url'] = item['url']
                self.insert_moive(movie_item)


    def insert_moive(self, moive_item):
        insert_sql='insert into maoyan_moive values(%s,%s,%s,%s)'
        cursor.execute(insert_sql,(moive_item['movie_name'],moive_item['movie_url'],moive_item['tags'],moive_item['req_url']))
        print('插入成功')
        conn.commit()

    def get_moive_list(self, urls):
        for i in range(len(urls)):
            item = urls[i]
            print(i + 1, '------->', item['url'],item['tags'])
            self.parse_moive(item)
            time.sleep(3)


if __name__ == '__main__':
    maoyan = MaoyanSpider()
    conn = maoyan.conn
    cursor = conn.cursor()
    all_urls = maoyan.parse_urls()
    maoyan.get_moive_list(all_urls)
    cursor.close()
    conn.close()

