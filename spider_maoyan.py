import os
import time
import requests
from lxml import etree
import pymysql


class MaoyanSpider(object):
        def __init__(self):
            self.start_url = "http://maoyan.com/films?offset={}"
            self.headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Cookie': '你的cookie',
                'Host': 'maoyan.com',
                'Referer': 'http://maoyan.com/',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.79 Safari/537.36'
            }
            #数据库配置
            self.MYSQL_CONFIG = {
                'host': 'localhost',
                'port': 3306,
                'user': 'root',
                'password': '密码',
                'db': 'maoyan',
                'charset': 'utf8'
            }
            self.conn = pymysql.connect(**self.MYSQL_CONFIG)

        def all_req_url(self):
            all_req_url = []
            for i in range(9):
                offset = str(i * 30)
                req_url = self.start_url.format(offset)
                all_req_url.append(req_url)
            return all_req_url

        # 根据不同请求 解析出电影的url以及该电影的属性
        def parse_moive(self, url):
            html = requests.get(url, headers=self.headers).text
            selector = etree.HTML(html)
            infos = selector.xpath('//div[@class="movies-list"]/dl[@class="movie-list"]//div[@class="channel-detail movie-item-title"]/a')
            if len(infos) > 0:
                for info in infos:
                    movie_url = 'http://maoyan.com' + info.xpath('@href')[0]
                    movie_name = info.xpath('text()')[0]
                   #猫眼页面 字典存储 电影名  电影url 请求的url
                    moive_item = {}
                    moive_item['movie_name'] = movie_name
                    moive_item['movie_url'] = movie_url
                    moive_item['req_url'] = url
                    #传递字典到电影详情页面
                    self.moive_detail(moive_item)
        #电影详情页
        def moive_detail(self, moive_item):
            item = moive_item
            html = requests.get(moive_item['movie_url'], headers=self.headers).text
            selector = etree.HTML(html)
            #猫眼电影 票房和想看人数 是用到了特殊字符加密  页面找到加密文件 解密
            try:
                eot_url = 'http://' + selector.xpath('//style/text()')[0].split(';')[1].strip().split('//')[1].replace("')",'')
                item['eot_url'] = eot_url
                # 解析字体 找到映射字典
                eot_dict = self.eot_to_dict(item)
                print("===解析eot===", eot_dict)
                # 想看数 票房
                want_see_nums_span = selector.xpath('//div[@class="movie-stats-container"]//div[@class="movie-index-content score normal-score"]//span[@class="stonefont"]/text()')
                if want_see_nums_span:
                    try:
                        want_see_nums = "".join([eot_dict[x.encode('raw_unicode_escape').decode()[2::]] for x in want_see_nums_span[0]])
                    except:
                        want_see_nums = "0"
                else:
                    want_see_nums = '0'
                piaofang_span = selector.xpath('//div[@class="movie-stats-container"]//div[@class="movie-index-content box"]//span[@class="stonefont"]/text()')
                if piaofang_span:
                    try:
                        piaofang = "".join([eot_dict[x.encode('raw_unicode_escape').decode()[2::]] for x in piaofang_span[0]])
                    except:
                        piaofang = "0"
                else:
                    piaofang = '0'
            except:
                print('eot文件异常',moive_item['movie_url'])
                want_see_nums = "0"
                piaofang = "0"
            #别名
            ename_div = selector.xpath('//div[@class="ename ellipsis"]/text()')
            ename = ename_div[0] if len(ename_div) > 0 else '无别名'
            #题材 地区 时长 上映地点
            types = selector.xpath('//div[@class="movie-brief-container"]/ul/li/text()')
            infos = ";".join(['/'.join([y.strip() for y in x.strip().replace('\n', '').split('/')]) for x in types])
            #剧情介绍
            movie_desc_span = selector.xpath('//span[@class="dra"]/text()')
            movie_desc = movie_desc_span[0] if len(movie_desc_span) > 0 else "无简介"
            #导演表
            celebritys_div = selector.xpath('//div[@class ="celebrity-container"]/div[@class="celebrity-group"][1]//div[@class="info"]/a/text()')
            celebritys = ';'.join([x.strip().replace('\n', '') for x in celebritys_div])
            #演员表
            actors_div = selector.xpath('//div[@class ="celebrity-container"]/div[@class="celebrity-group"][position()>1]//div[@class="info"]/a/text()')
            actors = ';'.join([x.strip().replace('\n', '') for x in actors_div])
            item['ename'] = ename
            item['movie_desc'] = movie_desc
            item['infos'] = infos
            item['celebritys'] = celebritys
            item['actors'] = actors
            item['want_see_nums'] = want_see_nums
            item['piaofang'] = piaofang
            #入库
            self.insert_moive(item)

        '''
        票房和想观看人数 参考知乎:怎样用scrapy爬猫眼电影的评分和票房？
        https://www.zhihu.com/question/51498805
        函数:has_next_tag和eot_to_dict
        '''
        def has_next_tag(self, f):
            if f.read(1):
                f.seek(-1, 1)
                return True
            else:
                return False
        #解析详情页 eot文件的url请求 并将请求源码写入 电影名.eot文件 找到加密的数字的映射关系
        def eot_to_dict(self, item):
            dic = []
            eot_url = item['eot_url']
            eot_content = requests.get(eot_url).content
            file_name = '%s.eot' % item['movie_name']
            with open(file_name, 'wb') as eot:
                eot.write(eot_content)
            with open(file_name, 'rb') as f:
                while self.has_next_tag(f):
                    f_type = f.read(1)
                    if f_type == b'\x07' and f.read(1) == b'u':
                        f.seek(2, 1)
                        dic.append(((f.read(4).decode()).lower()))
            num = [str(i) for i in range(10)]
            item = dict(zip(dic, num))
            os.remove(file_name)
            return item

        #删除表数据
        def truncate_movie(self):
            truncate_sql='truncate table movie'
            cursor.execute(truncate_sql)
            conn.commit()

        #写入数据库
        def insert_moive(self, item):
            insert_sql = 'insert into movie (movie_url,movie_name,ename,want_see_nums,piaofang,movie_desc,infos,celebritys,actors,req_url) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            cursor.execute(insert_sql, (
            item['movie_url'], item['movie_name'], item['ename'], item['want_see_nums'], item['piaofang'],item['movie_desc'], item['infos'],
            item['celebritys'], item['actors'], item['req_url']))
            conn.commit()


if __name__ == '__main__':
    maoyan = MaoyanSpider()
    conn = maoyan.conn
    cursor = conn.cursor()
    all_urls = maoyan.all_req_url()
    maoyan.truncate_movie()
    for url in all_urls:
        maoyan.parse_moive(url)
    cursor.close()
    conn.close()