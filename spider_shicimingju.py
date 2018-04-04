import requests
import pymysql
from lxml import etree

class TangShi(object):
    def __init__(self):
        self.base_url = 'http://www.shicimingju.com/category/tangdaishiren/page/{}'
        self.start_url = 'http://www.shicimingju.com/category/tangdaishiren/page/1'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
        }
        self.MYSQL_CONFIG = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '密码',
            'db': 'local_db',
            'charset': 'utf8'
        }
        self.conn = pymysql.connect(**self.MYSQL_CONFIG)

    def selector(self, url):
        html = requests.get(url, headers=self.headers).content
        selector = etree.HTML(html)
        return selector

    # 获取诗人总页面数  循环生成新url
    def get_poem_page_urls(self):
        poem_page_urls = []
        selector = self.selector(self.start_url)
        total_page = selector.xpath('//div[@class="yema"]/text()')[0].split('/')[1].replace('共', '').replace('页)', '')
        for i in range(1, int(total_page) + 1):
            poem_page_url = self.base_url.format(str(i))
            poem_page_urls.append(poem_page_url)
        return poem_page_urls

    # 解析诗人主页
    def parse_poem_page(self, url):
        print('parse_poem_page函数解析--->%s' % url)
        selector = self.selector(url)
        poems = selector.xpath('//div[@class="shirenlist"]//a')
        for poem in poems:
            poemer_item = {}
            poemer_url = 'http://www.shicimingju.com' + poem.xpath('@href')[0]
            poemer = poem.xpath('text()')[0]
            selector = self.selector(poemer_url)
            # 得到诗人作品集的总作品数 构建分页
            zuopins_total = selector.xpath('//div[@class="num"]/b/text()')[0]
            poemer_item['chaodai'] = '唐朝'
            poemer_item['zuopins_total'] = zuopins_total
            poemer_item['poemer_url'] = poemer_url
            poemer_item['poemer'] = poemer
            poemer_item_list.append(poemer_item)
            # 根据 得到诗人作品集的总作品数   得到诗人作品集的每一页请求
            zuopin_page_base_url = poemer_url.replace('.html', '') + '_{}.html'
            divmod_num = [i for i in divmod(int(zuopins_total), 40)]
            pages = divmod_num[0] if divmod_num[1] == 0 else divmod_num[0] + 1
            for page in range(1, int(pages) + 1):
                zuopin_page_url = zuopin_page_base_url.format(page)
                # 请求每一个诗人的page页面 解析得到作品url 作品名称 作者
                self.parse_page_zuopin(zuopin_page_url)

    def parse_page_zuopin(self, url):
        selector = self.selector(url)
        zuopin_pages = selector.xpath('//div[@class="shicilist"]/ul/li[1]/a')
        poemer_url = 'http://www.shicimingju.com' + selector.xpath('//div[@class="shicilist"]/ul/li[2]/a[2]/@href')[0]
        poemer = selector.xpath('//div[@class="shicilist"]/ul/li[2]/a[2]/em/text()')[0]
        for zuopin_page in zuopin_pages:
            item2 = {}
            zuopin_url = 'http://www.shicimingju.com' + zuopin_page.xpath('@href')[0]
            zuopin_name = zuopin_page.xpath('text()')[0]
            print('作者:%s,作者url:%s,作品==>%s,作品url==>%s' % (poemer, poemer_url, zuopin_name, zuopin_url))
            item2['poemer_url'] = poemer_url
            item2['poemer'] = poemer
            item2['zuopin_url'] = zuopin_url
            item2['zuopin_name'] = zuopin_name
            self.parse_zuopin_detail(item2)

    # 解析作品详情页
    def parse_zuopin_detail(self, item):
        print('parse_zuopin_detail函数解析--->%s' % item['zuopin_url'])
        zuopin_item = {}
        selector = self.selector(item['zuopin_url'])
        zuopin_item['poemer'] = item['poemer']
        zuopin_item['poemer_url'] = item['poemer_url']
        zuopin_item['zuopin_name'] = item['zuopin_name']
        zuopin_item['name_words'] = len(item['zuopin_name'])
        zuopin_item['zuopin_url'] = item['zuopin_url']
        try:
            zuopin_content = selector.xpath('//div[@class="shicineirong"]//text()')
            zuopin_item['zuopin_content'] = ''.join([x.strip() for x in zuopin_content])
            zuopin_item['zuopin_words'] = len(zuopin_item['zuopin_content'].replace('，', '').replace('。', ''))
        except:
            zuopin_item['zuopin_content'] = '抓取失败无数据'
            zuopin_item['zuopin_words'] = 0
        print(zuopin_item)
        zuopin_items_list.append(zuopin_item)

    def insert(self, sql, *args):
        cursor.executemany(sql, args)
        conn.commit()

    def main(self):
        get_poem_page_urls = self.get_poem_page_urls()
        for poem_page_url in get_poem_page_urls:
            self.parse_poem_page(poem_page_url)
        poemers = ['chaodai', 'poemer', 'zuopins_total', 'poemer_url']
        poemers_base_sql = 'insert into poemers ({}) values(%s,%s,%s,%s)'
        poemers_sql = poemers_base_sql.format(','.join(poemers))
        args_one = tuple(
            [[item['chaodai'], item['poemer'], item['zuopins_total'], item['poemer_url']] for item in poemer_item_list])
        self.insert(poemers_sql, *args_one)
        zuopins = ['poemer', 'poemer_url', 'zuopin_name', 'name_words', 'zuopin_content', 'zuopin_words', 'zuopin_url']
        zuopin_base_sql = 'insert into poem_zuopin ({}) values(%s,%s,%s,%s,%s,%s,%s)'
        zuopin_sql = zuopin_base_sql.format(','.join(zuopins))
        args_two = tuple([[item['poemer'], item['poemer_url'], item['zuopin_name'], item['name_words'],
                           item['zuopin_content'], item['zuopin_words'], item['zuopin_url']] for item in
                          zuopin_items_list])
        self.insert(zuopin_sql, *args_two)



if __name__ == '__main__':
    poemer_item_list = []
    zuopin_items_list = []
    tangshi = TangShi()
    conn = tangshi.conn
    cursor = conn.cursor()
    tangshi.main()
    cursor.close()
    conn.close()
