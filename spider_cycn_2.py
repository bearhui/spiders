# coding:utf-8

import time
import shelve

import xlrd
import xlwt
import requests
from bs4 import BeautifulSoup

from learn_pymysql_module.db import Database



class YuQingData(object):
    """
    中国证券时报舆情数据: url:http://news.capitalink.cn/yqsj.html
    """

    def __init__(self):
        self.con = Database.getConnection()
        self.cur = self.con.cursor()
        self.filename = 'xuewei06_19'
        self.parser()

    def __del__(self):
        self.cur.close()
        self.con.close()

    def parser(self):
        # 获得汇总页内容
        self.get_page_content()

    @staticmethod
    def get_next_url(current_url):
        """
        'http://news1.capitalink.cn/yqsj.html?curpage=1' 变为
        'http://news1.capitalink.cn/yqsj.html?curpage=2'
        """
        url_contents = current_url.split('=')
        next_page_num = int(url_contents[-1]) + 1
        return '='.join([url_contents[0], str(next_page_num)])

    def get_news_url(self, page_url):
        """
        获取页面的链接及部分信息
        :param page_url: 页面地址
        :return: 包含新闻url和其他信息的列表,如果没有返回None
        """
        result = {}

        try:
            response = requests.get(page_url)
        except Exception as e:
            print '抓取网页出现错误,错误为:%s' % e
            return None

        if response.status_code == 200:
            web_content = BeautifulSoup(response.text, 'lxml')
            news_content = web_content.find('div', class_='maj_box_list').find('ul').find_all('li')
            # 获取新闻数据
            for item in news_content:
                item_result = {}
                item_result['_id'] = 'http://news1.capitalink.cn/' + item.a.get('href')
                item_result['url'] = 'http://news1.capitalink.cn/' + item.a.get('href')
                item_result['company'] = item.a.span.string.split(':')[-1][:-1]
                item_result['title'] = item.a.contents[1].split('(')[0]
                item_result['num'] = item.var.string
                item_result['date'] = item.find_all('span')[-1].string
                result.setdefault('data', []).append(item_result)
            if result.get('data', None) is None:
                return None
            # 判断是否还有下一页
            next_url = self.get_next_url(page_url)
            # 如果在页面中,证明一定有下一页,如果则尝试去抓下一页
            if next_url in {item.get('href') for item in web_content.find('div', class_='pagelist').find_all('a')}:
                result['next_url'] = next_url
            else:
                result['next_url'] = None
            return result
        else:
            # 休息10秒
            time.sleep(10)
            return None

    def get_page_content(self):
        """
        解析界面,将界面存在的新闻放入
        :return:
        """
        none_num = 0
        page_url = 'http://news1.capitalink.cn/yqsj.html?curpage=1'
        while none_num < 5:
            result = self.get_news_url(page_url)
            if result is None or result.get('data', None) is None:
                time.sleep(5)
                none_num += 1
                logger.info(u'try: %s, %s' % (none_num, page_url))
                continue
            # 判断下一个页面的url地址
            next_url = result['next_url']
            if next_url is None:
                page_url = self.get_next_url(page_url)
            else:
                page_url = next_url

            # 重新赋值为0
            none_num = 0

            save_name = page_url.split('=')[-1]
            # python对象持久化存储
            # self.save_news_content_shelve(save_name, result['data'])
            # get_news_content_shelve
            # excel存储数据
            # self.save_news_content_excel(save_name, result['data'])
            # self.get_news_content_excel(save_name)
            # 数据库存储数据
            self.save_news_content_mysql('news_zqsb', result['data'])
            self.get_news_content_mysql('news_zqsb')
            break

    def save_news_content_shelve(self, name, data):
        """
        获得新闻的内容,保存在本地
        :return: None
        """
        db = shelve.open(filename=self.filename, writeback=True)
        db[name] = data
        db.close()

    def get_news_content_shelve(self):
        """
        获得新闻的内容,保存在本地
        :return: None
        """
        db = shelve.open(filename=self.filename)
        for key, values in db.items():
            print key, values
        db.close()

    def save_news_content_excel(self, name, data):
        """
        获得新闻的内容,保存在本地
        :return: None
        """
        workbook = xlwt.Workbook()
        sheet = workbook.add_sheet(name)
        title = [u'title', u'id', u'company', u'num', u'date', 'url']
        for i, item in enumerate(title):
            sheet.write(0, i, item)

        data = [item.values() for item in data]

        for row, item in enumerate(data):
            for i, info in enumerate(item):
                sheet.write(row + 1, i, info)

        workbook.save(self.filename + '.xls')

    def get_news_content_excel(self, name):
        """
        获得新闻的内容,保存在本地
        :return: None
        """
        workbook = xlrd.open_workbook(self.filename+'.xls')
        sheet = workbook.sheet_by_name(name)

        for i in range(sheet.nrows):
            print sheet.row_values(i)

    def save_news_content_mysql(self, name, data):
        """
        获得新闻的内容,保存在本地
        :return: None
        """
        # sql = "insert ignore into %s values(%s,%s,%s)"
        sql = "insert ignore into %s values(" % name + "%s," * (len(data[0]) - 1) + "%s)"
        data = [item.values() for item in data]
        self.cur.executemany(sql, tuple(data))
        self.con.commit()

    def get_news_content_mysql(self, name):
        """
        获得新闻的内容,保存在本地
        :return: None
        """
        sql = 'select * from %s' % name
        self.cur.execute(sql)
        for item in self.cur.fetchall():
            print item


if __name__ == '__main__':
    YuQingData()
