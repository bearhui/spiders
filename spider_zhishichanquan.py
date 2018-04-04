from datetime import datetime
import requests
from lxml import etree
import pymysql

# 根据抓取的数据 建表sql脚本
"""
CREATE DATABASE zhishichanquan;
USE zhishichanquan;
CREATE TABLE `cases_infos` (
`id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id自增',
`tio` text COMMENT '案件标题',
`cid` varchar(255) DEFAULT NULL COMMENT '案件id',
`ridn` varchar(255) DEFAULT NULL COMMENT '决定号',
`ridd` varchar(255) DEFAULT NULL COMMENT '决定日',
`riapo` varchar(255) DEFAULT NULL COMMENT '请求人',
`rilb` varchar(255) DEFAULT NULL COMMENT '法律依据',
`ano` varchar(255) DEFAULT NULL COMMENT '申请号',
`ans` varchar(255) DEFAULT NULL COMMENT '申请号2',
`ad` varchar(255) DEFAULT NULL COMMENT '申请日',
`ridt` varchar(255) DEFAULT NULL COMMENT '复审决定',
`pk` varchar(255) DEFAULT NULL COMMENT '专利类型',
`ridv` varchar(255) DEFAULT NULL COMMENT '复审决定结果',
`case_url` varchar(255) DEFAULT NULL COMMENT '案件url',
`req_url` varchar(255) DEFAULT NULL COMMENT '请求url接口',
`page` varchar(255) DEFAULT NULL COMMENT '请求页数',
`create_time` varchar(255) DEFAULT NULL COMMENT '抓取时间',
PRIMARY KEY (`id`),
KEY `cid` (`cid`),
KEY `ridn` (`ridn`),
KEY `create_time` (`create_time`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

"""


# mysql数据库操作类
class MysqlConn(object):
    def __init__(self):
        self.MYSQLCONFIG = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'admin2016',
            'db': 'zhishichanquan',
            'charset': 'utf8'
        }
        self.conn = pymysql.connect(**self.MYSQLCONFIG)
        self.cursor = self.conn.cursor()

    # 数据库cases_infos表的所有字段列表(除主键id外) 数据类型 list
    @property  # 方法变属性
    def fields(self):
        sql = 'select * from cases_info'
        self.cursor.execute(sql)
        # 数据表字段的描述
        description = self.cursor.description
        # 数据库字段 由于数据库字典加了主键自增id 抓取的数据里 并没有这个 因此切片从索引1开始
        fields = [x[0] for x in description][1::]
        return fields

    # 插入数据的方法 参数为一个生成器对象
    def insert_data(self, obj):
        fields = self.fields
        fields_num = len(fields)
        # 字段列表 转字符串 用于拼接sql
        str_fields = ','.join(fields)
        # %s占位符列表转字符串 根据字段列表长度 生成 用于拼接sql
        str_fields_num = ','.join(['%s' for i in range(fields_num)])
        insert_sql = 'insert into cases_info({}) values({})'.format(str_fields, str_fields_num)
        print(insert_sql)
        # 批量一次性插入解析之后的全部数据 obi是个生成器 遍历出每一条数据
        self.cursor.executemany(insert_sql, (i for i in obj))
        self.conn.commit()
        self.cursor.execute('select max(id) from cases_info')
        num = self.cursor.fetchall()[0][0]
        info = '抓取入库结束 共抓取了{}条数据'.format(num)
        return info

    def __str__(self):
        return self.MYSQLCONFIG['user']


# 爬虫类 requests.session() 模拟登录>>登录之后方可进行抓包>>请求>>解析数据>>存储入库
class ZldsjSpider(object):
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.login_data = {
            'username': self.username,
            'password': self.password,
        }
        # 请求的接口 基本字典

        self.form_data = {
            'select-key:expressCN': '((( % )))',
            'select-key:express': '',
            'select-key:thesaurus': '',
            'select-key:cross': '',
            'select-key:searchType': '',
            'select-key:buttonItem': '',
            'select-key:expressCN2Val': '',
            'select-key:expressCN2': ' ',
            'select-key:_keyWord': '名称',
            'select-key:_keyWordStr': '名称',
            'select-key:languageSelect': '',
            'attribute-node:patent_cache-flag': 'false',
            'attribute-node:patent_page-row': '50',
            'attribute-node:patent_sort-column': '-RELEVANCE',

        }
        #请求的接口 url
        self.record_ajax_url = 'http://search.zldsj.com/txnDecisionListRecord.ajax'

    # 登录 requests.session()模拟登录 登录之后才可以抓
    @property
    def login(self):
        session = requests.session()
        #分析登录情况 找到的接口
        login_url = 'http://search.zldsj.com/txn999999.do'
        data = session.post(login_url, data=self.login_data, timeout=None).text
        #print(session.cookies)
        return session

    # 解析请求接口(传递form_data字典)返回的response(xml文件) 返回xpath解析的selector对象
    def selector(self, form_data):
        html = self.login.post(self.record_ajax_url, data=form_data).content
        selector = etree.HTML(html)
        return selector

    # 返回总的form表单提交数据 生成器 获取总页数 用于翻页
    def get_all_form_data(self):
        form_data = self.form_data
        form_data['attribute-node:patent_start-row'] = 1
        form_data['attribute-node:patent_page'] = 1
        selector = self.selector(form_data)
        info = selector.xpath('//attribute-node/patent_record-number/text()')
        record_total_num = int(info[0]) if len(info) > 0 else 0
        pages = divmod(record_total_num, 50)
        # 获取总页数
        pages_num = pages[0] + 1 if pages[1] else pages[0]
        for patent_page in range(1, pages_num + 1):
            start_row = (patent_page - 1) * 50 + 1
            form_data = self.form_data
            form_data['attribute-node:patent_start-row'] = str(start_row)
            form_data['attribute-node:patent_page'] = str(patent_page)
            yield form_data

    # 返回案件数据 字典 生成器对象 case_items = self.make_make_case_items(form_data)
    def make_case_items(self, form_datas):
        for form_data in form_datas:
            selector = self.selector(form_data)
            infos = selector.xpath('//patent')
            page = form_data['attribute-node:patent_page']
            print('{}页 infos有 {} 条数据'.format(page, len(infos)))
            infos = selector.xpath('//patent')
            # 后来知道 超过200页没有数据
            if int(page) <= 200:
                for info in infos:
                    # 案件标题
                    tio = info.xpath('tio/text()')[0] if info.xpath('tio/text()') else None
                    # 案件id
                    cid = info.xpath('cid/text()')[0]
                    # 决定号
                    ridn = info.xpath('ridn/text()')[0]
                    # 决定日
                    ridd = info.xpath('ridd/text()')[0] if info.xpath('ridd/text()') else None
                    # 请求人
                    riapo = info.xpath('riapo/text()')[0] if info.xpath('riapo/text()') else None
                    # 法律依据
                    rilb = info.xpath('rilb/text()')[0] if info.xpath('rilb/text()') else None
                    # 申请号
                    ano = info.xpath('ano/text()')[0] if info.xpath('ano/text()') else None
                    # 申请号2
                    ans = info.xpath('ans/text()')[0] if info.xpath('ans/text()') else None
                    # 申请日
                    ad = info.xpath('ad/text()')[0] if info.xpath('ad/text()') else None
                    # 复审决定
                    ridt = info.xpath('ridt/text()')[0] if info.xpath('ridt/text()') else None
                    # 专利类型
                    pk = info.xpath('pk/text()')[0] if info.xpath('pk/text()') else None
                    # 复审决定结果
                    ridv = info.xpath('ridv/text()')[0] if info.xpath('ridv/text()') else None
                    # 用于详情页面解析的url 登录之后方可打开该url
                    case_url = 'http://search.zldsj.com/txnDecisionDetail.do?select-key:ID={}&select-key:RIDN={}'.format(cid, ridn)
                    item = {}
                    item['tio'] = tio
                    item['cid'] = cid
                    item['ridn'] = ridn
                    item['ridd'] = ridd
                    item['riapo'] = riapo
                    item['rilb'] = rilb
                    item['ano'] = ano
                    item['ans'] = ans
                    item['ad'] = ad
                    item['ridt'] = ridt
                    item['pk'] = pk
                    item['ridv'] = ridv
                    item['case_url'] = case_url
                    item['req_url'] = self.record_ajax_url
                    item['page'] = page
                    item['create_time'] = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
                    yield item
            else:
                break

    # 返回 解析所有的xml文件后的 全部数据生成器
    def make_datas(self):
        form_datas = self.get_all_form_data()
        items = self.make_case_items(form_datas)
        for item in items:
            # 和数据库的字段 映射绑定 用于写入数据库
            data = []
            for field in fields:
                data.append(item[field])
            yield data

    # 调度 启动 入库
    def run(self):
        obj = self.make_datas()
        info = db.insert_data(obj)
        return info

    def __str__(self):
        return self.username


if __name__ == '__main__':
    db = MysqlConn()
    conn = db.conn
    cursor = db.cursor
    fields = db.fields
    print(fields)
    username = 'wuhandong'
    password = 'wuhandong'
    zldsjspider = ZldsjSpider(username, password)
    print(zldsjspider)
    info = zldsjspider.run()
    print(info)
