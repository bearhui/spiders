import json
import requests
from lxml import etree
import pymysql


class Tsix_Ke(object):
    def __init__(self):
        self.base_url = 'http://36kr.com/api/post?column_id={}&b_id={}&per_page=100'
        self.start_url = 'http://36kr.com/api/post?column_id={}&per_page=1'
        self.req_urls = []
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
            'Cookie': 'aliyungf_tc=AQAAAFxhpiDW5AkAY9x/eymlUoTWlQsi; device-uid=479662c0-b53d-11e7-8bab-5f219bd75d8d; krnewsfrontss=aba699feb21b93859834e12dff97d40a; kr_stat_uuid=GeDX525141100; c_name=point; Hm_lvt_713123c60a0e86982326bae1a51083e1=1508466043,1508466413,1508466484; Hm_lpvt_713123c60a0e86982326bae1a51083e1=1508466484; ktm_ab_test=t.21_v.deploy!t.7_v.deploy!t.9_v.18!t.13_v.25!t.5_v.deploy!t.6_v.deploy; M-XSRF-TOKEN=02dfc6dd8033bec339eecbc2eba6317c1f4bb9d130ab5b4cbee27b930644d8d1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',

        }
        self.MYSQL_CONFIG = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'admin2016',
            'db': 'local_db',
            'charset': 'utf8'
        }
        self.conn = pymysql.connect(**self.MYSQL_CONFIG)
        self.column_ids_dict = {'column_ids': [67, 23, 102, 185, 180, 70, 103],
                                'column_names': ['早期项目', '大公司', '创投新闻', 'AI is', '消费升级', '深氪', '技能Get']}
        self.column_dict = dict(zip(self.column_ids_dict['column_ids'], self.column_ids_dict['column_names']))

    def parse_column_total(self):
        column_ids = self.column_ids_dict['column_ids']
        print(column_ids, self.column_dict)
        for column_id in column_ids:
            self.parse_first_req(column_id)

    def parse_first_req(self, column_id):
        first_url = self.start_url.format(column_id)
        print(first_url)
        html = requests.get(first_url).text
        items = json.loads(html)['data']['items']
        if items:
            last_b_id = items[-1]['id']
            full_url = self.base_url.format(column_id, last_b_id)
            print('解析导航栏:%s-->首页,id-->%s' % (self.column_dict[column_id], column_id))
            self.parse_json(full_url)
        else:
            pass

    def parse_json(self, full_url):
        print("解析组合url--->", full_url)
        self.req_urls.append(full_url)
        html = requests.get(full_url).text
        items = json.loads(html)['data']['items']
        if items:
            for item in items:
                parse_item = item
                parse_item['tag'] = self.column_dict[item['column_id']]
                parse_item['full_url'] = full_url
                self.insert_item(parse_item)
            last_b_id = items[-1]['id']
            column_id = items[-1]['column_id']
            next_req_url = self.base_url.format(column_id, last_b_id)
            print('下一次请求url-->', next_req_url)
            self.parse_json(next_req_url)
        else:
            pass

    def insert_item(self, item):
        full_url = item['full_url']
        column_id = item['column_id'] if 'column_id' in item.keys() else None
        tag = self.column_dict[column_id]
        b_id = item['id'] if 'id' in item.keys() else None
        article_url = 'http://36kr.com/p/{}.html'.format(b_id) if 'id' in item.keys() else None
        title = item['title'] if 'title' in item.keys() else None
        user_id = item['user_id'] if 'user_id' in item.keys() else None
        user_name = item['user']['name'] if 'user' in item.keys() and item['user']['name'] else None
        total_words = item['total_words'] if 'total_words' in item.keys() else None
        close_comment = item['close_comment'] if 'close_comment' in item.keys() else None
        favorite = item['counters']['favorite'] if 'counters' in item.keys() and item['counters']['favorite'] else None
        likes = item['counters']['like'] if 'counters' in item.keys() and item['counters']['like'] else None
        pv = item['counters']['pv'] if 'counters' in item.keys() and item['counters']['pv'] else None
        pv_app = item['counters']['pv_app'] if 'counters' in item.keys() and item['counters']['pv_app'] else None
        pv_mobile = item['counters']['pv_mobile'] if 'counters' in item.keys() and item['counters']['pv_mobile'] else None
        view_count = item['counters']['view_count']if 'counters' in item.keys() and item['counters']['view_count'] else None
        extraction_tags = item['extraction_tags'] if 'extraction_tags' in item.keys() else None
        summary = item['summary'] if 'summary' in item.keys() else None
        title_mobile = item['title_mobile'] if 'title_mobile' in item.keys() else None
        introduction = item['column']['introduction'] if 'column' in item.keys() and item['column']['introduction'] else None
        published_at = item['published_at'] if 'published_at' in item.keys() else None
        created_at = item['created_at'] if 'created_at' in item.keys() else None
        updated_at = item['updated_at'] if 'updated_at' in item.keys() else None
        related_company_id = item['related_company_id'] if 'related_company_id' in item.keys() else None
        related_company_type = item['related_company_type'] if 'related_company_type' in item.keys() else None
        related_company_name = item['related_company_name'] if 'related_company_name' in item.keys() else None
        args = (column_id,tag,b_id,article_url,title,user_id,user_name,total_words,close_comment,favorite,likes,pv,pv_app,pv_mobile,view_count,extraction_tags,summary,title_mobile,introduction,published_at,created_at,updated_at,related_company_id,related_company_type,related_company_name,full_url)
        values_format=",".join(['%s' for i in range(1,len(args)+1)])
        insert_sql = 'insert into 36kr (column_id,tag,b_id,article_url,title,user_id,user_name,total_words,close_comment,favorite,likes,pv,pv_app,pv_mobile,view_count,extraction_tags,summary,title_mobile,introduction,published_at,created_at,updated_at,related_company_id,related_company_type,related_company_name,full_url) values(%s)' % (values_format)
        cursor.execute(insert_sql,args)
        print(args)
        conn.commit()
if __name__ == '__main__':
    _36kr = Tsix_Ke()
    conn = _36kr.conn
    cursor = conn.cursor()
    _36kr.parse_column_total()
    cursor.close()
    conn.close()
    print('共请求%s个URL' % (len(_36kr.req_urls)))

