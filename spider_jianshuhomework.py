# coding:utf8
import re
import requests
from lxml import etree
from datetime import datetime
from learn_pymysql_module import MysqlConn


class JianshuSpider(MysqlConn):
    def __init__(self,database,table_name,*args,**kwargs):
        super(JianshuSpider,self).__init__(database,table_name)
        print(kwargs)
        self.start_url = kwargs['start_url']
        self.headers = kwargs['headers']

    def parse_page_total(self):
        html = requests.get(self.start_url, headers=self.headers).content
        selector = etree.HTML(html)
        #专题汇总数据 收录了13篇文章 · 25人关注 13 25
        totals = selector.xpath('//div[@class="info"]/text()')[0].strip()
        #作业总数 13篇文章
        job_num = int(re.findall('\d+', totals.split('·')[0])[0])
        #关注粉丝总数 25人关注
        focus_num = int(re.findall('\d+', totals.split('·')[1])[0])
        reg_page_total = divmod(job_num, 9)[0] + 1 if divmod(job_num, 9)[1] != 0 else divmod(job_num, 9)[0]
        for page in range(1, reg_page_total + 1):
            item = {}
            page_url = self.start_url + '?order_by=added_at&page=' + str(page)
            item['req_url'] = self.start_url
            item['job_num'] = str(job_num - 1)
            item['focus_num'] = str(focus_num - 4)
            item['page_url'] = page_url
            yield item

    #文章列表页
    def parse_title_list(self,items):
        for item in items:
            page_url = item['page_url']
            selector = etree.HTML(requests.get(page_url, headers=self.headers).text)
            title_urls = selector.xpath('//a[@class="title"]')
            for title_info in title_urls:
                item1 = {}
                item1.update(item)
                # http://www.jianshu.com/p/6a0d20a57312
                title_url = 'http://www.jianshu.com' + title_info.xpath('@href')[0]
                title = title_info.xpath('text()')[0]
                if 'ython爬虫' in title:
                    # print(full_title_url)
                    item1['title_url'] = title_url
                    item1['title'] = title
                    item1['course_type'] = '爬虫'
                    item1['course_type_id'] = '1'
                    yield item1
                elif 'ython数据处理' in title:
                    # print(full_title_url)
                    item1['title_url'] = title_url
                    item1['title'] = title
                    item1['course_type'] = '数据处理'
                    item1['course_type_id'] = '2'
                    yield item1
                elif 'ython-web开发' in title:
                    # print(full_title_url)
                    item1['title_url'] = title_url
                    item1['title'] = title
                    item1['course_type'] = 'web开发'
                    item1['course_type_id'] = '3'
                    yield item1
                else:
                    pass

    #详情页面
    def parse_title_detail(self,items):

        datas = []
        for item in items:
            title_url = item['title_url']
            print(title_url)
            selector = etree.HTML(requests.get(title_url, headers=self.headers).content)
            js_id = selector.xpath('//div[@class="info"]/span[@class="name"]/a/@href')[0].split('/')[2]
            if js_id != '9104ebf5e177':
                job_name = selector.xpath('//div[@class="article"]/h1[@class="title"]/text()')[0].strip()
                try:
                    js_name = selector.xpath('//div[@class="info"]/span[@class="name"]/a/text()')[0]
                except:
                    js_name = '名字含有非法字符'
                all_span_infos = selector.xpath('//div[@class="meta"]')
                for span in all_span_infos:
                    item2 = {}
                    item2.update(item)
                    item2['js_id'] = js_id
                    item2['js_name'] = js_name
                    pubtime = span.xpath('span[@class="publish-time"]/text()')
                    publish_time = pubtime[0][0:10].replace('.', '-') if len(pubtime)>0 else create_time
                    wd = span.xpath('span[@class="wordage"]/text()')
                    wordage_str = wd[0] if len(wd)>0 else '0'
                    wordage = re.findall('\d+', wordage_str)[0]
                    item2['publish_time'] = publish_time
                    item2['wordage'] = wordage
                    create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    item2['create_time'] = create_time
                    datas.append(item2)
        return datas

    def run(self):
        items = self.parse_page_total()
        item1 = self.parse_title_list(items)
        datas = self.parse_title_detail(item1)

        if not self.fields:
            keys = [key for key in datas[0].keys()]
            #添加主键id
            create_sql = self.create_sql(keys)
            create_info = self.create_table(create_sql)
            print(create_info)

        fields = self.fields
        for data in datas:
            print(data)
            item = {}
            for field in fields:
                item[field] = data[field]
            info = self.insert_data(fields,item)
            print(info)

if __name__ == '__main__':
    start_url = 'https://www.jianshu.com/c/b14a665ac68a'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Cookie': 'remember_user_token=W1szODg4OTk4XSwiJDJhJDEwJC9zNXBVb0lHdG5McUk2eHRZOUJleHUiLCIxNTAyMTU1OTE0LjI1NDg5NCJd--bf55695156794b8d368c4810118f7e5f08839907; _ga=GA1.2.732364892.1502155904; _gid=GA1.2.1185026040.1502155904; _gat=1; Hm_lvt_0c0e9d9b1e7d617b3e6842e85b9fb068=1502168869,1502172839,1502178655,1502178656; Hm_lpvt_0c0e9d9b1e7d617b3e6842e85b9fb068=1502184210; _session_id=TW5nWHl2dmdiUCtaVnNNTGJRYnVJY1pHTlpGdzRIRzJpWmVYK0ZtL1E2RE8xZUU4M3M3N3pXeVJMdUVIY0M2S0dzOXMvU3Y0b0hqeEJwTGhVeHhSWW5ta2NmOFlkQi9XVXJLbFpXbFFxaGFucjVxSlY4Z1R4NStVNDFWbjVBdnFaS0VWMjlPRE9TTk96QW5Nd1h0L3BWVEJlNC9nMEsyd08rU3RoT2NxVmxnK01aeFRqNVE2UTJUbEVZMkk0RUdUSDVPbjNGaEI1NGtmTTIwZVRQcXpIVU9TNllDYW93SFhPZXpnTG80U2NhRE0xY0V6NDFHczIrL3JVSW56MHVJSGNUWlhENElxSjhUdUFPMnJHc1NwOXVnZ3FEQnUrSTJUSGVFTlpqTlpUZ3l2d1ljRjBWMi85bzFGd3greDg2aFZrNTM1L05rUktLNThES01Jdkw3aVIwOW9KYUY3SWliM1JydkwzZWVCTzUyM0VnYmtrZzllZmErRzVRYU1pNWFiNDV2bGhNOU9QMHhybHljUWlXbEFOOGtZV280WGpQREQ5THM0eUhLVEpUcz0tLUI0ZE9VdE05am5oWWhxcFlBemRvZGc9PQ%3D%3D--167db5ba1a872385ec5de99d5347a42ebecf7046'
    }
    database = 'maoyan'
    table_name = 'train_crawl_works'
    jianshu = JianshuSpider(database=database,table_name=table_name,start_url=start_url,headers=headers)
    jianshu.run()
