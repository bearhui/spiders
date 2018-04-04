#coding:utf-8

import os
import shutil
import requests
from lxml import etree

BASEDIR = r'/Users/chengxinyao/api'

class SpiderShowdocApi(object):
    def __init__(self):
        super(SpiderShowdocApi,self).__init__()
        self.doc_url = r'http://10.11.18.24:4999/index.php?s=/1'
    def parse(self):
        html = requests.get(self.doc_url).content
        selector = etree.HTML(html)
        doc_dirs = selector.xpath('//ul[@class="nav nav-list bs-docs-sidenav"]/li')
        docs = {}
        dirs = []
        for doc_dir in doc_dirs:
            dir = doc_dir.xpath('a[@class="show_cut_title"]/@title')[0]
            docs[dir] = {}
            dirs.append(dir)
            child_dir_urls = doc_dir.xpath('ul[@class="child-ul nav-list hide"]/li/a/@href')
            child_dir_names = doc_dir.xpath('ul[@class="child-ul nav-list hide"]/li/a/text()')
            child_dir = list(zip(child_dir_names,child_dir_urls))
            docs[dir]['childs'] = []
            for i in child_dir:
                data = list(i)
                child_dir = data[0]
                child_item = {}
                page_url =  'http://10.11.18.24:4999' + data[1]
                child_item['child_dir'] = child_dir
                child_item['child_url'] = page_url

                html = requests.get(page_url).content
                selector = etree.HTML(html)
                title = '###' + selector.xpath('//h3[@id="page_title"]/text()')[0]
                contents = ''.join(selector.xpath('//div[@id="page_md_content"]/textarea/text()'))
                child_item['child_contents'] = title + contents
                docs[dir]['childs'].append(child_item)
        return dirs,docs
    def run(self):
        dirs, docs = self.parse()
        os.chdir(BASEDIR)
        for dir in dirs:
            api_markdown_dir = os.path.join(BASEDIR,dir)
            if os.path.exists(api_markdown_dir):
                # 不论目录是否为空都删除
                shutil.rmtree(api_markdown_dir)
                print('已删除 {} 目录'.format(api_markdown_dir))
            os.mkdir(api_markdown_dir)
        for dir in dirs:
            childs = docs[dir]['childs']
            for child in childs:
                child_dir = child['child_dir']
                child_url = child['child_url']
                child_contents = child['child_contents']
                print(child_dir,child_url,child_contents)
                file = os.path.join(BASEDIR,dir,child_dir+'.md')
                with open(file,'w',encoding='utf8') as fw:
                    fw.write(child_contents)




if __name__ == '__main__':
    spider = SpiderShowdocApi()
    spider.run()