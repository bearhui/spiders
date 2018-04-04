import os
import re
import shutil
import requests
from lxml import etree





BASEDIR = os.path.dirname(__file__)


class JiechuSpider(object):
    base_url =  'http://www.nsfc.gov.cn'

    #杰出青年url解析
    def parse_items(self):
        url = 'http://www.nsfc.gov.cn/publish/portal0/tab315/'
        req = requests.get(url)
        req.encoding='UTF-8'
        html = req.text
        selector = etree.HTML(html)
        infos = selector.xpath('//span[@class="jjyw"]/a')[2:12]
        for a in infos:
            item = {}
            item['year'] = a.xpath('text()')[0].split('（')[1].replace('）','')
            item['year_url'] = self.base_url + a.xpath('@href')[0]
            yield item


    #产品菜单下的图片 url 名称 要写入的路径
    def parse_module_page(self, items):
        for item in items:
            #解析图片地址
            year_url = item['year_url']
            req = requests.get(year_url)
            req.encoding = 'UTF-8'
            html = req.text
            selector = etree.HTML(html)
            #分页url构造
            module_info = selector.xpath('//a[@class="Normal"]/@href')[0].replace('.htm','')
            module_base_url = self.base_url + module_info[0:len(module_info)-1]+'{}.htm'
            total_record = int(selector.xpath('//td[@class="Normal"]/text()')[0].split(':')[1].split(',')[0])
            total_page = divmod(total_record,12)[0]+1 if divmod(total_record,12)[1] else divmod(total_record,12)[0]
            year_dir = item['year']
            os.chdir(BASEDIR)
            if os.path.exists(os.path.join(BASEDIR,year_dir)):
                # 不论目录是否为空都删除
                shutil.rmtree(os.path.join(BASEDIR,year_dir))
                print('已删除 {} 目录'.format(os.path.join(BASEDIR,year_dir)))
            #创建父目录
            os.mkdir(year_dir)

            for page in range(1,total_page+1):
                # 创建存放路径
                os.chdir(os.path.join(BASEDIR,year_dir))
                picture_dir = os.path.join(BASEDIR,year_dir,str(page))
                if os.path.exists(picture_dir):
                    # 不论目录是否为空都删除
                    shutil.rmtree(picture_dir)
                    print('已删除 {} 目录'.format(picture_dir))
                #创建子目录
                os.mkdir(picture_dir)
                module_item = {}
                module_item.update(item)
                module_page_url = module_base_url.format(page)
                module_item['module_page_url'] = module_page_url
                module_item['picture_dir'] = picture_dir
                yield module_item
    #图片数据
    def parse_page_pic(self,module_items):
        for module_item in module_items:
            #print(module_item)
            module_page_url = module_item['module_page_url']
            picture_dir = module_item['picture_dir']
            req = requests.get(module_page_url)
            req.encoding = 'UTF-8'
            html = req.text
            selector = etree.HTML(html)
            name_infos = selector.xpath('//span[@class="jjyw"]/a')
            pic_infos = selector.xpath('//td[@align="center"]/a')
            item2 = {}
            for pic_info in pic_infos:
                name_url1 = pic_info.xpath('@href')[0]
                pic_url1 =  pic_info.xpath('img/@src')[0]
                item2[name_url1] = pic_url1
            for name_info in name_infos:
                name_url =  name_info.xpath('@href')[0]
                name = name_info.xpath('text()')[0].replace('　','')
                pic_item = {}
                pic_item.update(module_item)
                pic_item['name_url'] = name_url
                pic_item['name'] = name
                pic_item['pic_url'] = self.base_url + item2[name_url]
                pic_item['file'] = os.path.join(picture_dir,name+'.jpg')
                req_pic = requests.get(pic_item['pic_url'])
                req_pic.encoding = 'UTF-8'
                pic_item['pic_content'] = req_pic.content
                yield pic_item


    def run(self):
        items = self.parse_items()
        module_items = self.parse_module_page(items)
        pic_items = self.parse_page_pic(module_items)
        for pic_item in pic_items:
            print(pic_item)

            with open(pic_item['file'],'wb') as pic:
                pic.write(pic_item['pic_content'])
            print(pic_item['file'] + '---写入成功')



if __name__ == '__main__':
    jiechu = JiechuSpider()
    jiechu.run()
