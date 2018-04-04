import os
import shutil
import requests
from lxml import etree


BASEDIR = os.path.dirname(__file__)


class JiechuSpider(object):
    #产品菜单 url
    def parse_product(self):
        url = 'http://www.ashui1998.com/product/'
        html = requests.get(url).text
        selector = etree.HTML(html)
        infos = selector.xpath('//div[@class="nr"]/h4/a')
        for info in infos:
            product_name = info.xpath('text()')[0]
            product_url = info.xpath('@href')[0]
            product_item = {}
            product_item['product_name'] = product_name
            product_item['product_url'] = product_url
            yield product_item

    #产品菜单下的图片 url 名称 要写入的路径
    def parse_pic(self, product_items):
        for product_item in product_items:
            # 创建存放路径
            picture_dir = product_item['product_name']
            os.chdir(BASEDIR)
            if os.path.exists(picture_dir):
                #不论目录是否为空都删除
                shutil.rmtree(picture_dir)
                print('已删除 {} 目录'.format(picture_dir))
            os.mkdir(picture_dir)
            #解析图片地址
            product_url = product_item['product_url']
            html = requests.get(product_url).text
            selector = etree.HTML(html)
            pics = selector.xpath('//div[@class="pro_main"]/dl[@class="pd_index_dl"]/dt/a')
            for pic in pics:
                pic_url = pic.xpath('img/@src')[0]
                pic_name = pic.xpath('img/@alt')[0]
                pic_item = {}
                pic_item['pic_url'] = pic_url
                pic_item['pic_name'] = pic_name
                pic_item['file'] = os.path.join(BASEDIR, picture_dir, pic_name+'.jpg')
                pic_content = requests.get(pic_url).content
                pic_item['pic_content'] = pic_content
                yield pic_item
    #图片写入对应路径
    def run(self):
        product_items = self.parse_product()
        pic_items = self.parse_pic(product_items)
        for pic_item in pic_items:
            with open(pic_item['file'],'wb') as pic:
                pic.write(pic_item['pic_content'])
            print(pic_item['file'] + '---写入成功')



if __name__ == '__main__':
    jiechu = JiechuSpider()
    jiechu.run()
