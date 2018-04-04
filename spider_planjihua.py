import requests
from lxml import etree

from learn_pymysql_module import MysqlConn



class PlanSpider(MysqlConn):

    base_url = 'http://www.1000plan.org/wiki/'
    page_url = base_url + 'index.php?category-view-13-{}'

    #杰出青年url解析
    def parse_all_url(self):
        for page in range(1,13):
            url = self.page_url.format(page)
            yield url

    def parse_item(self,urls):
        items = []
        for url in urls:
            print(url)
            req = requests.get(url)
            req.encoding='UTF-8'
            html = req.text
            selector = etree.HTML(html)
            dls = selector.xpath('//dl[@class="col-dl"]')
            for dl in dls:
                item = {}
                item['req_url'] = url
                item['name'] = dl.xpath('dt/a/text()')[0]
                name_url = self.base_url + dl.xpath('dt/a/@href')[0]
                item['name_url'] = name_url
                item['content'] = self.parse_name(name_url)
                item['create_citiao'] = dl.xpath('dd[1]/a/text()')[0]
                item['create_citiao_url'] = self.base_url + dl.xpath('dd[1]/a/@href')[0]
                item['create_time'] = dl.xpath('dd[1]/text()')[1].replace('创建时间:','')
                item['tags'] = ','.join(dl.xpath('dd[2]/a/text()'))
                item['summary'] = dl.xpath('dd[3]/p/text()')[0]
                item['summary_url'] = self.base_url + dl.xpath('dd[3]/p/a/@href')[0]
                item['edit_count'] = dl.xpath('dd[4]/text()')[0].split('|')[0].replace('编辑:','').replace('次','')
                item['view_count'] = dl.xpath('dd[4]/text()')[0].split('|')[1].strip().replace('浏览:', '').replace('次', '')
                items.append(item)
        return items
    def parse_name(self,url):
        req = requests.get(url)
        req.encoding = 'UTF-8'
        html = req.text
        selector = etree.HTML(html)
        contents = selector.xpath('//div[@class="content_topp"][2]//text()')
        content = ''.join(contents)
        return content.strip()


    def create_sql(self,keys):
        keys.insert(0, 'id')
        id_str = 'id int(11) NOT NULL AUTO_INCREMENT'
        field_str = ' varchar(255) DEFAULT NULL'
        head_str = '''
                USE `{}`;
                DROP TABLE IF Exists `{}`;
                CREATE TABLE `{}`(
            '''
        foot_sql = '''
              ,PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

            '''
        field_strs = []
        for key in keys:
            if key == 'id':
                field_strs.append(id_str)
            elif key == 'summary' or key == 'content':
                field_strs.append(key + field_str.replace('varchar(255)', 'text'))
            else:
                field_strs.append(key + field_str)
        create_sql = head_str + ','.join(field_strs) + foot_sql
        return create_sql

    def run(self):
        urls = self.parse_all_url()
        items = self.parse_item(urls)
        keys = [key for key in items[0].keys()]
        create_sql = self.create_sql(keys)
        info = self.create_table(create_sql.format(self.database,self.table_name,self.table_name))
        print(info)
        all_data = []
        for item in items:
            print(item)
            data = []
            for key in keys[1::]:
                data.append(item[key])
            all_data.append(data)
        if all_data:
            self.insert_manydata(all_data)


if __name__ == '__main__':
    database = 'local_db'
    table_name = 'planspider'
    plan = PlanSpider(database,table_name)
    plan.run()
