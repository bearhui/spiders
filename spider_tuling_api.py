import json
import requests
from learn_pymysql_module import MysqlConn


class TuLingSpider(MysqlConn):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
        'Cookie': 'UM_distinctid=1603616d3af1ad-0f96cd8d4070fe-17396d57-13c680-1603616d3b0b99; gr_user_id=7ec67ecc-ec99-4e5f-a749-bfd87e3d707b; gr_session_id_a879f15d23a9b5f0=5d3aff2e-7c2c-4cf9-bcbc-acff76265e65; login-token=BfVxWx-DeYRLEsCOVF8U1UJaq3LOgWlOuBOj9y0wlPJ1e_7hr5H2Njdklt-BqxroptKo7XkkR3WLib8DgIK68XYCMXHSL_rgk8hxv__8EWSD9CYfl8r9IcdV9uB0Gzo19bXVnYR8XJRfBRC7m5N966hBWj0-XBINtRbSRStgr2-OIZ5bTQRVHpHGbc1wuGfI9SeV34kVeh-XVTvBBJFEQ1yJze-tw9hfhRXfpMbhXAZWGlBCOk7LFLmJn9HmH0bYxmuPX0zAhFZJHC38-9cUtVoA6XqPtSHeHzFOOtXoIZ_xY4Q4rC_J2OlfKZHpUAYGcEQ6KbH7NE2fUycLymJ4l4RR2OyCuSJCg2p7Q_vP1u5t4MaepEa4AwLP1Vkyyirrget9gG6_KRfEt4OlbjwS63fIOI6jo-6t2IW3QhzVvvCxYfC9qkd-YVWlCAYM5jCAcvA.'
    }
    base_url = 'https://datacenter.tuling123.com/log/get?memberId=615837&apikey=2e5b581e1aa7448ea64d4c89fdb115a4&type=7&startDate=&endDate=&seachType=all&pageNum={}&moduleid=&userid=&keywords=&reverse=true&_=1513674373231'

    start_url = 'https://datacenter.tuling123.com/log/get?memberId=615837&apikey=2e5b581e1aa7448ea64d4c89fdb115a4&type=7&startDate=&endDate=&seachType=all&moduleid=&userid=&keywords=&reverse=true&_=1513674373231'

    def parse_totalPages(self):
        json_data = requests.get(self.start_url, headers=self.headers).json()
        totalPages = int(json_data['totalPages'])
        for i in range(1, 14):
            if i==2:
                pass
            else:
                yield self.base_url.format(i)

    def get_item_field(self):
        url = self.base_url.format(1)
        contents = self.parse_contents(url)[0]
        keys = [key for key in contents.keys()]
        return keys

    def parse_contents(self, url):
        json_data = requests.get(url, headers=self.headers).json()
        contents = json_data['content']
        return contents

    @property
    def create_sql(self):
        keys = self.get_item_field()
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
            elif key == 'answer' or key == 'question':
                field_strs.append(key + field_str.replace('varchar(255)','text'))
            else:
                field_strs.append(key + field_str)
        create_sql = head_str + ','.join(field_strs) + foot_sql
        return create_sql

    def run(self):
        fields = self.get_item_field()
        all_urls = self.parse_totalPages()
        all_data = []
        for url in all_urls:
            contents = self.parse_contents(url)
            if contents:
                for content in contents:
                    data = []
                    for field in fields:
                        value = content.get(field,None)
                        data.append(value)
                    all_data.append(data)
        if all_data:
            print(all_data)
            self.insert_manydata(all_data)
            return True
        else:
            return None


if __name__ == '__main__':
    database = 'local_db'
    table_name = 'tuling_api'
    tuling = TuLingSpider(database,table_name)
    create_sql = tuling.create_sql.format(database,table_name,table_name)
    create_info = tuling.create_table(create_sql)
    print(create_info)
    info = tuling.run()
    print(info)
