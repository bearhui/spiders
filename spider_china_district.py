import requests
from lxml import etree
import pymysql


class DistrictSpider(object):
    def __init__(self,table_name):
        self.table_name = table_name
        self.MYSQLCONFIG = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'admin2016',
            'db': 'local_db',
            'charset': 'utf8'
        }
        self.conn = pymysql.connect(**self.MYSQLCONFIG)
        self.cursor = self.conn.cursor()

    #执行sql
    def execute_sql(self,sql):
        self.cursor.execute(sql)
        self.conn.commit()

    # 建表
    def create_table(self,create_sql):
        self.execute_sql(create_sql)
        info = '{}表创建成功!'.format(self.table_name)
        return info

    # 数据库cases_infos表的所有字段列表(除主键id外) 数据类型 list
    @property  # 方法变属性
    def fields(self):
        sql = 'select * from `{}`'.format(self.table_name)
        self.cursor.execute(sql)
        # 数据表字段的描述
        description = self.cursor.description
        # 数据库字段 由于数据库字典加了主键自增id 抓取的数据里 并没有这个 因此切片从索引1开始
        fields = [x[0] for x in description][1::]
        return fields

    # 插入数据的方法 参数为一个生成器对象
    def insert_data(self, datas):
        fields = self.fields
        fields_num = len(fields)
        # 字段列表 转字符串 用于拼接sql
        str_fields = ','.join(fields)
        # %s占位符列表转字符串 根据字段列表长度 生成 用于拼接sql
        str_fields_num = ','.join(['%s' for i in range(fields_num)])
        insert_sql = 'insert into `{}`({}) values({})'.format(self.table_name,str_fields, str_fields_num)
        print(insert_sql)
        # 批量一次性插入解析之后的全部数据 obi是个生成器 遍历出每一条数据
        self.cursor.executemany(insert_sql,datas)
        self.conn.commit()


    req_url = 'http://www.stats.gov.cn/tjsj/tjbz/xzqhdm/201703/t20170310_1471429.html'
    headers = {
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'
    }

    def parse(self):
        req= requests.get(self.req_url,headers = self.headers)
        req.encoding = 'utf8'
        html = req.text
        selector = etree.HTML(html)
        infos = selector.xpath('//div[@class="TRS_PreAppend"]/p[@class="MsoNormal"]')
        print(len(infos))
        datas =  []
        for info in infos:
            pros = info.xpath('b/span/text()')
            citys = [j for j in [x.replace('\u3000','') for x in info.xpath('span/text()')] if j]
            if len(pros)==2:
                pro_id = pros[0][:2]
                city_id = None
                pros.append(pro_id)
                pros.append(city_id)
                pros.append('省')
                datas.append(tuple(pros))

            if len(citys)==2:
                pro_id = citys[0][:2]
                city_id = citys[0][:4]
                citys.append(pro_id)
                citys.append(city_id)
                if citys[0].endswith('00'):
                    citys.append('市')
                else:
                    citys.append('区')
                datas.append(tuple(citys))
        datas.append(('460000', '海南省','46',None, '省'))
        datas.append(('500000', '重庆市', '50',None,'省'))
        return datas
    def add_field(self):
        add_sql = '''
#添加省字段
alter table `{}` add province_name varchar(255) after district_num;
#添加市名称字段
alter table `{}` add city_name varchar(255) after province_name;
#添加时间字段
alter table `{}` add create_time varchar(255) after city_name;
'''
        self.execute_sql(add_sql.format(self.table_name,self.table_name,self.table_name))

    def update_data(self):

        # 更新省字段
        update_province_sql = '''
update `{}` as a join 
(select district_num,area,area_type,pro_id
from `{}`
where area_type = '省' 
order by district_num
) as b on a.pro_id = b.`pro_id`
set a.province_name = b.area;
'''
        self.execute_sql(update_province_sql.format(self.table_name, self.table_name))
        # 更新市字段
        update_city_sql = '''
update `{}` as a join 
(select district_num,area,area_type,city_id
from `{}`
where area_type = '市' and district_num like '%00'
order by district_num
) as b on a.city_id = b.`city_id`
set a.city_name = b.area,create_time = sysdate();
'''
        self.execute_sql(update_city_sql.format(self.table_name,self.table_name))



    def run(self):
        datas = self.parse()
        print(datas)
        self.insert_data(datas)
        self.add_field()
        self.update_data()




if __name__ == '__main__':
    table_name = 'dim_district2'
    ds = DistrictSpider(table_name)
    create_sql = '''
DROP TABLE IF Exists `{}`;
    CREATE TABLE `{}` (
id int(11) auto_increment comment"主键id",

  `district_num` int(11) DEFAULT NULL,
  `area` varchar(255) DEFAULT NULL,
  pro_id int(11) comment"省id",
  city_id int(11) comment"市id",
  area_type varchar(50)comment"省市区分",
 
  primary key(id),
  key(district_num),
  key(area)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''
    info = ds.create_table(create_sql.format(table_name,table_name))
    print(info)
    ds.run()