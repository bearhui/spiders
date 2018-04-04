
import pymysql


# mysql数据库操作类
class MysqlConn(object):
    def __init__(self,database,table_name):
        self.database = database
        self.table_name = table_name

        self.MYSQLCONFIG = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'password',
            'db': self.database,
            'charset': 'utf8'
        }
        self.conn = pymysql.connect(**self.MYSQLCONFIG)
        self.cursor = self.conn.cursor()

    def execute_sql(self,sql):
        self.cursor.execute(sql)
        self.conn.commit()

    # 建表
    def create_table(self, create_sql):
        self.execute_sql(create_sql)
        info = '{}表创建成功!'.format(self.table_name)
        return info

    # 数据库 实例化后的表的所有字段列表(除主键自增id外) 数据类型 list
    @property  # 方法变属性
    def fields(self):
        try:
            sql = "select * from `{}`.`{}` limit 1".format(self.database,self.table_name)
            self.cursor.execute(sql)
            # 数据表字段的描述
            description = self.cursor.description
            # 数据库字段 由于数据库字典加了主键自增id 抓取的数据里 并没有这个 因此切片从索引1开始
            fields = [x[0] for x in description][1::]
            return fields
        except:
            return None

    def create_sql(self, keys):
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
            else:
                field_strs.append(key + field_str)
        create_sql = head_str + ','.join(field_strs) + foot_sql
        return create_sql.format(self.database,self.table_name,self.table_name)

    # 插入数据的方法 参数为字段列表,数据字典
    def insert_data(self, fields,item):
        fields_num = len(fields)
        # 字段列表 转字符串 用于拼接sql
        str_fields = ','.join(fields)
        # %s占位符列表转字符串 根据字段列表长度 生成 用于拼接sql
        str_fields_num = ','.join(['%s' for i in range(fields_num)])
        insert_sql = 'insert into `{}`.`{}`({}) values({})'.format(self.database,self.table_name,str_fields, str_fields_num)
        print(insert_sql)
        data = []
        for field in fields:
            data.append(item[field])
        args = tuple(data)
        # 批量一次性插入解析之后的全部数据 obi是个生成器 遍历出每一条数据
        self.cursor.execute(insert_sql, args)
        self.conn.commit()
        info = '{}表成功插入1条数据!'.format(self.table_name)
        return info

        # 插入数据的方法 参数为一个生成器对象
    def insert_manydata(self, item):
        fields = self.fields
        fields_num = len(fields)
        # 字段列表 转字符串 用于拼接sql
        str_fields = ','.join(fields)
        # %s占位符列表转字符串 根据字段列表长度 生成 用于拼接sql
        str_fields_num = ','.join(['%s' for i in range(fields_num)])
        insert_sql = 'insert into `{}`.`{}`({}) values({})'.format(self.database, self.table_name, str_fields,str_fields_num)
        # 批量一次性插入解析之后的全部数据
        print(insert_sql)
        self.cursor.executemany(insert_sql,item)
        self.conn.commit()
        info = '{}表成功插入{}条数据!'.format(self.table_name,len(item))
        return info

    def fetch_data(self,sql):
        self.cursor.execute(sql)
        datas = self.cursor.fetchall()
        fields = [key[0] for key in self.cursor.description]
        print(fields, datas)
        items = [dict(zip(fields, data)) for data in datas]
        return items

    def __str__(self):
        return self.MYSQLCONFIG['user']