import itchat



class ItchatSpider(object):

    def login(self):

        itchat.auto_login(enableCmdQR=2)

    def get_friends(self):
        friends = itchat.get_friends()
        return friends
    def get_qun(self):
        mpsList = itchat.get_chatrooms(update=True)[1:]
        return mpsList



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
            else:
                field_strs.append(key + field_str)
        create_sql = head_str + ','.join(field_strs) + foot_sql
        return create_sql

    def run(self):
        self.login()
        mpsList = self.get_qun()
        for i in mpsList:
            for k,v  in i.items():
                print(k,v)

        # friends = self.get_friends()
        # print(friends[0])
        # keys = [ 'UserName', 'City', 'DisplayName', 'PYQuanPin', 'Province', 'KeyWord','PYInitial', 'NickName', \
        #  'HeadImgUrl', 'SnsFlag', 'MemberCount', 'OwnerUin', 'ContactFlag', 'Uin', 'StarFriend', \
        #  'WebWxPluginSwitch', 'HeadImgFlag']
        # print(keys)
        # create_sql = self.create_sql(keys).format(self.database,self.table_name,self.table_name)
        # info = self.create_table(create_sql)
        # print(info)
        # for friend in friends:
        #     data = {}
        #     for key in keys[1::]:
        #         data[key] = friend.get(key,None)
        #     try:
        #         self.insert_data(data)
        #     except:
        #         print(data)




if __name__ == '__main__':
    database = 'local_db'
    table_name = 'itchatspider'
    it = ItchatSpider()
    it.run()