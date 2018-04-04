#coding:utf-8

import time
import requests
from bs4 import BeautifulSoup
import re
import json



#忽略InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/latest/security.html
#InsecureRequestWarning)错误
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class CheckTicket(object):
    def __init__(self, fro, to, date):
        self.fro = fro
        self.to = to
        self.date = date
        self.base_url = 'https://kyfw.12306.cn/otn/lcxxcx/query?purpose_codes=ADULT&queryDate={0}&from_station={1}&to_station={2}'

    def main(self):
        self.get_content()

    #获取城市字母
    def get_letter(self):
        f = open('station_info.json')
        station_info = json.loads(f.read())
        tmp_fro = station_info.get(self.fro)
        tmp_to = station_info.get(self.to)
        if tmp_fro and tmp_to:
            f.close()
            return (tmp_fro, tmp_to)
        else:
            print ('输入地址有误')
            f.close()

    #请求链接返回json
    def get_content(self):
        # try:
        fro, to = self.get_letter()
        #存一份, 之后监控余票有用
        self.fro, self.to = fro, to

        target_url = self.base_url.format(self.date, fro, to)
        r = requests.get(target_url, verify=False)
        if r.status_code == 200:
            res = dict(r.json())['data']['datas']
            self.p_content(res)
        else:
            print ('请求失败')
        # except Exception, e:
        #     print '查询错误', e
        #     return

    #负责在终端格式化输出
    def p_content(self, res):
        row = PrettyTable()
        row.field_names = ['车次', '出发站/到达站', '出发/到达时间', '历时', '商务座', '一等座', '二等座', '软卧', '硬卧', '硬座', '无座']
        for info in res:
            ticket_infos = [
                info['station_train_code'],
                '\n'.join([info['from_station_name'], info['to_station_name']]),
                '\n'.join([info['start_time'], info['arrive_time']]),
                info['lishi'],
                info['swz_num'],
                info['zy_num'],
                info['ze_num'],
                info['rw_num'],
                info['yw_num'],
                info['yz_num'],
                info['wz_num'],
            ]
            row.add_row(ticket_infos)
            row.add_row(['--------','------------','------------','--------','--------','--------','--------','--------','--------','--------','--------'])
        print (row)
        print ('\n')

        #询问是否需要监控余票
        brief = input('需要监控车次余票并通知么(y/n)?')
        if brief == 'y':
            print ("""
                商务座 : swz_num \n
                一等座 : zy_num \n
                二等做 : ze_num \n
                软卧   : rw_num \n
                硬卧   : yw_num \n
                硬座   : yz_num \n
            """
                   )
            select_train_info = input('输入车次信息和席次(G118 zy_num): ').upper().split(" ")
            self.target_mail = input('接收邮箱: ')
            self.train_num = select_train_info[0]
            self.seat = select_train_info[1].lower()

            self.set_interval()
        else:
            return


    #控制循环时间
    def set_interval(self):
        check_nums = 1
        while 1:
            symbol = self.check_ticket()
            if symbol == None:
                print ('第 %d 次查询'%check_nums)
                check_nums += 1
                time.sleep(60)
                continue
            else:
                break
        print('结束')


    def check_ticket(self):
        target_url = self.base_url.format(self.date, self.fro, self.to)
        r = requests.get(target_url, verify=False)
        if r.status_code == 200:
            results = dict(r.json())['data']['datas']
            #把该车次的所有信息输出
            train_info = [res for res in results if res['station_train_code'] == self.train_num]
            ticket_num = train_info[0][self.seat]

            if ticket_num != "--" and ticket_num != '无':
                #有票的话发送邮件
                content = '{} 车次有 {} 票 {}张'.format(self.train_num, self.seat, ticket_num)
                sendMail.send_mail(self.target_mail, content)
                return 'done'
            else:
                return None





if __name__ == "__main__":
    p_input = input('输入信息, 如:(北京 深圳 20161010): ').split(' ')
    fro = p_input[0]
    to = p_input[1]
    date = p_input[2]
    date = '-'.join([date[:4], date[4:6], date[6:]])
    a = CheckTicket(fro, to, date)
    a.main()