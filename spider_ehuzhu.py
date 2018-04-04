#!/usr/bin/env python
#coding:utf-8
import sys
import os
import requests
import datetime
from sdutil.SqlHelper import SqlHelper
from sdutil.date_util import get_before_days_str

class EhuzhuSpider(object):
    event_list_data = []
    headers = {
        'cookie':'JSESSIONID=AF7DB446525CE362DBC860A836761F0B; __DAYU_PP=ejFzQJarmFf2Z7FzzFeN59f5074b4f30; Hm_lvt_def38382520283f3c13e8e9264411354=1522215904; Hm_lpvt_def38382520283f3c13e8e9264411354=1522291059',

        'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',

    }

    update_sqls = []
    event_detail_data = []
    person_info_list = []
    update_plan_sqls = []
    person_info_images_list = []
    #解析列表页
    def parse_event_list(self,start,num=3):
        event_list_api = 'https://wx.ehuzhu.com/ehuzhu2/noticeGS/getNewEachGSList.do?start={}&num={}'
        event_list_url = event_list_api.format(start,num)
        post_data = {'start':start,'num':num}
        print(event_list_url, start,post_data)
        data = requests.post(event_list_url,data=post_data,headers=self.headers).json()
        try:
            events = data['list']
            if not len(events):
               return None
            else:
                for event in events:
                    eventTime = event['eventTime']
                    eventlists = event['eventlist']
                    for eventitem in eventlists:
                        item = {}
                        item['eventTime'] = eventTime
                        # 解析数据
                        item['actualRaiseAmount'] = eventitem.get('actualRaiseAmount', None)
                        item['actualRaisePeople'] = eventitem.get('actualRaisePeople', None)
                        item['avg'] = eventitem.get('avg', None)
                        item['caseNum'] = eventitem.get('caseNum', None)
                        item['per'] = eventitem.get('per', None)
                        item['productId'] = eventitem.get('productId', None)

                        item['productName'] = eventitem.get('productName', None)
                        item['raiseAmount'] = eventitem.get('raiseAmount', None)
                        item['raisePeople'] = eventitem.get('raisePeople', None)
                        item['req_url'] = event_list_url
                        self.event_list_data.append(item)
                start += num
                print(start)
                self.parse_event_list(start,num)
        except:
            print('异常无数据')
            raise Exception


    #列表页 的互助计划 存入plan_person表
    def parse_plan_person(self,autoid,eventTime,productId):

        detail_event_api = 'https://wx.ehuzhu.com/ehuzhu2/noticeGS/getGSDetail.do?eventTime={}&productId={}'
        url = detail_event_api.format(eventTime,productId)
        print '取出第{}条数据，接口为:{}'.format(autoid,url)
        post_data = {
            'eventTime':eventTime,
            'productId':productId
        }
        eventitem = requests.post(url,data=post_data,headers=self.headers).json()
        req_url = url
        try:
            if eventitem:
                # 解析数据
                actualRaiseAmount = eventitem.get('actualRaiseAmount', None)
                actualRaiseRate = eventitem.get('actualRaiseRate', None)
                avg = eventitem.get('avg', None)
                endEventTime = eventitem.get('endEventTime', None)
                eventDescription = eventitem.get('eventDescription', None)
                eventTime = eventitem.get('eventTime', None)

                productName = eventitem.get('productName', None)
                raiseAmount = eventitem.get('raiseAmount', None)
                raisePeople = eventitem.get('raisePeople', None)
                transferDescription = eventitem.get('transferDescription', None)
                transferTime = eventitem.get('transferTime', None)
                data_items = eventitem.get('list', None)
                if data_items:
                    for item in data_items:
                        #获得互助计划人
                        case = {}
                        case['actualRaiseAmount'] = actualRaiseAmount
                        case['actualRaiseRate'] = actualRaiseRate
                        case['avg'] = avg
                        case['endEventTime'] = endEventTime
                        case['eventDescription'] = eventDescription
                        case['eventTime'] = eventTime
                        case['productName'] = productName
                        case['raiseAmount'] = raiseAmount
                        case['raisePeople'] = raisePeople
                        case['transferDescription'] = transferDescription
                        case['transferTime'] = transferTime
                        #获捐人互助金额
                        case['case_actualRaiseAmount'] = item.get('actualRaiseAmount',None)
                        case['diseaseName'] = item.get('diseaseName',None)
                        case['eventNo'] = item.get('eventNo', None)
                        case['headPath'] = item.get('headPath', None)
                        case['memberId'] = item.get('memberId', None)
                        case['province'] = item.get('province', None)
                        case['case_raiseAmount'] = item.get('raiseAmount', None)
                        case['realName'] = item.get('realName', None)
                        case['req_url'] = req_url
                        self.event_detail_data.append(case)

                else:
                    case = {}
                    case['actualRaiseAmount'] = actualRaiseAmount
                    case['actualRaiseRate'] = actualRaiseRate
                    case['avg'] = avg
                    case['endEventTime'] = endEventTime
                    case['eventDescription'] = eventDescription
                    case['eventTime'] = eventTime
                    case['productName'] = productName
                    case['raiseAmount'] = raiseAmount
                    case['raisePeople'] = raisePeople
                    case['transferDescription'] = transferDescription
                    case['transferTime'] = transferTime
                    case['req_url'] = req_url
                    self.event_detail_data.append(case)
                key_dict = {'autoid': autoid}
                self.update_sqls.append(key_dict)
        except:
            print '第{}条数据，接口:{}解析失败'.format(autoid, url)

    #详情页解析 分为2个表
    def parse_detail(self,memberId):
        print '已从 ehuzhu_notice_plan_person 取到memberId:{}'.format(memberId)
        api = 'https://wx.ehuzhu.com/ehuzhu2/noticeGS/detail.do?memberId={}'.format(memberId)
        datas = requests.post(api,headers=self.headers).json()
        try:
            mainObj = datas['mainObj']
            keys = [k for k in mainObj.keys()]
            #添加其他字段信息 以便入库和数据库字段映射
            keys.append('name')
            keys.append('proName')
            keys.append('raiseRate')
            keys.append('startTime')
            keys.append('joinTime')
            keys.append('eventStartTime')
            #建表语句
            # create_sql = "create table if not exists ehuzhu_notice_person_info(autoid int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',"
            # columns = []
            # for k in keys:
            #     if 'cription' in k or 'Process' in k:
            #         col = k + ' text'
            #     else:
            #         col = k + ' varchar(255)'
            #     columns.append(col)
            # create_sql += ','.join(columns)
            # create_sql += ',  PRIMARY KEY (`autoid`),KEY `memberId` (`memberId`)) ENGINE=InnoDB  DEFAULT CHARSET=utf8;'
            mainObj['name'] = datas.get('name',None)
            mainObj['proName'] = datas.get('proName', None)
            mainObj['raiseRate'] = datas.get('raiseRate', None)
            mainObj['startTime'] = datas.get('startTime', None)
            mainObj['joinTime'] = datas.get('joinTime', None)
            mainObj['eventStartTime'] = datas.get('eventStartTime', None)
            mainObj['req_url'] = api
            self.person_info_list.append(mainObj)
            #材料图片
            imageList = datas['imageList']
            for image in imageList:
                image['memberId'] = memberId
                self.person_info_images_list.append(image)
            key_dict = {'memberId': memberId}
            self.update_plan_sqls.append(key_dict)
        except:
            print 'member_id:{}-->接口:{}解析失败'.format(memberId,api)

    #run函数
    def run(self):
        db = SqlHelper(mode="spy")
        db.db.autocommit(True)
        buffer = 'https://wx.ehuzhu.com/ehuzhu2/noticeGS/getNewEachGSList.do?start='
        table_ehuzhu_notice_list = 'ehuzhu_notice_list'
        get_pre_start = 'select replace(replace(req_url,"{}",""),"&num=3","") from {} order by autoid desc limit 1'.format(buffer,table_ehuzhu_notice_list)
        st = db.query(get_pre_start)
        if not st:
            start = 0
        else:
            start = int(st[0][0])
        print('ehuzhu_notice_list 上一次请求的url为:{}{}&num=3'.format(buffer,start))
        num = 3
        # 抓取日期
        day_key = get_before_days_str(0)
        print('day_key:', day_key)
        datas = self.parse_event_list(start)
        event_list_data = self.event_list_data
        if not event_list_data:
            pass
        else:
            #抓取列表页入库
            for item  in event_list_data:
                print(item)
                eventTime, productId = item['eventTime'],item['productId']
                #判断请求的接口返回的数据是否存在数据库中
                check_exists_sql = 'select eventTime,productId from {} where eventTime="{}" and productId="{}"'.format(table_ehuzhu_notice_list,eventTime,productId)
                is_exists = db.exists(check_exists_sql)
                if is_exists:
                    print('数据:{} \n 已存在!不插入'.format(item))
                    pass
                else:
                    item['day_key'] = day_key
                    db.insertMap(table=table_ehuzhu_notice_list,my_dict=item)

        #取上一步存入数据库中的数据取参数eventTime,productId 传入到新的api请求 解析各产品的互助计划
        get_param_sql = 'select autoid,eventTime,productId from {} where status=0'.format(table_ehuzhu_notice_list)
        results = db.query(get_param_sql)
        if results:
            #传递到parse_plan_person函数 如果请求接口返回了数据 将list表中的记录 status更新为1
            for autoid,eventTime,productId in results:
                self.parse_plan_person(autoid,eventTime,productId)
            update_sqls = self.update_sqls
            # 更新状态
            my_dict = {'status': 1}
            for key_dict in update_sqls:
                db.updateMap(table=table_ehuzhu_notice_list, key_dict=key_dict, my_dict=my_dict)
            print table_ehuzhu_notice_list + '更新了{}条状态'.format(len(update_sqls))
            self.update_sqls = []
        else:
            pass

        event_detail_data = self.event_detail_data
        table_ehuzhu_notice_plan_person = 'ehuzhu_notice_plan_person'
        if event_detail_data:
            for item in event_detail_data:
                print(item)
                memberId = item['memberId']
                check_exists_sql = 'select memberId from {} where memberId="{}"'.format(table_ehuzhu_notice_plan_person,memberId)
                is_exists = db.exists(check_exists_sql)
                if is_exists:
                    print('数据:{} \n 已存在!不插入'.format(item))
                    pass
                else:
                    item['day_key'] = day_key
                    db.insertMap(table=table_ehuzhu_notice_plan_person, my_dict=item)
        else:
            pass

        #抓取个人获助详情   写入受助人信息表ehuzhu_notice_plan_person
        get_member = 'select memberId from {} where status=1 group by memberId'.format(table_ehuzhu_notice_plan_person)
        memberIds = db.query(get_member)
        table_ehuzhu_notice_detail_person_info = 'ehuzhu_notice_detail_person_info'
        if memberIds:
            for memberId in memberIds:
                self.parse_detail(memberId[0])
            update_plan_sqls = self.update_plan_sqls
            # 从数据库去完参数后更新状态为已抓取
            my_dict = {'status': 1}
            for key_dict in update_plan_sqls:
                db.updateMap(table=table_ehuzhu_notice_plan_person, key_dict=key_dict, my_dict=my_dict)
            print table_ehuzhu_notice_plan_person + '更新了{}条状态'.format(len(update_plan_sqls))
            self.update_plan_sqls = []
            person_info_list = self.person_info_list
            if not person_info_list:
                pass
            else:
#                 keys = [k for k in person_info_list[0].keys()]
#                 create_sql = "create table if not exists {}(autoid int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',".format(person_table)
#                 columns = []
#                 for k in keys:
#                     if 'cription' in k or 'Process' in k:
#                         col = k + ' text'
#                     else:
#                         col = k + ' varchar(255)'
#                     columns.append(col)
#                 create_sql += ','.join(columns)
#                 other_col = r''',`req_url` varchar(255) DEFAULT NULL COMMENT '请求接口',
#   `day_key` varchar(255) NOT NULL DEFAULT '抓取的日期',
#   `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
#   `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
#   `status` tinyint(4) DEFAULT '0' COMMENT '抓取状态',PRIMARY KEY (`autoid`),KEY `memberId` (`memberId`)) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
# '''
#                 create_sql += other_col
#                 print(create_sql)
#
                for mainObj in person_info_list:
                    print(mainObj)
                    memberId = mainObj['memberId']
                    check_exists_sql = 'select memberId from {} where memberId="{}" '.format(table_ehuzhu_notice_detail_person_info, memberId)
                    is_exists = db.exists(check_exists_sql)
                    if is_exists:
                        print('数据:{} \n 已存在!不插入'.format(mainObj))
                        pass
                    else:
                        mainObj['day_key'] = day_key
                        db.insertMap(table=table_ehuzhu_notice_detail_person_info, my_dict=mainObj)

            #材料图片入库
            person_info_images_list = self.person_info_images_list
            table_ehuzhu_notice_detail_images = 'ehuzhu_notice_detail_images'
            if not person_info_images_list:
                pass
            else:
                for image in person_info_images_list:
                    eventNo,id= image['eventNo'],image['id']
                    check_exists_sql = 'select eventNo,id from {} where eventNo="{}" and id="{}"'.format(table_ehuzhu_notice_detail_images, eventNo,id)
                    is_exists = db.exists(check_exists_sql)
                    if is_exists:
                        print('数据:{} \n 已存在!不插入'.format(image))
                        pass
                    else:
                        image['day_key'] = day_key
                        db.insertMap(table=table_ehuzhu_notice_detail_images, my_dict=image)


if __name__ == '__main__':
    ehuzhu = EhuzhuSpider()
    ehuzhu.run()

