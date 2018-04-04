#coding=utf-8
from selenium import webdriver
from bs4 import BeautifulSoup
from xvfbwrapper import Xvfb
import pymysql
from selenium.webdriver.chrome.options import Options
from PIL import Image
import time
import datetime
import json
import redis
import base64

def save_logs(task_id, user_id ,status, logs ):
    conn = pymysql.connect(host='localhost', port=3306,
                           user='root',
                           passwd='admin2016',
                           db='local_db',
                           charset='utf8')

    cur = conn.cursor()
    sql = "Insert into " \
          "`spiders_log`(" \
          " `task_id`,`user_id` ,`status` ,`mid` ,`create_time`,`logs`, `spider_name`) " \
          "VALUES " \
          "(%s,%s,%s,%s,%s,%s,%s)"
    mid = '10.8.48.226'
    spider_name = 'PBC'
    create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    cur.execute(sql, (task_id, user_id, status, mid, create_time, logs, spider_name))
    print "日志入库成功"
    conn.commit()
    cur.close()
    conn.close()
#消息队列函数
def task():
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0)
    r = redis.StrictRedis(connection_pool=pool)
    try:
        task = r.lpop('TASK_PROCESS_QUEUE_READY_HANDLE_ALIPAY')
    except:
        task = None
    return task
#链接redis函数
def connect():
    pool = redis.ConnectionPool(host='10.8.48.235', port=6379, db=10, password='bigdata')
    r = redis.StrictRedis(connection_pool=pool)
    return r
# #资源减一函数
# def  resource_cut():
#     print '资源数减一'
#     pool = redis.ConnectionPool(host='10.8.48.235', port=6379, db=10, password='bigdata')
#     r = redis.StrictRedis(connection_pool=pool)
#     print '资源数：', r.decr('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY')
# #资源加一函数
# def resource_add():
#     print '资源数加一'
#     pool = redis.ConnectionPool(host='10.8.48.235', port=6379, db=10, password='bigdata')
#     r = redis.StrictRedis(connection_pool=pool)
#     print '资源数：', r.incr('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY')
#获取二维码函数
def element(taskId,driver,r):
    try:
        print "taskId:", taskId
        path = '{}.png'.format(taskId)
        driver.save_screenshot(path)
        driver.get_screenshot_as_file(path)
        img = Image.open(path)
        region = img.crop((710, 220, 840, 350))
        region.save(path)
        f = open('{}.png'.format(taskId), 'rb')  # 二进制方式打开图文件
        ls_f = base64.b64encode(f.read())  # 读取文件内容，转换为base64编码
        print ls_f
        f.close()
        c = r.hget('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId)
        print '正在传送的二维码信息', c
        print type(c)
        a = json.loads(c, encoding='utf-8')
        print a
        dicts={}
        a['content']=dicts
        dicts['content'] = ls_f
        dicts['format'] = 'png'
        a["status"] = 121
        a['statusDesc'] = '获取成功'
        dicts['createdTime'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        dicts['expirationTime'] = (datetime.datetime.now() + datetime.timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")

        print '二维码信息已修改'
        a1 = json.dumps(a).encode('utf-8')
        print 111111, a1
        r.hset('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId, a1)
        print '二维码信息已传送'

    except Exception as e8:
        try:
            print e8
            print '二维码未传送'
            pool = redis.ConnectionPool(host='10.8.48.235', port=6379, db=10, password='bigdata')
            r = redis.StrictRedis(connection_pool=pool)
            c6 = r.hget('ali_qrcode_init', taskId)
            print '失败的二维码：', c6
            print type(c6)
            a9 = json.loads(c6, encoding='utf-8')
            print a9
            a9['content']['format'] = 'png'
            a9["status"] = 141
            a9['statusDesc'] = '获取失败'
            a7 = json.dumps(a9).encode('utf-8')
            print 111111, a7
            r.hset('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId, a7)
            print '二维码失败信息已上传'
        except:
            driver.quit()
            xvfb.stop()
#登录状态检测函数
def login_test(driver):
    print driver.find_element_by_xpath("//*[@id='globalUser']/span").text.replace(" ", "").split("")[
        0].decode().encode('utf-8')
    print '用户已扫码'
#花呗额度抓取函数
def huabai(driver):
    try:
        driver.find_element_by_xpath("//*[@id='global-header-area']/div[2]/a").click()
    except:
        pass

    print'抓取花呗额度'
    try:
        edu = driver.find_element_by_xpath(
            '//*[@id="J-assets-pcredit"]/div/div/div[2]/div/p[2]/strong').text
        print '花呗额度:', edu
        r=connect()
        o1 = r.hget('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId)
        b1 = json.loads(o1)
        b1['content']['edu'] = edu
        b2 = json.dumps(b1).encode('utf-8')
        r.hset('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId, b2)

    except:
        edu = 0
        r = connect()
        o1 = r.hget('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId)
        b1 = json.loads(o1)
        b1['content']['edu'] = edu
        b2 = json.dumps(b1).encode('utf-8')
        r.hset('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId, b2)
    print '花呗额度:',edu
    return edu
#抓取名字函数
def name(driver,taskId):
    try:
        print '开始抓取名字'
        mingzi = driver.find_element_by_xpath("//*[@id='globalUser']/span").text.replace(" ", "").split("")[
            0].decode().encode('utf-8')
        print mingzi, type(mingzi)
        pool = redis.ConnectionPool(host='10.8.48.235', port=6379, db=10, password='bigdata')
        r = redis.StrictRedis(connection_pool=pool)
        o = r.hget('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId)
        b = json.loads(o)
        dicts={}
        b['content']=dicts
        dicts['userName']=mingzi
        b['status'] = 123
        b['statusDecs']='登录成功'
        b['statusCode']='LOGIN_SUCCEED'
        b1 = json.dumps(b).encode('utf-8')
        r.hset('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId, b1)
    except Exception as u:
        print '错误！！！！', u
        pool = redis.ConnectionPool(host='10.8.48.235', port=6379, db=10, password='bigdata')
        r = redis.StrictRedis(connection_pool=pool)

        o = r.hget('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId)
        b = json.loads(o)
        b['status'] = 144
        b['content']['username'] = ''
        b1 = json.dumps(b).encode('utf-8')
        r.hset('ali_qrcode_login_status', taskId, b1)
    return mingzi
#爬虫程序
def zhua(taskId,driver,mingzi,edu,comId):
    try:
        driver.find_element_by_xpath("//*[@id='globalContainer']/div[2]/div/div[1]/div/ul/li[2]/a").click()
        print '点击交易记录'
    except:
        pass

    try:
        driver.find_element_by_xpath("//*[@id='content']/div[1]/div[2]/div/div[1]/a").click()
        print '转变为交易流水高级版'
    except:
        pass

    driver.find_element_by_id("J-datetime-select")
    print '扫码成功，开始抓取'
    try:
        driver.find_element_by_xpath("//*[@id='J-datetime-select']/a[1]").click()
    except:
        pass
    print '开始点击前六个月'
    driver.find_element_by_xpath("//li[@data-value='customDate']").click()
    driver.find_element_by_xpath("//*[@id='beginDate']").click()
    js = 'document.getElementById("beginDate").removeAttribute("readonly");'
    driver.execute_script(js)
    driver.find_element_by_xpath("//*[@id='beginDate']").clear()
    now_time = datetime.datetime.now()
    yes_time = now_time + datetime.timedelta(days=-180)
    yes_time_nyr = yes_time.strftime('%Y.%m.%d')
    print yes_time_nyr
    driver.find_element_by_xpath("//*[@id='beginDate']").send_keys(yes_time_nyr)

    driver.find_element_by_xpath("//*[@id='J-state-select']/a[1]").click()

    print '11111111六个月11111111'
    time.sleep(1)
    driver.find_element_by_id("J-set-query-form").click()  # 点击搜索
    print '点击搜索'
    time.sleep(2)

    while True:
        conn = pymysql.connect(host='10.10.1.20', port=3306, user='usertest', passwd='Usertest@123', db='dbty',
                               charset='utf8')
        cur = conn.cursor()
        # sql = "Replace into `alipay_test`(`taskID`，`zhanghao`,`create_time`,`create_detailtime`,`name`,`number`,`duifang`,`duifangname`,`money`,`status`,`action`,`detail_link`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        print'连接数据库成功'
        try:
            print'22222222222'
            driver.find_element_by_xpath("//a[@class='page-next page-trigger']")

            print'找到下一页'
            html = driver.page_source
            print '得到页面'
            soup = BeautifulSoup(html, 'html.parser')
            print'解析成功'
            liushui_list = soup.find_all('tr')
            print'找到列表'
            for i in liushui_list[1:]:
                print '1111111111111第12345678...页111111111111'
                print taskId

                create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                updata_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                try:
                    # 交易创建时间
                    create_detailtime1 = i.find('td', class_='time').get_text().replace('\n', ' ').replace('\t',
                                                                                                           '').replace(
                        '\r', '').replace('.', '-').strip() + ':00'
                    create_detailtime = datetime.datetime.strptime(create_detailtime1, "%Y-%m-%d  %H:%M:%S")

                except:
                    create_detailtime = None
                try:
                    # 交易名称
                    name = i.find('td', class_='name').get_text().replace('\n', '').replace(' ', '').replace('\t',
                                                                                                             '').replace(
                        '\r', '')
                except:
                    name = None
                try:
                    number = i.find('td', class_='tradeNo ft-gray').get_text().replace('\n', '').replace(' ',
                                                                                                         '').replace(
                        '\t', '').replace('\r', '')
                    if "|" in number:
                        number1 = number.split('|')[1].split(":")[1]  # 交易号
                        number2 = number.split('|')[0].split(":")[1]  # 订单号
                        # print number1
                        # print number2
                    else:
                        number1 = number.split(":")[1]
                        number2 = None
                except:
                    number1 = None
                    number2 = None
                try:
                    duifang = i.find('p', class_='name').get_text().replace('\n', '').replace(' ', '').replace('\t',
                                                                                                               '').replace(
                        '\r', '')
                except:
                    duifang = None
                try:
                    duifangname = i.find('p', class_='no ft-gray').get_text().replace('\n', '').replace(' ',
                                                                                                        '').replace(
                        '\t', '').replace('\r', '')
                except:
                    duifangname = None
                try:
                    money = i.find('td', class_='amount').get_text().replace('\n', '').replace(' ', '').replace('\t',
                                                                                                                '').replace(
                        '\r', '')
                except:
                    money = None
                try:
                    status = i.find('td', class_='status').get_text().replace('\n', '').replace(' ', '').replace('\t',
                                                                                                                 '').replace(
                        '\r', '')
                except:
                    status = None
                try:
                    action = i.find('span', class_='item-text').get_text().replace('\n', '').replace(' ', '').replace(
                        '\t', '').replace('\r', '')
                except:
                    action = None
                try:
                    detail_link = i.find('option', attrs={'data-target': '_blank'}).get('data-link')
                except:
                    detail_link = None
                platform='ALIPAY'
                print'全部找到'
                print taskId, mingzi, create_time, create_detailtime, name, number, duifang, duifangname, money, status, action, detail_link, comId
                try:
                    if create_detailtime == None:
                        pass
                    elif number1 == None:
                        pass
                    elif mingzi == None:
                        pass
                    else:
                        sql = "Insert ignore into `per_third_payment_flow`(`task_id`,`per_uni_code`,`per_name`,`created_at`,`updated_at`,`traded_at`,`trade_desc`,`trade_id`,`order_id`,`dest_count_id`,`dest_count_name`,`amount`,`trade_type`,`action`,`detail_link`,`limit`,`platform`) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"

                        sql2 = sql % (
                        taskId, comId, mingzi, create_time, updata_time, create_detailtime, name, number1, number2,
                        duifang, duifangname, money, status, action, detail_link, edu,platform)
                        cur.execute(sql2)
                        conn.commit()
                except Exception as e:
                    print e
                print'数据已入库'

            driver.find_element_by_xpath("//a[@class='page-next page-trigger']").click()


        except:
            print '11111111111111最后一页1111111111111111'
            page = driver.page_source
            content = BeautifulSoup(page, 'html.parser')
            liushui_list1 = content.find_all('tr')
            for z in liushui_list1[1:]:
                create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

                updata_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                try:
                    # 交易创建时间
                    create_detailtime1 = z.find('td', class_='time').get_text().replace('\n', ' ').replace('\t',
                                                                                                           '').replace(
                        '\r', '').replace('.', '-').strip() + ':00'
                    create_detailtime = datetime.datetime.strptime(create_detailtime1, "%Y-%m-%d  %H:%M:%S")

                except:
                    create_detailtime = None
                try:
                    # 交易名称
                    name = z.find('td', class_='name').get_text().replace('\n', '').replace(' ', '').replace('\t',
                                                                                                             '').replace(
                        '\r', '')
                except:
                    name = None
                try:
                    number = z.find('td', class_='tradeNo ft-gray').get_text().replace('\n', '').replace(' ',
                                                                                                         '').replace(
                        '\t', '').replace('\r', '')
                    if "|" in number:
                        number1 = number.split('|')[1].split(':')[1]  # 交易号
                        number2 = number.split('|')[0].split(':')[1]  # 订单号
                        print number1
                        print number2
                    else:
                        number1 = number.split(':')[1]
                        number2 = None
                except:
                    number1 = None
                    number2 = None
                try:
                    duifang = z.find('p', class_='name').get_text().replace('\n', '').replace(' ', '').replace('\t',
                                                                                                               '').replace(
                        '\r', '')
                except:
                    duifang = None
                try:
                    duifangname = z.find('p', class_='no ft-gray').get_text().replace('\n', '').replace(' ',
                                                                                                        '').replace(
                        '\t', '').replace('\r', '')
                except:
                    duifangname = None
                try:
                    money = z.find('td', class_='amount').get_text().replace('\n', '').replace(' ', '').replace('\t',
                                                                                                                '').replace(
                        '\r', '')
                except:
                    money = None
                try:
                    status = z.find('td', class_='status').get_text().replace('\n', '').replace(' ', '').replace('\t',
                                                                                                                 '').replace(
                        '\r', '')
                except:
                    status = None
                try:
                    action = z.find('span', class_='item-text').get_text().replace('\n', '').replace(' ', '').replace(
                        '\t', '').replace('\r', '')
                except:
                    action = None
                try:
                    detail_link = z.find('option', attrs={'data-target': '_blank'}).get('data-link')
                except:
                    detail_link = None
                print'全部找到'
                platform='ALIPAY'
                print taskId, mingzi, create_time, create_detailtime, name, number, duifang, duifangname, money, status, action, detail_link, comId
                try:
                    if create_detailtime == None:
                        pass
                    elif number1 == None:
                        pass
                    elif mingzi == None:
                        pass

                    else:
                        sql = "Insert ignore into `per_third_payment_flow`(`task_id`,`per_uni_code`,`per_name`,`created_at`,`updated_at`,`traded_at`,`trade_desc`,`trade_id`,`order_id`,`dest_count_id`,`dest_count_name`,`amount`,`trade_type`,`action`,`detail_link`,`limit,`platform`) VALUES ('%s',,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"

                        sql2 = sql % (
                        taskId, comId, mingzi, create_time, updata_time, create_detailtime, name, number1, number2,
                        duifang, duifangname, money, status, action, detail_link, edu,platform)
                        cur.execute(sql2)
                        conn.commit()

                except Exception as e:
                    print e
                # print create_time, create_detailtime, name, number, duifang, duifangname, money, status, action, detail_link
                print '数据已入库'
            break
    time.sleep(1)
    driver.quit()
    cur.close()
    conn.close()

while True:
    taskId = task()
    if taskId:
        st = time.time()
        print '接收到任务', taskId
        # resource_cut()
        xvfb = Xvfb(width=1280, height=720)
        xvfb.start()
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-setuid-sandbox")
        driver = webdriver.Chrome('/usr/bin/chromedriver', chrome_options=chrome_options)
        # driver=webdriver.Chrome()
        while 1:
            # try:
                r = connect()
                print r
                a = r.hget('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId)
                print a
                b = json.loads(a)
                c = b.get('status')
                if c==184:
                    r.lpush('TASK_PROCESS_QUEUE_HANDLE_RESPONSE',taskId)
                    print '~~~~~~用户取消~~~~~~'
                    r.hdel('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId)
                    driver.quit()
                    xvfb.stop()
                    break
                elif c==101:
                    driver.get('https://my.alipay.com/portal/i.htm?referer=https%3A%2F%2Fauth.alipay.com%2Flogin%2FhomeB.htm%3FredirectType%3Dparent')
                    s=time.time()
                    # driver.find_element_by_xpath('//*[@id="J-loginMethod-tabs"]/li[1]').click()
                    while 1:
                        t2_time = time.time()
                        if t2_time - s > 15:
                            break
                        else:
                            pass
                        try:
                            driver.find_element_by_xpath("//*[@id='J-loginMethod-tabs']/li[1]").click()#点击扫码登录
                            break
                        except:
                            time.sleep(1)
                    element(taskId,driver,r)
                    r.lpush('TASK_PROCESS_QUEUE_HANDLE_RESPONSE',taskId)
                    while 1:
                        e=time.time()
                        try:
                            print '！！！！判断用户是否扫码！！！！'
                            try:
                                driver.find_element_by_xpath('//*[@id="global-header-area"]/div[2]/a').click()#转换为个人版
                            except:pass
                            print driver.find_element_by_xpath('//*[@id="globalUser"]').text
                            # driver.find_element_by_xpath("//*[@id='globalUser']/span").text.replace(" ", "").split("")[
                            #     0].decode().encode('utf-8')
                            print '～～～～用户已经扫码～～～～'
                            edu=huabai(driver)
                            mingzi=name(driver,taskId)
                            print '<<<<<<',edu,mingzi,'>>>>>>>'
                            r.lpush('TASK_PROCESS_QUEUE_HANDLE_RESPONSE',taskId)
                            break
                        except:
                            time.sleep(1)
                            print '等待用户扫码'
                            r = connect()
                            a6 = r.hget('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId)
                            b5 = json.loads(a6)
                            c2 = b5.get('status')
                            if c2==184:
                                break
                            elif e-s>600:
                                driver.quit()
                                xvfb.stop()
                                break
                elif c==181:
                    r=connect()
                    q=r.hget('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId)
                    s=json.loads(q)
                    # mingzi=s.get('content').get('username')
                    # edu=s.get('content').get('edu')
                    comId=s.get('perUniCode')
                    print '~~~~~~',mingzi,edu,comId,'~~~~~~'
                    try:
                        zhua(taskId, driver,mingzi,edu,comId)
                    except Exception as e2:
                        print '@@@@@@@@@@@@@@@@',e2



                    conn = pymysql.connect(host='10.10.1.20', port=3306, user='usertest', passwd='Usertest@123',
                                           db='dbty',
                                           charset='utf8')
                    cur = conn.cursor()
                    sql = 'SELECT COUNT(*) from per_third_payment_flow WHERE task_id=%s'%taskId
                    cur.execute(sql)
                    flow1=cur.fetchone()
                    flow=flow1[0]
                    print '记录条数：',flow
                    conn.commit()
                    cur.close()
                    conn.close()
                    i1 = r.hget('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId)
                    d7 = json.loads(i1)
                    dict1={}
                    d7['content']=dict1
                    dict1['flowCount']=flow
                    d7['status'] = 124
                    d7['message'] = '抓取成功'
                    d5 = json.dumps(d7).encode('utf-8')
                    r.hset('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId, d5)
                    r.lpush('TASK_PROCESS_QUEUE_HANDLE_SUCCEED',taskId)
                    # resource_add()
                    break
                else:
                    time.sleep(1)
                    et = time.time()
                    if et - st > 600:
                        print'操作超时'
                        i1 = r.hget('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId)
                        d7 = json.loads(i1)
                        d7['status'] = 170
                        d7['message'] = '抓取失败'
                        d5 = json.dumps(d7).encode('utf-8')
                        r.hset('TASK_PROCESS_QUEUE_HANDLIND_ALIPAY', taskId, d5)
                        # resource_add()
                        # status = 0
                        # logs = '操作超时'
                        # comId = 11
                        # save_logs(taskId, comId, status, logs)
                        break
                    else:
                        pass
                    print '状态检测中...'


    else:
        time.sleep(1)
        print '等待申请中'