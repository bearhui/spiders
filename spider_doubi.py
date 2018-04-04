import requests
import json
from lxml import etree
import csv
#获取问题
headers={
 'user-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
'cookie':'__cfduid=da8fa4f2ba6d41e243608f08f5fe8b6691504768603; cf_clearance=51a8c9b7449ea0560717f8daa28cd747c43b6337-1504768608-300; Hm_lvt_8067e5ade532c4f3711b320f1e88f213=1504768609; Hm_lpvt_8067e5ade532c4f3711b320f1e88f213=1504768617; Hm_lvt_26499fa0b99ff63dd9867c0c43ec1a9d=1504768609; Hm_lpvt_26499fa0b99ff63dd9867c0c43ec1a9d=1504768617'

}
url = 'https://doub.bid/sszhfx/'
html = requests.get(url,headers=headers).text
selector = etree.HTML(html)
problem = selector.xpath('//p[@id="problem"]/text()')[0]
print(problem)
post_url=selector.xpath('//div[@class="passwd_form"]/form/@action')[0]
print(post_url)
result='路人皆知'
#网页王源 form action='' 也就是post请求地址
#构造post提交的数据
post_data = {
"post_password":result
}
s=requests.session()
# #会话对象中带cookies提交数据 获取远吗
html = s.post(post_url,data=post_data).text
print(html)
#
# selector = etree.HTML(html)
# tr_num = len(selector.xpath('//table/tbody/tr'))
# print(tr_num)
# ip_title=selector.xpath('//table/tbody/tr[1]/th/text()')[0:7]
# print(ip_title)
# with open('ip_info1.csv','w',encoding = 'utf-8',newline = '') as file:
#     writer = csv.writer(file)
#     for i in range(2,tr_num):
#         ip_infos_all = selector.xpath('//table/tbody/tr[%s]/td//text()' % str(i))
#         ip_infos_list = ip_infos_all[0:6]
#         ip_infos_list.append('-'.join(ip_infos_all[6:8]))
#         if len(ip_infos_list) == 7:
#             print(ip_infos_list)
#             writer.writerow(ip_infos_list)
#         else:
#             continue
