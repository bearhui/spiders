import requests
from lxml import etree

root_url = 'https://www.huxiu.com'
post_data={
    'ids':''
}
headers={
'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'

}
html=requests.post(root_url,data=post_data,headers=headers).text
selector=etree.HTML(html)
zixuninfos=selector.xpath('//ul[@class="header-column header-column1 header-column-zx menu-box"]/li/a')
for info in zixuninfos:
    catId=info.xpath('@href')[0].replace('/channel/','').replace('.html','')
    channel_name=info.xpath('text()')[0]
    print(channel_name,)
    post_url='https://www.huxiu.com/channel/ajaxGetMore'
    page=1
    post_data={
    'huxiu_hash_code':'18f3ca29452154dfe46055ecb6304b4e',
    'page':page,
    'catId':catId
    }
    data=requests.post(post_url,data=post_data,headers=headers).text
    import json
    json_data=json.loads(data)
    page_total=int(json_data['data']['total_page'])
    for i in range(2,page_total+1):
        page=i
        data = requests.post(post_url, data=post_data, headers=headers).text
        json_data = json.loads(data)
        page_total = int(json_data['data']['total_page'])
        print('获取频道:%s-%s页成功 总页数:%s' % (channel_name,i,page_total))


