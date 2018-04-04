#coding:utf8

import json
import requests
import time


class SuningSpider(object):

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
        'Cookie': '你的cookie',
        'Content-Type':'json/plain;charset=UTF-8',
        'Referer':'https://licai.suning.com/lcportal/portal/bill/list.htm'
    }

    base_url = 'https://licai.suning.com/lcportal/portal/bill/productList.htm?ajax=true&type=ticket&loanPeriod=&incomeRate=&sortName=&sortType=&pageIndex=%s&_={}'
    start_url = base_url % (1)

    def parse_urls(self):
        urls = []
        tms = str(time.time()).replace('.', '')[:13]
        data = requests.get(self.start_url.format(tms), headers=self.headers).json()
        totalDataCount = int(data['lists']['totalDataCount'])
        print(totalDataCount)
        for i in range(1,totalDataCount+1):
            urls.append(self.base_url % (str(i)))
        return urls

    def run(self):
        urls = self.parse_urls()
        print(urls)
        for url in urls:
            tms = str(time.time()).replace('.', '')[:13]
            datas = requests.get(url.format(tms), headers=self.headers).json()['lists']['datas']
            for item in datas:
                for k, v in item.items():
                    print(k, v)


if __name__ == '__main__':
    suning = SuningSpider()
    suning.run()

