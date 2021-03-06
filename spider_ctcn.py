import requests
from lxml import etree
import csv


def get_all_url():
    urls = []
    for i in range(1, 3):
        page_url = 'http://data.stcn.com/list/djsj_%s.shtml' % i
        urls.append(page_url)
    return urls


def parse_url(url):
    print(url)
    result = []
    print(requests.get(url, headers=headers).status_code)
    if requests.get(url, headers=headers).status_code == 200:
        response = requests.get(url, headers=headers)
        print(response.text)
        selector = etree.HTML(response.text)
        web_content = selector.xpath('//p[@class="tit"]')
        print(web_content)
        for news in web_content:
            item_result = {}
            item_result['href'] = news.xpath('a/@href')[0]
            item_result['title'] = news.xpath('a/text()')[0]
            item_result['date'] = news.xpath('span/text()')[0]
            print(item_result)
            result.append(item_result)
            print(result)
    return result


def save_news_csv(result):
    for row in result:
        w.writerow(row.values())
        print("写入成功！")


def get_fieldnames(urls):
    for url in urls:
        result = parse_url(url)
        if len(result) > 0:
            fieldnames = result[0].keys()
        else:
            pass
    return fieldnames


if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'
    }
    urls = get_all_url()
    fieldnames = get_fieldnames(urls)
    with open('result.csv', 'w', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(fieldnames)
        for url in urls:
            result = parse_url(url)
            if len(result) > 0:
                save_news_csv(result)
