import json
import requests


class AnxinSpider(object):
    name = 'anxinspider'
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Cookie': 'financial_products=Mn5EKHnYqsyWNX4OcFkhD1PXX2w17/2JjL7uTF4oKL61bu3Q3IY6H17+h7ERq5E8KuQqk5WL7jgQMi6DOGenV17+h7ERq5E8oKTuGJvVhKoziOHXFN7xGYqEQinI8tdNmwnjJvQj7NE=; information_products=Mn5EKHnYqsyWNX4OcFkhD/GfhsJAeXXfc/3bkBfMJhXxfzni81O0UQVe/xRIVmTvyqQbgUiMI6etdmac8gnjLqkPnXgc8CJeXKSROlLwUyCTZYx14hFs2A==; JSESSIONID=abcM1kM2sCTrEc6IgVldw; invest_products=Mn5EKHnYqsyWNX4OcFkhDxvESFI3aOEoXvWjWbYYZsyT1pC9C6Ba2nwinYFLp9H4FWTgFUFpdyLkNpZnuHz8LEsITbTI/s++F6UPdYnb+lQ8SucRUDyGhcNba8NrQ4Gd',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'
    }
    req_url = 'https://mall.essence.com.cn/servlet/json'
    form_data = {
        'funcNo': '1000050',
        'product_shelf': '1',
        'fina_belongs': '1',
        'page': '1',
        'numPerPage': '10',
        'risk_level': '',
        'profit_type': '',
        'product_expires_start': '',
        'product_expires_end': '',
        'fina_type': '0',
        'per_buy_limit_start': '',
        'per_buy_limit_end': '',
        'search_value': '',
        'user_id': '',
        'asset_busin_type': '',
    }

    def parse_totalnum(self):
        data = requests.post(self.req_url, headers=self.headers, data=self.form_data).json()
        print(data)


if __name__ == '__main__':
    anxin = AnxinSpider()
    anxin.parse_totalnum()
    