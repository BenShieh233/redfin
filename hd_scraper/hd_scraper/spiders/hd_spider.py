import scrapy
import json
import time
import pandas as pd
import logging
from datetime import datetime
import os

class HomedepotSpider(scrapy.Spider):
    name = "homedepot"
    allowed_domains = ["apionline.homedepot.com"]
    start_urls = ["https://apionline.homedepot.com/federation-gateway/graphql?opname=productClientOnlyProduct"]

    def __init__(self, *args, **kwargs):
        super(HomedepotSpider, self).__init__(*args, **kwargs)
        
        # 读取 headers 和 payload
        with open('headers.json', 'r') as file:
            self.headers = json.load(file)
        with open('payload.json', 'r') as file:
            self.payload = json.load(file)

        # 读取 Excel 文件
        self.df = pd.read_excel(r"C:\Users\cs\OneDrive\Desktop\carro\redfin\Carro-sku.xlsx", sheet_name="每月一次爬取Carro产品信息")
        self.results = []

    def start_requests(self):
        for index, row in self.df.iterrows():
            item_id = str(row['<ID>'])
            if any(result['itemId'] == item_id for result in self.results):
                # 如果已经处理过此 item_id，则跳过
                continue

            # 设置 payload 的 itemId
            self.payload['variables']['itemId'] = item_id
            yield scrapy.Request(
                url=self.start_urls[0],
                method='POST',
                body=json.dumps(self.payload),
                headers=self.headers,
                callback=self.parse,
                meta={'item_id': item_id}
            )
            time.sleep(15)  # 添加延时，避免过多请求


    def parse(self, response):
        item_id = response.meta['item_id']

        if response.status == 206:
            logging.warning(f"Cookie expired for item {item_id}. Terminating crawl...")
            # 保存当前数据并终止爬取
            self.save_checkpoint(self.results)
            self.crawler.engine.close_spider(self, 'Cookie expired')
            return
        
        if response.status == 200:
            result = self.extract_data(response.json(), item_id)
            self.results.append(result)
            self.save_data(result)  # 保存数据到新文件
            self.save_checkpoint(self.results)  # 断点保存
        else:
            logging.error(f"Failed request for item {item_id}. Status code: {response.status}")

    def extract_data(self, data, item_id):
        try:
            product = data.get('data', {}).get('product', {})
            identifiers = product.get('identifiers', {})
            fulfillment_options = product.get('fulfillment', {}).get('fulfillmentOptions', [])
            pricing = product.get('pricing', {})
            reviews = product.get('reviews', {})

            locations = {}
            if fulfillment_options and 'services' in fulfillment_options[0]:
                locations = fulfillment_options[0]['services'][0].get('locations', [{}])[0]

            result = {
                'itemId': item_id,
                'storeSkuNumber': identifiers.get('storeSkuNumber'),
                'brandName': identifiers.get('brandName'),
                'productLabel': identifiers.get('productLabel'),
                'canonicalUrl': identifiers.get('canonicalUrl'),
                'parentId': identifiers.get('parentId'),
                'modelNumber': identifiers.get('modelNumber'),
                'inventory': locations.get('inventory', {}).get('quantity'),
                'out_of_stock_status': locations.get('inventory', {}).get('isInStock'),
                'value': pricing.get('value'),
                'original': pricing.get('original'),
                'review_num': reviews.get('ratingsReviews', {}).get('totalReviews'),
                'rating': reviews.get('ratingsReviews', {}).get('averageRating')
            }

        except Exception as e:
            logging.error(f"Error extracting data for item {item_id}: {e}")
            result = {
                'itemId': item_id,
                'storeSkuNumber': None,
                'brandName': None,
                'productLabel': None,
                'canonicalUrl': None,
                'parentId': None,
                'modelNumber': None,
                'inventory': None,
                'out_of_stock_status': None,
                'value': None,
                'original': None,
                'review_num': None,
                'rating': None
            }
        return result
    
if __name__ == '__main__':
    def print_current_directory_files():
        # 获取当前工作目录
        current_directory = os.getcwd()
        print(f"Current working directory: {current_directory}")
        
        # 列出当前目录下的所有文件和子目录
        files_and_dirs = os.listdir(current_directory)
        
        print("Files and directories in current directory:")
        for item in files_and_dirs:
            full_path = os.path.join(current_directory, item)
            if os.path.isfile(full_path):
                print(f"File: {item}")
            elif os.path.isdir(full_path):
                print(f"Directory: {item}")

    # 执行函数
    print_current_directory_files()