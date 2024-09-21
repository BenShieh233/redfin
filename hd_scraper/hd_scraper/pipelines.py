# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json
import os
from scrapy.exceptions import DropItem
from datetime import datetime

class HomedepotPipeline:

    def __init__(self):
        self.results = []
        self.output_file = f'results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        self.checkpoint_file = 'checkpoint.json'
        self.load_checkpoint()

    def process_item(self, item, spider):
        item_id = item.get('itemId')
        if item_id in self.processed_ids:
            # 如果已经处理过，则丢弃此项
            raise DropItem(f"Duplicate item found: {item_id}")

        self.results.append(item)
        self.save_data(item)  # 将数据追加到文件
        self.save_checkpoint()  # 更新断点记录
        return item

    def save_data(self, item):
        with open(self.output_file, 'a') as f:
            json.dump(item, f)
            f.write('\n')

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'r') as f:
                self.results = json.load(f)
                self.processed_ids = {item['itemId'] for item in self.results}
        else:
            self.processed_ids = set()

    def save_checkpoint(self):
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.results, f, indent=4)