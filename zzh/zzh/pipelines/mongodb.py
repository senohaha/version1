# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo


class MongoPipeline(object):
    """保存爬取到的信息到数据"""

    def __init__(self, host, port, dbname, docname):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.docname = docname
        server = pymongo.MongoClient(port=self.port, host=self.host)
        db = server[self.dbname]
        self.db = db[self.docname]

    @classmethod
    def from_crawler(cls, crawler):
        return cls(host=crawler.settings.get('MONGODB_HOST'),
                   port=crawler.settings.get('MONGODB_PORT'),
                   dbname=crawler.settings.get('MONGODB_DBNAME'),
                   docname=crawler.settings.get('MONGODB_DOCNAME'))

    def process_item(self, item, spider):
        """将item存储到mongodb中"""

        item = dict(item)
        item.pop('file_urls_names', '')
        self.db.insert(item)
        return item


