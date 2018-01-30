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
        # mongo集群
        # server = pymongo.MongoClient(port=self.port, host=self.host)
        # db = server[self.dbname]
        # self.db = db[self.docname]
        client = pymongo.MongoClient('mongodb://192.168.1.115:20000,192.168.1.160:20000,192.168.1.115:20001')
        db_admin = client['admin']

        db_config = client['config']
        db = client[self.dbname]

        is_partitioned = db_config.databases.find({"_id": self.dbname, "partitioned": True}).count()  # 判断该表是否已经设置分片
        if is_partitioned == 0:
            db_admin.command({"enablesharding": self.dbname})
            db_admin.command({"shardcollection": self.dbname + "." + self.docname, "key": {"_id": 1}})
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


