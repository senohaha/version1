from scrapy.crawler import CrawlerProcess
from scrapy.conf import settings
from zzh.spiders.spider import ZzhSpider
from zzh.spiders.demo import Demo

process = CrawlerProcess(settings)
process.crawl(ZzhSpider())
process.start()

