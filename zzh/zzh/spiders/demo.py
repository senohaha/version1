# -*- coding: utf-8 -*-
from scrapy.spider import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class Demo(CrawlSpider):
    name = "demo"


    rules = [
        Rule(LinkExtractor(allow='ifeng\.com'), callback='parse_item', follow=True),
    ]

    def parse_item(self, response):
        print response.url
