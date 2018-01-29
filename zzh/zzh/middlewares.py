# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html

import random
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware


class RandomUserAgentMiddware(UserAgentMiddleware):

    def __init__(self, settings, user_agent='scrapy'):
        super(RandomUserAgentMiddware, self).__init__(user_agent)
        user_agent_file = settings.get('USER_AGENT_FILE')
        if user_agent_file:
            with open(user_agent_file) as f:
                user_agent_with_blank = f.readlines()
            self.user_agent = [ua.strip() for ua in user_agent_with_blank]

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls(crawler.settings)
        return obj

    def process_request(self, request, spider):
        ua = random.choice(self.user_agent)
        print 'user_agent', ua
        if ua:
            request.headers.setdefault('User-Agent', ua)

