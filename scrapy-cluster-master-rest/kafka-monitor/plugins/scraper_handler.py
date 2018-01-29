from __future__ import absolute_import
from .base_handler import BaseHandler
import tldextract
import redis
import sys
from scrapy.http import Request
from redis.exceptions import ConnectionError
import pickle
from scrapy.utils.python import to_native_str


class ScraperHandler(BaseHandler):

    schema = "scraper_schema.json"

    def setup(self, settings):
        '''
        Setup redis and tldextract
        '''
        self.extract = tldextract.TLDExtract()
        self.redis_conn = redis.Redis(host=settings['REDIS_HOST'],
                                      port=settings['REDIS_PORT'],
                                      db=settings.get('REDIS_DB'))

        try:
            self.redis_conn.info()
            self.logger.debug("Connected to Redis in ScraperHandler")
        except ConnectionError:
            self.logger.error("Failed to connect to Redis in ScraperHandler")
            # plugin is essential to functionality
            sys.exit(1)

    def handle(self, dict):
        '''
        Processes a vaild crawl request

        @param dict: a valid dictionary object
        '''
        # format key
        print dict['spiderid'], 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        key = "{sid}:{spiderid}:queue".format(
            sid=dict['appid'],
            spiderid=dict['spiderid'],
        )
        print dict
        val = pickle.dumps(dict)  # dict->str
        self.redis_conn.zadd(key, val, -dict['priority'])

        # if timeout crawl, add value to redis
        if 'expires' in dict and dict['expires'] != 0:
            key = "timeout:{sid}:{appid}:{crawlid}".format(
                            sid=dict['spiderid'],
                            appid=dict['appid'],
                            crawlid=dict['crawlid'])
            self.redis_conn.set(key, dict['expires'])

        # log success
        dict['parsed'] = True
        dict['valid'] = True
        self.logger.info('Added crawl to Redis', extra=dict)



