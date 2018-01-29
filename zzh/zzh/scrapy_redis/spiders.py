# -*-coding:utf-8 -*-

from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider, CrawlSpider
from scrapy_splash import SplashRequest

from . import connection
import json

# Default batch size matches default concurrent requests setting.
DEFAULT_START_URLS_BATCH_SIZE = 16
DEFAULT_START_URLS_KEY = '%(name)s:start_urls'


class RedisMixin(object):
    """Mixin class to implement reading urls from a redis queue."""
    # Per spider redis key, default to DEFAULT_KEY.
    redis_key = None
    # Fetch this amount of start urls when idle. Default to DEFAULT_BATCH_SIZE.
    redis_batch_size = None
    # Redis client instance.
    server = None

    def start_requests(self):
        """Returns a batch of start requests from redis."""
        return self.next_requests()

    def setup_redis(self, crawler=None):
        """Setup redis connection and idle signal.

        This should be called after the spider has set its crawler object.
        """
        if self.server is not None:
            return

        if crawler is None:
            # We allow optional crawler argument to keep backwards
            # compatibility.
            # XXX: Raise a deprecation warning.
            crawler = getattr(self, 'crawler', None)

        if crawler is None:
            raise ValueError("crawler is required")

        settings = crawler.settings

        # 如果spider的redis_key为空,则是这个spider添加redis_key属性
        if self.redis_key is None:
            self.redis_key = settings.get(
                'REDIS_START_URLS_KEY', DEFAULT_START_URLS_KEY,
            )

        self.redis_key = self.redis_key % {'name': self.name}

        if not self.redis_key.strip():
            raise ValueError("redis_key must not be empty")

        if self.redis_batch_size is None:
            self.redis_batch_size = settings.getint(
                'REDIS_START_URLS_BATCH_SIZE', DEFAULT_START_URLS_BATCH_SIZE,
            )

        try:
            self.redis_batch_size = int(self.redis_batch_size)
        except (TypeError, ValueError):
            raise ValueError("redis_batch_size must be an integer")

        self.logger.info("Reading start URLs from redis key '%(redis_key)s' "
                         "(batch size: %(redis_batch_size)s)", self.__dict__)

        self.server = connection.from_settings(crawler.settings)
        # The idle signal is called when the spider has no requests left,
        # that's when we will schedule new requests from redis queue
        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def next_requests(self):
        """Returns a request to be scheduled or none."""
        use_set = self.settings.getbool('REDIS_START_URLS_AS_SET')
        fetch_one = self.server.spop if use_set else self.server.lpop
        # XXX: Do we need to use a timeout here?

        found = 0  # found的作用: 前面设置了第一次从redis取多少个url,用for来实现.
        while found < self.redis_batch_size:
            data = fetch_one(self.redis_key)
            if not data:
                # Queue empty.
                break
            req = self.make_request_from_data(data)
            if req:
                yield req  # 关键点: scrapy引擎会把这个request递交给这个spider对应的scheduler对象进行调度
                found += 1
            else:
                self.logger.debug("Request not made from data: %r", data)

        if found:
            self.logger.debug("Read %s requests from '%s'", found, self.redis_key)

    def make_request_from_data(self, data):
        """convert data into another format that we need"""
        try:
            url_rules = json.loads(data)
            start_url = 'http://404.html'
            field_css = ''
            for key, val in url_rules.items():
                start_url = key
                field_css = val
            if start_url != 'http://404.html' and field_css:
                return SplashRequest(url=start_url, meta={'field_css': field_css}, headers={'User-Agent': 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)'},
                                     callback=self.parse_inform_index, dont_filter=True)
        except Exception, e:
            self.logger.error("Unexpected URL from '%s': %r", self.redis_key, data)

    def schedule_next_requests(self):
        """Schedules a request if available"""
        for req in self.next_requests():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        # XXX: Handle a sentinel to close the spider.
        self.schedule_next_requests()
        raise DontCloseSpider

    def parse_inform_index(self, response):
        pass


class RedisSpider(RedisMixin, Spider):
    """Spider that reads urls from redis queue when idle."""

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(RedisSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_redis(crawler)
        return obj


class RedisCrawlSpider(RedisMixin, CrawlSpider):
    """Spider that reads urls from redis queue when idle."""

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(RedisCrawlSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_redis(crawler)
        return obj
