# -*-coding:utf-8 -*-

import importlib
import six

from scrapy.utils.misc import load_object
from scrapy import Request
from . import connection


# TODO: add SCRAPY_JOB support.
class Scheduler(object):
    """Redis-based scheduler"""

    def __init__(self, server,
                 persist=False,
                 flush_on_start=False,

                 queue_key='%(appid)s:%(spiderid)s:queue',
                 queue_cls='zzh.scrapy_redis.queue.SpiderPriorityQueue',

                 dupefilter_key='%(spider)s:dupefilter',
                 dupefilter_cls='zzh.scrapy_redis.dupefilter.RFPDupeFilter',
                 idle_before_close=0,
                 serializer=None):
        """Initialize scheduler.

        Parameters
        ----------
        server : Redis
            The redis server instance.
        persist : bool
            Whether to flush requests when closing. Default is False.
        flush_on_start : bool
            Whether to flush requests on start. Default is False.
        queue_key : str
            Requests queue key.
        queue_cls : str
            Importable path to the queue class.
        dupefilter_key : str
            Duplicates filter key.
        dupefilter_cls : str
            Importable path to the dupefilter class.
        idle_before_close : int
            Timeout before giving up.

        """
        if idle_before_close < 0:
            raise TypeError("idle_before_close cannot be negative")

        self.server = server
        self.persist = persist
        self.flush_on_start = flush_on_start
        self.queue_key = queue_key
        self.queue_cls = queue_cls
        self.dupefilter_cls = dupefilter_cls
        self.dupefilter_key = dupefilter_key
        self.idle_before_close = idle_before_close
        self.serializer = serializer
        self.stats = None

    def __len__(self):
        return len(self.queue)

    @classmethod
    def from_settings(cls, settings):
        kwargs = {
            'persist': settings.getbool('SCHEDULER_PERSIST'),
            'flush_on_start': settings.getbool('SCHEDULER_FLUSH_ON_START'),
            'idle_before_close': settings.getint('SCHEDULER_IDLE_BEFORE_CLOSE'),
        }

        # If these values are missing, it means we want to use the defaults.
        optional = {
            # TODO: Use custom prefixes for this settings to note that are
            # specific to scrapy-redis.
            'queue_key': 'SCHEDULER_QUEUE_KEY',  # 自定义request存放的key
            'queue_cls': 'SCHEDULER_QUEUE_CLASS',
            'dupefilter_key': 'SCHEDULER_DUPEFILTER_KEY',  # 自定义判重存放的key
            # We use the default setting name to keep compatibility.
            'dupefilter_cls': 'DUPEFILTER_CLASS',
            'serializer': 'SCHEDULER_SERIALIZER',
        }
        for name, setting_name in optional.items():
            val = settings.get(setting_name)
            if val:
                kwargs[name] = val

        # Support serializer as a path to a module.
        if isinstance(kwargs.get('serializer'), six.string_types):
            kwargs['serializer'] = importlib.import_module(kwargs['serializer'])

        server = connection.from_settings(settings)
        # Ensure the connection is working.
        server.ping()

        return cls(server=server, **kwargs)

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls.from_settings(crawler.settings)
        # FIXME: for now, stats are only supported from this constructor
        instance.stats = crawler.stats
        return instance

    def open(self, spider):
        self.spider = spider
        # self.appid = spider.settings['appid']
        self.appid = 'dfysb'
        try:  # 锁定需要抓取的爬虫队列

            self.queue = load_object(self.queue_cls)(
                server=self.server,
                spider=spider,
                key=self.queue_key % {'appid': self.appid, 'spiderid': spider.name},
                serializer=self.serializer,
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate queue class '%s': %s",
                             self.queue_cls, e)

        try:
            self.df = load_object(self.dupefilter_cls)(
                server=self.server,
                key=self.dupefilter_key % {'spider': spider.name},
                debug=spider.settings.getbool('DUPEFILTER_DEBUG'),
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate dupefilter class '%s': %s",
                             self.dupefilter_cls, e)

        if self.flush_on_start:
            self.flush()
        # notice if there are requests already in the queue to resume the crawl
        if len(self.queue):
            spider.log("Resuming crawl (%d requests scheduled)" % len(self.queue))

    def close(self, reason):
        if not self.persist:
            self.flush()

    def flush(self):
        self.df.clear()
        self.queue.clear()

    def enqueue_request(self, request):
        print request
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        if self.stats:
            self.stats.inc_value('scheduler/enqueued/redis', spider=self.spider)
        self.queue.push(request)
        return True

    def next_request(self):
        block_pop_timeout = self.idle_before_close
        item = self.queue.pop(block_pop_timeout)
        if item:

            try:
                req = Request(item['url'])
            except ValueError:
                # need absolute url
                # need better url validation here
                req = Request(item['url'])

            try:
                if 'callback' in item and item['callback'] is not None:
                    req.callback = getattr(self.spider, item['callback'])
            except AttributeError:
                print 'kk'

            try:
                if 'errback' in item and item['errback'] is not None:
                    req.errback = getattr(self.spider, item['errback'])
            except AttributeError:
                print 'kk'

            # defaults not in schema
            if 'curdepth' not in item:
                item['curdepth'] = 0
            if "retry_times" not in item:
                item['retry_times'] = 0

            req.meta['field_css'] = item['meta']
            if 'item' in item['meta']:
                req.meta['item'] = item['meta']['item']

            if 'field_css' in item['meta']:
                req.meta['field_css'] = item['meta']['field_css']
            # extra check to add items to request
            if 'useragent' in item and item['useragent'] is not None:
                req.headers['User-Agent'] = item['useragent']

            return req

    def has_pending_requests(self):
        return len(self) > 0
