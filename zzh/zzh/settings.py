# -*- coding: utf-8 -*-

# Scrapy settings for zzh project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
import os

BOT_NAME = 'zzh'

SPIDER_MODULES = ['zzh.spiders']
NEWSPIDER_MODULE = 'zzh.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'zzh (+http://www.yourdomain.com)'

# mysql数据库配置
# MYSQL_HOST = '127.0.0.1'
MYSQL_HOST = '121.40.183.170'
MYSQL_PORT = '3306'
MYSQL_DBNAME = 'zzh'
MYSQL_USER = 'root'
MYSQL_PWD = '123456'

# mongodb数据库配置
# MONGODB_HOST = '127.0.183.170'
MONGODB_HOST = '127.0.0.1'
MONGODB_PORT = 27017
MONGODB_DBNAME = 'zzh'
MONGODB_DOCNAME = 'from_scrapy'

# Gridfs数据库配置
ShardMONGODB_HOST = '127.0.0.1'
ShardMONGODB_PORT = 27017
ShardMONGODB_DBNAME = "zzh_file"
GridFs_Collection = "fs"

# redis数据库配置
REDIS_URL = 'redis://127.0.0.1:6379/0'


# 文件存储
# FILES_STORE = os.getcwd()  # 附件存储

# LOG_FILE = 'log/log.txt'  # 日志文件存储

# STATS_CLASS = 'zzh.stats.MyStatsCollector'
# STATS_FILE = 'log/stats.txt'  # 状态文件存储

# USER_AGENT_FILE = 'zzh/useragents.txt'

# 分布式爬虫配置
SCHEDULER_PERSIST = True
# SCHEDULER_QUEUE_KEY = '%(appid)s:%(spiderid)s:queue'
SCHEDULER_DUPEFILTER_KEY = 'zzh:dupefilter'
SCHEDULER = "zzh.scrapy_redis.scheduler.Scheduler"
DUPEFILTER_CLASS = "zzh.scrapy_redis.dupefilter.RFPDupeFilter"


# JS动态渲染配置
SPLASH_URL = 'http://0.0.0.0:8050/'
DOWNLOADER_MIDDLEWARES = {
    'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware': None,
    'zzh.middlewares.RandomUserAgentMiddware': 400,
    'scrapy_splash.SplashCookiesMiddleware': 723,
    'scrapy_splash.SplashMiddleware': 725,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
}
HTTPCACHE_STORAGE = 'scrapy_splash.SplashAwareFSCacheStorage'


# 持久化层配置
ITEM_PIPELINES = {
   # 'zzh.pipelines.mongodb_file.MongodbZzhFile': 200,
   'zzh.pipelines.file.FilesPipeline': 400,
   'zzh.pipelines.mongodb.MongoPipeline': 500,
}


