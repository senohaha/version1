# -*-coding: utf-8 -*-
from zope.interface import implementer

from .interfaces import ISpiderScheduler
from .utils import get_spider_queues

@implementer(ISpiderScheduler)
class SpiderScheduler(object):

    def __init__(self, config):
        self.config = config
        self.update_projects()

    def schedule(self, project, spider_name, **spider_args):
        q = self.queues[project]
        #  self.queues： {u'dfy': <scrapyd.spiderqueue.SqliteSpiderQueue object at 0x1c3df90>}
        q.add(spider_name, **spider_args)

    def list_projects(self):
        return self.queues.keys()

    def update_projects(self):
        self.queues = get_spider_queues(self.config)
