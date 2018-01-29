from zope.interface import implementer
from six import iteritems
from twisted.internet.defer import DeferredQueue, inlineCallbacks, maybeDeferred, returnValue

from .utils import get_spider_queues
from .interfaces import IPoller


import socket
from scrapyd.webservice import ListJobs
import requests
from twisted.application.service import IServiceCollection


@implementer(IPoller)
class QueuePoller(object):

    def __init__(self, config, app):
        self.app = app
        self.config = config
        self.update_projects()
        self.dq = DeferredQueue(size=1)
        self.HOST = '192.168.1.222'  # server de ip
        self.PORT = 9011
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.connect((self.HOST, self.PORT))

    @inlineCallbacks
    def poll(self):

        app = IServiceCollection(self.app, self.app)
        launch = app.getServiceNamed('launcher')
        spiders = launch.processes.values()

        running = [
            {
                "spider": s.spider,
                "jobid": s.job,
            } for s in spiders
        ]
        # [{'spider': 'link', 'jobid': 'fd967e32f53b11e7a966485b39c53ff1'},
        # {'spider': 'link', 'jobid': '03ba6c7ef53c11e7a966485b39c53ff1'}]
        # print '!!!!!!!!!!!!!!!!!!!', running
        job_num = len(running)

        self.socket.send(str(job_num))
        # data = self.socket.recv(1024)
        # print 'data',data
        # print 'data send to server !!!!!'


        if self.dq.pending:
            return
        for p, q in iteritems(self.queues):
            c = yield maybeDeferred(q.count)
            if c:
                msg = yield maybeDeferred(q.pop)
                if msg is not None:  # In case of a concurrently accessed queue
                    returnValue(self.dq.put(self._message(msg, p)))
        # print 22


    def next(self):
        return self.dq.get()

    def update_projects(self):
        self.queues = get_spider_queues(self.config)

    def _message(self, queue_msg, project):
        d = queue_msg.copy()
        d['_project'] = project
        d['_spider'] = d.pop('name')
        return d
