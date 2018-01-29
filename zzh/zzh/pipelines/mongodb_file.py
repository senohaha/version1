# -*- coding:utf-8 -*-

from scrapy.pipelines.files import FilesPipeline, FSFilesStore
from scrapy.pipelines.files import FileException
from pymongo import MongoClient
from scrapy.http import Request
from scrapy.item import Item
from twisted.internet import defer
from twisted.internet.defer import DeferredList
from scrapy.utils.misc import arg_to_iter
from scrapy import log

import datetime
import hashlib
import gridfs
import traceback
import os


class MongodbFilesStore(FSFilesStore):
    """save file to gridfs of mongodb"""

    ShardMONGODB_HOST = "localhost"
    ShardMONGODB_PORT = 27017
    ShardMONGODB_DBNAME = "files"
    GridFs_Collection = "fs"

    def __init__(self, shard_host, shard_port, shard_dbname, shard_gridfs_collection):
        """initialize database"""
        try:
            client = MongoClient(shard_host, shard_port)
            self.db = client[shard_dbname]
            self.fs = gridfs.GridFS(self.db, shard_gridfs_collection)
        except Exception as e:
            traceback.print_exc()

    def persist_file(self, key, file_content, info, filename):
        """save file to mongodb"""
        contentType = os.path.splitext(filename)[1][1:].lower()
        file_id = self.fs.put(file_content, _id=key, filename=filename, contentType=contentType)
        checksum = self.fs.get(file_id).md5
        return file_id, checksum

    def stat_file(self, key, info):
        """the stat is the file key dir"""
        checksum = self.fs.get(key).md5
        last_modified = self.fs.get(key).upload_date
        return {'checksum': checksum, 'last_modified': last_modified}


class MongodbZzhFile(FilesPipeline):
    """
    This is for download zzh files and then define the file_id
    field to the file's gridfs id in the mongodb.
    """
    MEDIA_NAME = 'files'
    EXPIRES = 90  # 有效期
    STORE_SCHEMES = {
        '': MongodbFilesStore,
        'mongodb': MongodbFilesStore,
    }
    FILE_EXTENTION = ['.doc', '.txt', '.docx', '.rar', '.zip', '.pdf']

    def __init__(self, shard_host, shard_port, shard_dbname, shard_gridfs_collection, download_func=None):
        self.spiderinfo = {}
        self.download_func = download_func
        self.store = self._get_store(shard_host, shard_port, shard_dbname, shard_gridfs_collection)
        self.item_download = {}

    @classmethod
    def from_settings(cls, settings):
        cls.EXPIRES = settings.getint('FILE_EXPIRES', cls.EXPIRES)
        cls.FILE_CONTENT_TYPE = settings.get('FILE_CONTENT_TYPE', [])
        cls.ATTACHMENT_FILENAME_UTF8_DOMAIN = settings.get('ATTACHMENT_FILENAME_UTF8_DOMAIN', [])
        cls.URL_GBK_DOMAIN = settings.get('URL_GBK_DOMAIN', [])
        cls.FILE_EXTENTION = settings.get('FILE_EXTENTION', cls.FILE_EXTENTION)
        shard_host = settings.get('ShardMONGODB_HOST', "localhost")
        shard_port = settings.get('ShardMONGODB_PORT', 27017)
        shard_dbname = settings.get('ShardMONGODB_DBNAME', "zzh_files")
        shard_gridfs_collection = settings.get('GridFs_Collection', 'fs')
        return cls(shard_host, shard_port, shard_dbname, shard_gridfs_collection)

    def _get_store(self, shard_host, shard_port, shard_dbname, shard_gridfs_collection):
        """get MongodbFilesStory instance"""
        scheme = 'mongodb'
        store_cls = self.STORE_SCHEMES[scheme]
        return store_cls(shard_host, shard_port, shard_dbname, shard_gridfs_collection)

    def process_item(self, item, spider):
        """it will manage the Request result"""
        info = self.spiderinfo
        requests = self.get_media_requests(item, info)
        dlist = [self._process_request(r, info) for r in requests if r]
        if dlist:
            dfd = DeferredList(dlist, consumeErrors=True)
            dfd.addCallback(self.item_completed, item, info)
            return dfd.addCallback(self.another_process_item, item, info)
        else:
            return item

    def another_process_item(self, result, item, info):
        assert isinstance(result, (Item, Request)), "File pipeline' item_completed must return Item or Request, got %s" % (type(result))
        if isinstance(result, Item):
            return result
        elif isinstance(result, Request):
            dlist = [self._process_request(r, info) for r in arg_to_iter(result)]
            dfd = DeferredList(dlist, consumeErrors=True)
            dfd.addCallback(self.item_completed, item, info)
            #XXX:This will cause one item maybe return many times,it depends on how many
            #times the download url failed.But it doesn't matter.Because when raise errors,
            #the items are no longer processed by further pipeline components.And when all
            #url download failed we can drop that item which book_file or book_file_url are
            #empty.
            return dfd.addCallback(self.another_process_item, item, info)
        else:
            print 'there is no item or request'

    def get_media_requests(self, item, info):
        """
        Those requests will be processed by the pipeline and, when they have finished downloading,
        the results will be sent to the item_completed() method, as a list of 2-element tuples.
        """
        if item.get('fileUrlName'):
            for file_url_name in item['fileUrlName']:
                yield Request(file_url_name[0], meta={'filename': file_url_name[1]})
        else:
            yield None

    def media_downloaded(self, response, request, info):
        """Handler for downloads"""
        referer = request.headers.get('Referer')
        if response.status != 200:
            log.msg(
                format='%(medianame)s (code: %(status)s): '
                       'Error downloading %(medianame)s from %(request)s referred in <%(referer)s>',
                level=log.WARNING, spider=info.spider, medianame=self.MEDIA_NAME,
                status=response.status, request=request, referer=referer)
            raise FileException(request.url, '%s: download-error' % (request.url,))

        if not response.body:
            log.msg(format='%(medianame)s (empty-content): '
                           'Empty %(medianame)s from %(request)s referred in <%(referer)s>: no-content',
                    level=log.WARNING, spider=info.spider, medianame=self.MEDIA_NAME,
                    request=request, referer=referer)
            raise FileException(request.url, '%s: empty-content' % (request.url,))

        status = 'cached' if 'cached' in response.flags else 'downloaded'
        log.msg(
            format='%(medianame)s (%(status)s): '
                   'Downloaded %(medianame)s from %(request)s referred in <%(referer)s>',
            level=log.DEBUG, spider=info.spider, medianame=self.MEDIA_NAME,
            status=status, request=request, referer=referer)
        if self.is_valid_content_type(response):
            raise FileException(request.url, '%s: invalid-content_type' % (request.url,))

        filename = response.meta['filename']
        if not filename:
            raise FileException(request.url, '%s: noaccess-filename' % (request.url,))

        self.inc_stats(info.spider, status)

        try:
            key = self.file_key(response.url)
            # save file to Gridfs
            file_id, checksum = self.store.persist_file(key, response.body, info, filename)
        except FileException as exc:
            whyfmt = '%(medianame)s (error):' \
                     'Error processing %(medianame)s from %(request)s referred in <%(referer)s>: %(errormsg)s'
            log.msg(format=whyfmt, level=log.WARNING, spider=info.spider, medianame=self.MEDIA_NAME,
                    request=request, referer=referer, errormsg=str(exc))
            raise

        return {'url': request.url, 'file_id': file_id, 'checksum': checksum}

    def media_to_download(self, request, info):
        def _onsuccess(result):

            if not result:
                return  # returning None force download

            last_modified = result.get('last_modified', None)
            if not last_modified:
                return  # returning None force download

            timedelta_obj = datetime.datetime.now() - last_modified
            age_seconds = timedelta_obj.total_seconds()
            age_days = age_seconds / 60 / 60 / 24
            if age_days > self.EXPIRES:
                return  # returning None force download

            referer = request.headers.get('Referer')
            log.msg(
                format='%(medianame)s (uptodate):'
                       'Downloaded %(medianame)s from %(request)s referred in <%(referer)s>',
                level=log.DEBUG, spider=info.spider,
                medianame=self.MEDIA_NAME, request=request, referer=referer)
            self.inc_stats(info.spider, 'uptodate')

            checksum = result.get('checksum', None)

            return {'url': request.url, 'file_id': key, 'checksum': checksum}

        key = self.file_key(request.url)  # return the SHA1 hash of the file url
        dfd = defer.maybeDeferred(self.store.stat_file, key, info)
        dfd.addCallbacks(_onsuccess, lambda _: None)
        dfd.addErrback(log.err, self.__class__.__name__ + '.store.stat_file')
        return dfd

    def item_completed(self, results, item, info):
        """
        value of the results argument:
        [(True,
          {'checksum': '2b00042f7481c7b056c4b410d28f33cf',
           'file_id': '2efea5ccadc771263a333a064d94ba19167b8066',
           'url': 'http://www.example.com/files/product1.pdf'}),
         (False,
          Failure(...))]
        """
        if self.LOG_FAILED_RESULTS:
            msg = '%s found errors processing %s' % (self.__class__.__name__, item)
            for ok, value in results:
                if not ok:
                    log.err(value, msg, spider=info.spider)
        item['file_id'] = [value['file_id'] for ok, value in results if ok]
        item['file_url'] = [value['url'] for ok, value in results if ok]
        return item

    def is_valid_content_type(self, response):
        """judge whether is it a valid response by the Content-Type"""
        content_type = response.headers.get('Content-Type', '')
        return content_type not in self.FILE_CONTENT_TYPE

    def inc_stats(self, spider, status):
        spider.crawler.stats.inc_value('%s_file_count' % (self.MEDIA_NAME,), spider=spider)
        spider.crawler.stats.inc_value('%s_file_status_count/%s' % (self.MEDIA_NAME, status), spider=spider)

    def file_key(self, url):
        """return the SHA1 hash of the file url"""
        file_guid = hashlib.sha1(url).hexdigest()
        return '%s' % file_guid
