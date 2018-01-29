# coding: utf-8

import functools
import hashlib
import os
import os.path
import time
import logging
from urlparse import urlparse
from collections import defaultdict


try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

from twisted.internet import defer, threads
from twisted.internet.defer import DeferredList

from scrapy.pipelines.media import MediaPipeline
from scrapy.settings import Settings
from scrapy.item import Item
from scrapy.utils.misc import arg_to_iter
from scrapy.exceptions import NotConfigured, IgnoreRequest
from scrapy.http import Request
from scrapy.utils.misc import md5sum
from scrapy.utils.log import failure_to_exc_info
from scrapy.utils.python import to_bytes
from scrapy.utils.request import referer_str


logger = logging.getLogger(__name__)


class FileException(Exception):
    """General media error exception"""


class FSFilesStore(object):
    def __init__(self, basedir):

        self.basedir = basedir
        self._mkdir(self.basedir)
        self.created_directories = defaultdict(set)

    def persist_file(self, path, buf, info, meta=None, headers=None):
        """写文件"""

        absolute_path = self._get_filesystem_path(path)
        self._mkdir(os.path.dirname(absolute_path), info)
        with open(absolute_path, 'wb') as f:
            f.write(buf.getvalue())

    def stat_file(self, path, info):
        """返回文件最近修改时间和对应的md5值，不存在则返回空字典"""

        absolute_path = self._get_filesystem_path(path)
        try:
            last_modified = os.path.getmtime(absolute_path)  # 文件最近修改时间
        except:  # FIXME: catching everything!
            return {}

        with open(absolute_path, 'rb') as f:
            checksum = md5sum(f)  # 计算一个类文件对象的md5值

        return {'last_modified': last_modified, 'checksum': checksum}

    def _get_filesystem_path(self, path):
        """返回文件位置的绝对路径"""

        path_comps = path.split('/')
        return os.path.join(self.basedir, *path_comps)

    def _mkdir(self, dirname, domain=None):
        """创建文件存放的文件夹"""

        seen = self.created_directories[domain] if domain else set()
        if dirname not in seen:
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            seen.add(dirname)


class FilesPipeline(MediaPipeline):
    """Abstract pipeline that implement the file downloading"""

    MEDIA_NAME = "file"
    EXPIRES = 90
    STORE_SCHEMES = {
        '': FSFilesStore,
        'file': FSFilesStore,
    }
    DEFAULT_FILES_URLS_FIELD = 'file_urls_names'  # 和item中的字段绑定
    DEFAULT_FILES_RESULT_FIELD = 'files'

    def __init__(self, store_uri, download_func=None, settings=None):
        if not store_uri:
            raise NotConfigured

        if isinstance(settings, dict) or settings is None:
            settings = Settings(settings)

        cls_name = "FilesPipeline"
        self.store = self._get_store(store_uri)  # 获取相应的存储器

        resolve = functools.partial(self._key_for_pipe,
                                    base_class_name=cls_name)  # functools.partial 偏函数
        self.expires = settings.getint(
            resolve('FILES_EXPIRES'), self.EXPIRES
        )
        self.files_urls_field = settings.get(
            resolve('FILES_URLS_FIELD'), self.DEFAULT_FILES_URLS_FIELD
        )
        self.files_result_field = settings.get(
            resolve('FILES_RESULT_FIELD'), self.DEFAULT_FILES_RESULT_FIELD
        )

        super(FilesPipeline, self).__init__(download_func=download_func)

    @classmethod
    def from_settings(cls, settings):
        store_uri = settings.get('FILES_STORE')  # 获取附件存储地址
        return cls(store_uri, settings=settings)

    def _get_store(self, uri):
        """根据不同的协议，初始化不同的存储对象"""

        if os.path.isabs(uri):  # to support win32 paths like: C:\\some\dir
            scheme = 'file'
        else:
            scheme = urlparse(uri).scheme
        store_cls = self.STORE_SCHEMES[scheme]
        return store_cls(uri)

    def process_item(self, item, spider):
        """入口函数，对request进行处理"""

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

    def media_to_download(self, request, info):
        """下载前对request检查"""

        def _onsuccess(result):
            """判断下载的文件是否过期"""

            if not result:
                return  # returning None force download

            last_modified = result.get('last_modified', None)
            if not last_modified:
                return  # returning None force download

            age_seconds = time.time() - last_modified
            age_days = age_seconds / 60 / 60 / 24
            if age_days > self.expires:
                return  # returning None force download

            referer = referer_str(request)
            logger.debug(
                'File (uptodate): Downloaded %(medianame)s from %(request)s '
                'referred in <%(referer)s>',
                {'medianame': self.MEDIA_NAME, 'request': request,
                 'referer': referer},
                extra={'spider': info.spider}
            )
            self.inc_stats(info.spider, 'uptodate')

            checksum = result.get('checksum', None)
            return {'url': request.url, 'path': path, 'checksum': checksum}

        path = self.file_path(request, info=info)
        dfd = defer.maybeDeferred(self.store.stat_file, path, info)  # 调用存储器的stat_file方法
        dfd.addCallbacks(_onsuccess, lambda _: None)
        dfd.addErrback(
            lambda f:
            logger.error(self.__class__.__name__ + '.store.stat_file',
                         exc_info=failure_to_exc_info(f),
                         extra={'spider': info.spider})
        )
        return dfd

    def media_failed(self, failure, request, info):
        """下载失败时，日志处理"""

        if not isinstance(failure.value, IgnoreRequest):
            referer = referer_str(request)
            logger.warning(
                'File (unknown-error): Error downloading %(medianame)s from '
                '%(request)s referred in <%(referer)s>: %(exception)s',
                {'medianame': self.MEDIA_NAME, 'request': request,
                 'referer': referer, 'exception': failure.value},
                extra={'spider': info.spider}
            )

        raise FileException

    def media_downloaded(self, response, request, info):
        """对下载后的response处理"""

        referer = referer_str(request)

        if response.status != 200:
            logger.warning(
                'File (code: %(status)s): Error downloading file from '
                '%(request)s referred in <%(referer)s>',
                {'status': response.status,
                 'request': request, 'referer': referer},
                extra={'spider': info.spider}
            )
            raise FileException('download-error')

        if not response.body:
            logger.warning(
                'File (empty-content): Empty file from %(request)s referred '
                'in <%(referer)s>: no-content',
                {'request': request, 'referer': referer},
                extra={'spider': info.spider}
            )
            raise FileException('empty-content')

        status = 'cached' if 'cached' in response.flags else 'downloaded'
        logger.debug(
            'File (%(status)s): Downloaded file from %(request)s referred in '
            '<%(referer)s>',
            {'status': status, 'request': request, 'referer': referer},
            extra={'spider': info.spider}
        )
        self.inc_stats(info.spider, status)

        try:
            path = self.file_path(request, response=response, info=info)
            checksum = self.file_downloaded(response, request, info)
        except FileException as exc:
            logger.warning(
                'File (error): Error processing file from %(request)s '
                'referred in <%(referer)s>: %(errormsg)s',
                {'request': request, 'referer': referer, 'errormsg': str(exc)},
                extra={'spider': info.spider}, exc_info=True
            )
            raise
        except Exception as exc:
            logger.error(
                'File (unknown-error): Error processing file from %(request)s '
                'referred in <%(referer)s>',
                {'request': request, 'referer': referer},
                exc_info=True, extra={'spider': info.spider}
            )
            raise FileException(str(exc))

        return {'url': request.url, 'path': path, 'checksum': checksum, 'file_name': response.meta['file_name']}

    def inc_stats(self, spider, status):
        spider.crawler.stats.inc_value('file_count', spider=spider)
        spider.crawler.stats.inc_value('file_status_count/%s' % status, spider=spider)

    ### Overridable Interface
    def get_media_requests(self, item, info):

        for file_url, file_name in item.get(self.files_urls_field, []):
            yield Request(file_url, meta={'file_name': file_name})
        else:
            yield None

    def file_downloaded(self, response, request, info):
        """把文件以二进制形式存入文件"""

        path = self.file_path(request, response=response, info=info)
        buf = BytesIO(response.body)
        checksum = md5sum(buf)
        buf.seek(0)
        self.store.persist_file(path, buf, info)  # 调用存储器的persist_file方法
        return checksum

    def item_completed(self, results, item, info):
        """对item中file字段赋值"""

        if isinstance(item, dict) or self.files_result_field in item.fields:
            item[self.files_result_field] = [x for ok, x in results if ok]
        return item

    def file_path(self, request, response=None, info=None):
        """根据request返回存储文件的相对路径"""

        def _warn():
            from scrapy.exceptions import ScrapyDeprecationWarning
            import warnings
            warnings.warn('FilesPipeline.file_key(url) method is deprecated, please use '
                          'file_path(request, response=None, info=None) instead',
                          category=ScrapyDeprecationWarning, stacklevel=1)

        # check if called from file_key with url as first argument
        if not isinstance(request, Request):
            _warn()
            url = request
        else:
            url = request.url

        media_guid = hashlib.sha1(to_bytes(url)).hexdigest()  # sha1 哈希算法
        media_ext = os.path.splitext(url)[1]
        return 'files/%s%s' % (media_guid, media_ext)