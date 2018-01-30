# -*-coding:utf-8 -*-
# import redis
import six
from rediscluster import StrictRedisCluster
from scrapy.utils.misc import load_object


# DEFAULT_REDIS_CLS = redis.StrictRedis
DEFAULT_REDIS_CLS = StrictRedisCluster

# Sane connection defaults.
DEFAULT_PARAMS = {
    'socket_timeout': 30,
    'socket_connect_timeout': 30,
    'retry_on_timeout': True,
}

# Shortcut maps 'setting name' -> 'parmater name'.
SETTINGS_PARAMS_MAP = {
    'STARTUP_NODES': 'startup_nodes',

}


def get_redis_from_settings(settings):
    """Returns a redis client instance from given Scrapy settings object.

    This function uses ``get_client`` to instantiate the client and uses
    ``DEFAULT_PARAMS`` global as defaults values for the parameters. You can
    override them using the ``REDIS_PARAMS`` setting.

    Parameters
    ----------
    settings : Settings
        A scrapy settings object. See the supported settings below.

    Returns
    -------
    server
        Redis client instance.

    Other Parameters
    ----------------
    REDIS_URL : str, optional
        Server connection URL.
    REDIS_HOST : str, optional
        Server host.
    REDIS_PORT : str, optional
        Server port.
    REDIS_PARAMS : dict, optional
        Additional client parameters.

    """
    params = DEFAULT_PARAMS.copy()
    # 通过setting设置REDIS_PARAMS更新默认params的参数
    params.update(settings.getdict('REDIS_PARAMS'))

    # XXX: Deprecate REDIS_* settings.
    # 通过settings设置URL,HOST,PORT更新默认params的参数
    for source, dest in SETTINGS_PARAMS_MAP.items():
        # setting: 一个字典
        val = settings.get(source)
        if val:
            params[dest] = val

    # Allow ``redis_cls`` to be a path to a class.
    if isinstance(params.get('redis_cls'), six.string_types):
        params['redis_cls'] = load_object(params['redis_cls'])
    return get_redis(**params)


# Backwards compatible alias.
from_settings = get_redis_from_settings


def get_redis(**kwargs):
    """Returns a redis client instance.

    Parameters
    ----------
    redis_cls : class, optional
        Defaults to ``redis.StrictRedis``.
    url : str, optional
        If given, ``redis_cls.from_url`` is used to instantiate the class.
    **kwargs
        Extra parameters to be passed to the ``redis_cls`` class.

    Returns
    -------
    server
        Redis client instance.

    """
    print kwargs
    redis_cls = kwargs.pop('redis_cls', DEFAULT_REDIS_CLS)
    # 删除kwargs中键url对应的值,如果没有url返回None
    url = kwargs.pop('url', None)
    if url:
        return redis_cls.from_url(url, **kwargs)
    else:
        return redis_cls(**kwargs)


# use for test
if __name__ == "__main__":
    from scrapy.utils.project import get_project_settings

    settings = get_project_settings()
    print settings
    server = get_redis_from_settings(settings)
    print server
    server.ping()
    a = server.lpush('wert', 'aaaaaaaaaaa')
    print a, 'kkkkkkk'