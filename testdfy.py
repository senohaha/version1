import pickle
import rediscluster
con = rediscluster.RedisCluster('192.168.1.115','7002')
a = pickle.dumps({u'allow_regex': None,u'crawlid': u'1',u'callback': u'parse_inform_index',u'url': u'http://www.moe.edu.cn/jyb_xxgk/moe_1777/moe_1779/', u'expires': 0, 'ts': 1516609540.575796, u'priority': 1, u'deny_regex': None, u'meta': {u'from': u'.scy_lbsj-right-nr ul li', u'continue': u'true', u'title': u'a::attr(title)', u'detail': {u'mtitle': u'h1', u'general': u'true'}, u'href': u'a::attr(href)', u'time': u'span'}, u'spiderid': u'zzh', u'maxdepth': 0, u'appid': u'dfysb',u'cookie': None, u'useragent': None, u'deny_extensions': None, u'allowed_domains': None})

con.zadd('dfysb:zzh:queue', a, 1)
print con.zscan('dfysb:zzh:queue', 0)
# from rediscluster import StrictRedisCluster
#
# STARTUP_NODES=[{'host':'192.168.1.115','port':7000},
#                     {'host':'192.168.1.115','port':7001},
#                     {'host':'192.168.1.115','port':7002},
#                     {'host':'192.168.1.160','port':7003},
#                     {'host':'192.168.1.160','port':7004},
#                     {'host':'192.168.1.160','port':7005}
#                    ]
# con = StrictRedisCluster(startup_nodes=STARTUP_NODES)
# con.set('dfy','ssssss')
# print con.get('dfy')
