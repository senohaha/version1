# coding: utf-8

# from scrapy.statscollectors import StatsCollector
# from scrapy.utils.project import get_project_settings
#
# settings = get_project_settings()
# stats_file = settings.get("STATS_FILE")
#
#
# class MyStatsCollector(StatsCollector):
#
#     def __init__(self, crawler):
#         super(MyStatsCollector, self).__init__(crawler)
#         self.spider_stats = {}
#
#     def _persist_stats(self, stats, spider):
#        with open(stats_file, "a") as f:
#            for stat in stats:
#                if (stat in ['finish_time', 'start_time']):
#                    time = stats[stat].strftime('%y-%m-%d %H:%M:%S')
#                    f.write(':'.join([stat, time]))
#                    f.write('\n')
#                else:
#                    f.write(':'.join([stat, str(stats[stat])]))
#                    f.write('\n')
#            f.write('\n')