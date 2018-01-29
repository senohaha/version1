# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html


def convertCoding(data, loader_context):
    """对于通知中的提取的title,content统一转化为utf-8编码"""

    encode = loader_context.get('encode')
    if encode == 'utf-8':
        return data
    try:
        for content in data:
            content = content.decode(encode, 'ignore').encode('utf-8')
            return content
    except BaseException, ex:
        print 'encode error!', ex
        return




