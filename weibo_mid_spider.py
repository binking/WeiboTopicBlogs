#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------

import re
import json
import requests
from collections import Counter
from bs4 import BeautifulSoup as bs
from datetime import datetime as dt
from zc_spider.weibo_spider import WeiboSpider
from zc_spider.weibo_utils import catch_parse_error


class WeiboMidSpider(WeiboSpider):
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        WeiboSpider.__init__(self, start_url, account, password, timeout=timeout, delay=delay, proxy=proxy)
        self.mid = ''


    @catch_parse_error((AttributeError, Exception))
    def get_weibo_mid(self):
        mid = ''
        if len(self.page) < 20000:
            return mid
        mid_rexp = re.findall(r'mid=(\d{16})', self.page)
        import ipdb; ipdb.set_trace()
        if mid_rexp:
            mid_group = Counter(mid_rexp)
            print mid_group
            return mid_group.most_common()[0][0]
        return mid
