#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------

import re
import json
import time
import requests
from lxml import etree
from datetime import datetime as dt
from template.weibo_utils import catch_parse_error,extract_chinese_info
from template.weibo_spider import WeiboSpider


class WeiboBlogsSpider(WeiboSpider):
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        WeiboSpider.__init__(self, start_url, account, password, timeout=timeout, delay=delay, proxy=proxy)
        self.info = {}

    @catch_parse_error((AttributeError, Exception))
    def parse_bozhu_info(self):
        res = []
        if len(self.page) < 20000:
            return res
        source = json.loads(code)['data']
        tree = etree.HTML(source)
        divs = tree.xpath("//div[@node-type='feed_content']")
        one_div.find('a', attrs={'class': 'W_f14 W_fb S_txt1'})
        one_div.find('a', attrs={'class': 'W_f14 W_fb S_txt1'}).text
        print one_div.find('a', attrs={'class': 'W_f14 W_fb S_txt1'}).text
        print one_div.find('a', attrs={'class': 'W_f14 W_fb S_txt1'}).text.strip()
