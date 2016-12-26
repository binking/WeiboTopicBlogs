#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------
#coding=utf-8
import re
import json
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime as dt
from template.weibo_spider import WeiboSpider
from template.weibo_utils import catch_parse_error

class WeiboBlogsSpider(WeiboSpider):
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        WeiboSpider.__init__(self, start_url, account, password, timeout=timeout, delay=delay, proxy=proxy)
        self.info = {}


    @catch_parse_error((AttributeError, Exception))
    def parse_tweet_list(self):
        tweet_list = []
        if len(self.page) < 20000:
            return tweet_list
        # source = json.loads(self.page)['data']  # for http://weibo.com/p/aj/v6/mblog/mbloglist
        # Parse game is on !!!
        parser = bs(self.page, "html.parser")
        print 'Parsing weibo: %s' % self.url
        scripts = [sc for sc in parser.find_all('script') if 'WB_feed_detail' in sc.text]
        divs = []
        for script in scripts:
            temp = parser.find_all('div', attrs={'node-type': 'feed_content'})
            divs.extend(temp)
        if not divs:
            print >>open('./html/no_feed_list_error_%s.html' % dt.now().strftime("%Y-%m-%d %H:%M:%S"), 'w'), parser
        for div in divs:
            feed_info = {}
            a_tag = div.find('a', {'class': 'W_face_radius'})
            if a_tag:
                img_tag = protrait.find('img')
                feed_info['nickname'] = a_tag['title']
                if img_tag:
                    usercard = re.match(r'id=(\d+)', img_tag['usercard'])
                    feed_info['usercard'] = usercard.group(1) if usercard else ''
                    # weibo_url = re.match(r'(http://weibo\.com/\w+)', first_image['href'])
                    feed_info['user_url'] = img_tag['href']
                    feed_info['image'] = img_tag['src']
            time_div = div.find('a', attrs={'node-type': 'feed_list_item_date'})
            feed_info['time'] = time_div['title'] if time_div else ''
            feed_info['weibo_url'] = time_div['href'] if time_div else ''
            text_a =  div.find('div', attrs={'node-type': 'feed_list_content'})
            feed_info['text'] = text_a.text if text_a else ''
            device_a = div.find('a', attrs={'action-type': 'app_source'})
            feed_info['device'] = device_a.text if device_a else ''
            forward_a = div.find(attrs={'node-type': 'forward_btn_text'})
            feed_info['forward'] = forward.text if forward_a else 0
            


    


"""
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

"""