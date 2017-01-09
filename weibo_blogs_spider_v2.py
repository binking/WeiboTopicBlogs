#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------

import re
import json
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime as dt
from zc_spider.weibo_spider import WeiboSpider
from zc_spider.weibo_utils import catch_parse_error
from zc_spider.weibo_config import WEIBO_URLS_CACHE

# 100808e826f6371997698a545aab579528a46f
# 253A23055763d3d983819d66869c27ae8da86cb176
# http://m.weibo.cn/container/getIndex?
    # containerid=230530{topic_id}
    # __timeline__mobile_info_-_pageapp%{unknown_id}  (Also ignore)
    # &uid=5983388253 (Ignore)
# http://weibo.com/p/{topic_id}
# no content : 咦？暂时没有内容哦，稍后再来试试吧~
# special : http://weibo.com/p/100808f09bfbfec98a2ae3aad977c05cc52207
# mblogs list : http://weibo.com/p/aj/v6/mblog/mbloglist?
#   ajwvr=6&domain=100808&from=page_100808&mod=TAB&pagebar=0&tab=emceercd
#   &current_page=1&since_id=14&pl_name=Pl_Third_App__45
#   &id=100808f09bfbfec98a2ae3aad977c05cc52207
#   &script_uri=/p/100808f09bfbfec98a2ae3aad977c05cc52207/emceercd
#   &feed_type=1&page=1&pre_page=1&domain_op=100808&__rnd=1483523126129
# http://weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=100808&from=page_100808&mod=TAB&pagebar=0&tab=emceercd&current_page=1&since_id=14&pl_name=Pl_Third_App__45&id=100808f09bfbfec98a2ae3aad977c05cc52207&script_uri=/p/100808f09bfbfec98a2ae3aad977c05cc52207/emceercd&feed_type=1&page=1&pre_page=1&domain_op=100808&__rnd=1483524602908


def extract_content(html):
    parser = bs(html, 'html.parser')
    return parser.text


def format_publish_date(date):
    if u"今天" in date:  # today
        return dt.now().strftime("%Y-%m-%d")+date[3:]
    elif len(date.split('-')) = 2: # this year
        return dt.now().strftime("%Y")+date
    else:  # long long ago
        return date

class WeiboBlogsSpider(WeiboSpider):
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        WeiboSpider.__init__(self, start_url, account, password, timeout=timeout, delay=delay, proxy=proxy)


    @catch_parse_error((AttributeError, Exception))
    def parse_tweet_list(self):
        """
        10 blogs / once
        """
        res = {}
        total_count = -1
        tweet_list = []; user_list = []
        try:
            data = json.loads(self.page)
        except Exception as e:
            print str(e)
            return res
        if data['ok'] != '1':
            print "Not OK: ", self.page
            return res
        # page info, may be from cardlistInfo or pageInfo
        try:
            page_info = data['cardlistInfo']
        except:
            page_info = data['pageInfo']
        max_page = ceil(page_info['total'] / 10) 
        # format other pages
        if "&page=" not in self.url:
            # first page
            for i in range(1, max_page + 1):
                rconn.rpush(WEIBO_URLS_CACHE, self.url+"&page=%d"%i)
        # Game is on !!!
        for card in data['cards']:
            for group in card['card_group']:
                m_info = {}
                u_info = {}
                mblog = group['mblog']
                user = group['mblog']['user']
                long_id, short_id = group['itemid'].split('_')
                # format user's info
                u_info['name'] = user['screen_name']
                u_info['intro'] = user['verified_reason']
                u_info['blogs'] = iser['statuses_count']
                u_info['follow'] = user['follow_count']
                u_info['img_url'] = user['profile_image_url']
                u_info['fans'] = user['followers_count']
                u_info['rank'] = user['urank']
                u_info['type'] = user['mbtype']
                u_info['usercard'] = user['id']
                u_info['cn_url'] = user['profile_url']
                u_info['desc'] = user['description']
                # format blog's info
                m_info['repost_count'] = mblog['repost_count']
                m_info['text'] = extract_content(mblog['text'])
                m_info['mid'] = mblog['id']
                m_info['u_name'] = user['screen_name']
                m_info['u_url'] = user['profile_url']
                m_info['u_id'] = user['id']
                m_info['u_img'] = user['profile_image_url']
                m_info['url'] = 'http://weibo.com/%s/%s' % (user['id'], mblog['bid'])
                m_info['topic_url'] = 'http://weibo.com/p/' + long_id
                m_info['sub_date'] = format_publish_date(mblog[''])
                m_info['date'] = dt.now().strftime("%Y-%m-%d %H:%M:%S") 
                m_info['device'] = blog['source']
                m_info['repost'] = blog['repost_count']
                m_info['like'] = blog['attitudes_count']
                m_info['comment'] = blog['comments_count']

                tweet_list.append(m_info)
                user_list.append(u_info)
        return { "blogs": tweet_list, "users": user_list }
                

"""
fullpath
realpath, like http://weibo.com/p/100808289ad9eb102dffb586f850ab6b4183f4
theme
middle
createdate
uri, like http://weibo.com/p/100808289ad9eb102dffb586f850ab6b4183f4
weibo_mid
weibo_author_nickname
weibo_author_id
weibo_author_url, like http://weibo.com/u/2942419923?refer_flag=1008085010_
weibo_author_portrait
weibo_url, like http://weibo.com/2942419923/Eq2Iq4Ohi
weibo_content
sub_date, like 2017-01-09 18:05
device
weibo_forward_num
weibo_comment_num
weibo_thumb_up_num
topic_url, like http://weibo.com/p/100808289ad9eb102dffb586f850ab6b4183f4
"""