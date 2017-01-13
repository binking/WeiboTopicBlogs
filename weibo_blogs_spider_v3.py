#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------

import re
import json
import math
import requests
from datetime import timedelta
from bs4 import BeautifulSoup as bs
from datetime import datetime as dt
from zc_spider.weibo_spider import WeiboSpider
from zc_spider.weibo_utils import catch_parse_error, chin_num2dec
from zc_spider.weibo_config import WEIBO_URLS_CACHE

# url: http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176
# 1st: 
#       containerid:2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%3A23055763d3d983819d66869c27ae8da86cb176
#       luicode:10000011
#       lfid:1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro
#       featurecode:20000180
# 2nd:
#       containerid:2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%3A23055763d3d983819d66869c27ae8da86cb176
#       luicode:10000011
#       lfid:1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro
#       featurecode:20000180
#       since_id:{"last_since_id":4063219502849963,"res_type":1,"next_since_id":4063134023350041}
def extract_content(html):
    parser = bs(html, 'html.parser')
    # temp = re.sub(r'@.+[ :]|#.+#', '', parser.text)
    [a.extract() for a in parser(('a', 'span'))]
    return parser.text

def extract_user_cn_url(url):
    pos = url.find('?')
    if pos>0:
        return url[:pos]
    return url

def format_publish_date(date):
    if u"分钟前" in date:
        temp = dt.now() - timedelta(minutes=int(date[:-3]))
        return temp.strftime("%Y-%m-%d %H:%M") 
    elif u"今天" in date:  # today
        return dt.now().strftime("%Y-%m-%d")+' '+date[3:]
    elif len(date.split('-')) == 2: # this year
        return dt.now().strftime("%Y")+'-'+date
    else:  # long long ago
        return date

class WeiboBlogsSpider(WeiboSpider):
    def __init__(self, start_url, account, password, timeout=10, delay=1, proxy={}):
        WeiboSpider.__init__(self, start_url, account, password, timeout=timeout, delay=delay, proxy=proxy)


    @catch_parse_error((AttributeError, Exception))
    def parse_tweet_list(self, rconn):
        """
        10 blogs / once
        """
        res = {}
        total_count = -1
        tweet_list = []; user_list = []
        try:
            data = json.loads(self.page)
        except Exception as e:
            print str(e), "(%s)--> " % self.url, self.page
            return res
        if data['ok'] != 1:
            print "Not OK(%s)" % (self.url)
            rconn.rpush(WEIBO_URLS_CACHE, self.url)
            return res
        # page info, may be from cardlistInfo or pageInfo
        try:
            page_info = data['cardlistInfo']
        except:
            page_info = data['pageInfo']
        # format next page
        since_id = page_info.get('since_id')
        if since_id and "since_id" not in self.url:
            # first page
            rconn.rpush(WEIBO_URLS_CACHE, self.url+"&since_id="+since_id)
        elif since_id:  # update since_id
            next_page = re.sub(r'since_id=(.+)', 'since_id=' + since_id, self.url)
            rconn.rpush(WEIBO_URLS_CACHE, next_page)
        else:
            print "No since id, no next page: ", page_info['containerid']
        # Game is on !!!
        # format topic info
        t_info = {}
        topic_stat = page_info.get('desc_more')
        containerid = page_info.get('containerid')
        if containerid and topic_stat and len(topic_stat) > 1:
            try:
                three_num_str, holder = topic_stat[0], topic_stat[1]
                num_tuple = three_num_str.split(u"　")  # not ascii
                t_info['url'] = 'http://weibo.com/p/' + containerid
                t_info['read_num'] = num_tuple[0][2:]
                t_info['disc_num'] = num_tuple[1][2:]
                t_info['like_num'] = num_tuple[2][2:]
                t_info['read_num_dec'] = chin_num2dec(t_info['read_num'])
            except :
                print "Can not parse topic info"
        print 'Topic info: ', t_info
        for card in data['cards']:
            for group in card['card_group']:
                m_info = {}; u_info = {}
                if not group.get('mblog'):
                    print "No any blog..."
                    continue
                mblog = group['mblog']
                user = group['mblog']['user']
                temp_id, short_id = group['itemid'].split('_')
                long_id = temp_id.split(':')[-1]
                # format user's info
                u_info['name'] = user['screen_name']
                u_info['intro'] = user.get('verified_reason', '')
                u_info['blogs'] = user['statuses_count']
                u_info['follows'] = user['follow_count']
                u_info['img_url'] = user['profile_image_url']
                u_info['fans'] = user['followers_count']
                u_info['rank'] = user['urank']
                u_info['type'] = user['mbtype']
                u_info['usercard'] = str(user['id'])
                u_info['cn_url'] = extract_user_cn_url(user['profile_url'])
                u_info['desc'] = user.get('description', '')
                # format blog's info
                m_info['xhr_url'] = self.url
                m_info['reposts'] = mblog['reposts_count']
                m_info['text'] = extract_content(mblog['text'])
                m_info['mid'] = mblog['id']
                m_info['u_name'] = user['screen_name']
                m_info['u_url'] = extract_user_cn_url(user['profile_url'])
                m_info['u_id'] = user['id']
                m_info['u_img'] = user['profile_image_url']
                m_info['url'] = 'http://weibo.com/%s/%s' % (user['id'], mblog['bid'])
                m_info['topic_url'] = 'http://weibo.com/p/' + long_id
                m_info['sub_date'] = format_publish_date(mblog['created_at'])
                m_info['date'] = dt.now().strftime("%Y-%m-%d %H:%M:%S") 
                m_info['device'] = mblog['source']
                m_info['likes'] = mblog['attitudes_count']
                m_info['comments'] = mblog['comments_count']
                # list all values
                # print "="*60
                # for k, v in m_info.items():
                #     print k, v
                # print "-"*60
                # for k, v in u_info.items():
                #     print k, v
                tweet_list.append(m_info)
                user_list.append(u_info)
        return { "blogs": tweet_list, "users": user_list , "topic": t_info}
