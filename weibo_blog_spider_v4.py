# -*- encoding=utf-8 -*-
import re
import json
import requests
from datetime import datetime as dt
from datetime import timedelta
from bs4 import BeautifulSoup as bs
from zc_spider.weibo_utils import \
    gen_abuyun_proxy, retry, extract_post_data_from_curl,chin_num2dec

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


class WeiboBlogSpider:
    def __init__(self):
        self.uri = ''
        self.response = ''

    @retry((Exception,), tries=4, delay=2, backoff=2)
    def send_request_to_weibo(self, url, cookie):
        self.uri = url
        print "Send request to weibo: ", url
        r = requests.get(self.uri, timeout=20, headers=extract_post_data_from_curl(cookie), proxies=gen_abuyun_proxy())
        if r.status_code == 404 or "404 Not Found" in r.text:
            print "404 Not Found"
            return False
        elif r.status_code != 200:
            print "HTTP Code : ", r.status_code
            raise Exception("Retry")
        self.response = r.text
        return True

    def parse_tweet_list(self):
        """
        10 blogs / once
        """
        res = {}
        next_url = ''
        tweet_list = []; user_list = []
        print "Parsing : ", self.uri
        try:
            data = json.loads(self.response)
        except Exception as e:
            print str(e)
            print "(%s)--> " % self.uri, self.response
            return res
        # if data['ok'] != 1:
        #     print "Not OK(%s)" % (self.url)
        #     rconn.rpush(WEIBO_URLS_CACHE, self.url)
        #     return res
        # page info, may be from cardlistInfo or pageInfo
        try:
            page_info = data['cardlistInfo']
        except:
            page_info = data['pageInfo']
        # format next page
        since_id = page_info.get('since_id')
        if since_id and "since_id" not in self.uri: # first page
            next_url = self.uri + "&since_id=" + since_id
        elif since_id:  # update since_id
            next_url = re.sub(r'since_id=(.+)', 'since_id=' + since_id, self.uri)
            # rconn.rpush(WEIBO_URLS_CACHE, next_page)
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
            except Exception as e:
                print e
                print "Can not parse topic info"
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
                m_info['xhr_url'] = self.uri
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
                tweet_list.append(m_info)
                user_list.append(u_info)
        print "There are %d tweets and %d users : %s." % (len(tweet_list), len(user_list), self.uri)
        return { "blogs": tweet_list, "users": user_list , "topic": t_info, "next_url": next_url}
