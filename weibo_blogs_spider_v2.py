#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------

import re
import json
import math
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
            # with open('123456.json', 'w') as fw:
            #     json.dump(data, fw, indent=4)
        except Exception as e:
            print str(e)
            return res
        if data['ok'] != 1:
            print "Not OK: ", self.page
            return res
        # page info, may be from cardlistInfo or pageInfo
        try:
            page_info = data['cardlistInfo']
        except:
            page_info = data['pageInfo']
        max_page = page_info.get('total', 0) / 10
        if max_page < 1:
            print "No micro blog V1 "
            return res
        # format other pages
        if "&page=" not in self.url:
            # first page
            print "%s has %d mblogs.." % (self.url, page_info['total'])
            for i in range(1, max_page + 1):
                rconn.rpush(WEIBO_URLS_CACHE, self.url+"&page=%d"%i)
        # Game is on !!!
        for card in data['cards']:
            for group in card['card_group']:
                m_info = {}
                u_info = {}
                if not group.get('mblog'):
                    continue
                mblog = group['mblog']
                user = group['mblog']['user']
                long_id, short_id = group['itemid'].split('_')
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
                u_info['cn_url'] = user['profile_url']
                u_info['desc'] = user.get('description', '')
                # format blog's info
                m_info['reposts'] = mblog['reposts_count']
                m_info['text'] = extract_content(mblog['text'])
                m_info['mid'] = mblog['id']
                m_info['u_name'] = user['screen_name']
                m_info['u_url'] = user['profile_url']
                m_info['u_id'] = user['id']
                m_info['u_img'] = user['profile_image_url']
                m_info['url'] = 'http://weibo.com/%s/%s' % (user['id'], mblog['bid'])
                m_info['topic_url'] = 'http://weibo.com/p/' + long_id
                m_info['sub_date'] = format_publish_date(mblog['created_at'])
                m_info['date'] = dt.now().strftime("%Y-%m-%d %H:%M:%S") 
                m_info['device'] = mblog['source']
                m_info['likes'] = mblog['attitudes_count']
                m_info['comments'] = mblog['comments_count']
                # display
                print "="*60
                for k, v in u_info.items():
                    print k, v
                print "-"*60
                for k, v in m_info.items():
                    print k, v
                tweet_list.append(m_info)
                user_list.append(u_info)
        return { "blogs": tweet_list, "users": user_list }
                

"""
json format:
cards: list of dict:
    itemid: "2305301008088602443930b9b3c613bf69554b731aec__timeline__mobile_info_-_pageapp:23055763d3d983819d66869c27ae8da86cb176"
    openurl: str
    display_arrow: boolean
    title: str
    show_type: int, e.g: '1'
    card_type: int, e.g: '11'
    card_group: list of dict:
        itemid: str, e.g: '1022:1008088602443930b9b3c613bf69554b731aec_4059871185911949'
        mblog: dict
            reposts_count: 转发量 int,  
            cardid: str, e.g: 'star_059'
            picStatus: str, e.g:'0:1,1:1,2:1,3:1,4:1,5:1,6:1,7:1,8:1'
            text: html code, e.g: "<a class='k' href='http://m.weibo.cn/k/深圳身边事?from=feed'>#深圳身边事#</a><a href='http://m.weibo.cn/n/Listen虹'>@Listen虹</a>     1993年，身高158，初中后一直工作到现在，上班深圳，老家普宁，内向外向随机转换，其实是外向型<i class="face face_1 icon_19">[笑cry]</i><i class="face face_1 icon_19">[笑cry]</i><i class="face face_1 icon_19">[笑cry]</i>，喜欢偶尔户外，小旅行（这两年痴迷，以前是宅女）。想找一个有责任心，顾家的，幽默一点的男生，要找广东男生，不然就被扼杀在摇篮里，年龄大我几岁即可...<a href="/status/4059871185911949">全文</a>"
            sync_mblog: boolean, e.g: True
            visible: dict
                type: int
                list_id: int
            bmiddle_pic: url, e.g: 'http://ww4.sinaimg.cn/bmiddle/792008ffgw1fbdivc0my4j20ku0rsgqh.jpg'
            rid: str, e.g: '0_0_0_2666478854690811059'
            id: str(16), e.g: '4059871185911949'
            thumbnail_pic: url, e.g:'http://ww4.sinaimg.cn/bmiddle/792008ffgw1fbdivc0my4j20ku0rsgqh.jpg'
            attitudes_count: int
            source: str, e.g: '单身有毒Android'
            is_imported_topic: boolean, e.g: False
            mblog_show_union_info: int, e.g: 1
            topic_id: str, e.g: '1022:10080867e346d6acb71c0a12d1e33b4692abed'
            page_info: dict
                page_pic: dict
                    url: url, e.g: 'http://ww2.sinaimg.cn/thumbnail/b4a4e842jw1ety1s3rhc1j20qe0hln3v.jpg'}, 
                page_title: str, e.g: '#深圳身边事#'
                type: str, e.g: 'webpage'
                content1: str, e.g: ''
                page_url: str, e.g: 'http://m.weibo.cn/p/index?containerid=1008088602443930b9b3c613bf69554b731aec&extparam=%E6%B7%B1%E5%9C%B3%E8%BA%AB%E8%BE%B9%E4%BA%8B&uid=5983388253'
            favorited: boolean
            original_pic: url, e.g: 'http://ww4.sinaimg.cn/bmiddle/792008ffgw1fbdivc0my4j20ku0rsgqh.jpg'
            bid: alphabet, e.g: 'Ep7wqoNY1'
            user: dict
                verified: boolean, e.g: True
                screen_name: str, e.g: '月老在深圳'
                follow_count: 168
                cover_image_phone: url, e.g: 'https://ww1.sinaimg.cn/crop.0.0.640.640.640/7077dc1djw1ewyhdedluvj20ku0kuglt.jpg'
                follow_me: boolean, e.g: False
                profile_image_url: url, e.g: 'https://tva3.sinaimg.cn/crop.0.0.996.996.180/792008ffjw8f6eklzprh3j20ro0rognz.jpg'
                verified_reason: str, e.g: 微博本地资讯博主（深圳）http://t.cn/RUmMhZ7
                followers_count: int, e.g: 33603
                verified_type: int, e.g: 0
                mbrank: int, e.g: 3
                following: boolean, e.g: False
                statuses_count: int, e.g: 1304
                urank: int, e.g: 29
                mbtype: int, e.g: 12
                id: int, e.g: 2032142591
                profile_url: url, e.g: 'http://m.weibo.cn/u/2032142591'
                description: str, e.g: '科技园科苑某房网刘史彦欠我朋友1000元耍赖不还'
            isLongText: boolean, e.g: 'True'
            attitudes_status: int, e.g: 0
            gif_ids: str
            textLength: int, e.g: 322
            hasActionTypeCard: int, e.g: 0
            created_at: str, e.g: '今天 16:29'
            pics: list of dict
                url
                geo
                large
                pid
                size
            comments_count: int, e.g: 2
            is_show_bulletin: int, e.g: 1
        openurl: str
        show_type: int, e.g: 1
        card_type: int, e.g: 9
        scheme: url, e.g: 'http://m.weibo.cn/status/4059871185911949'
    _appid: str, e.g: 'p'
    _cur_filter, str, e.g: 't'
seeLevel: int, e.g: 3
cardlistInfo: list of dict
    v_p: str, e.g: '33'
    containerid: str, e.g: '2305301008088602443930b9b3c613bf69554b731aec__timeline__mobile_info_-_pageapp:23055763d3d983819d66869c27ae8da86cb176'
    button: NoneType
    since_id: str, e.g: '{"last_since_id":4059897262180042,"res_type":1,"next_since_id":4059880081908159}'
    title_top: str, e.g:'全部讨论'
    hide_oids: list of str, e.g: [u'1022:1008088602443930b9b3c613bf69554b731aec']
    cardlist_title: str, e.g: '全部讨论'
    shared_text_qrcode: str
    show_style: int, e.g: 1
    total: int, e.g: 10000
    shared_text: str
    desc: str
scheme: str, e.g: 'sinaweibo://cardlist?containerid=2305301008088602443930b9b3c613bf69554b731aec__timeline__mobile_info_-_pageapp:23055763d3d983819d66869c27ae8da86cb176&luicode=10000011&lfid=2305301008088602443930b9b3c613bf69554b731aec__timeline__mobile_info_-_pageapp:23055763d3d983819d66869c27ae8da86cb176&featurecode='
ok: boolean
"""