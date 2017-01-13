#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------
import os
import sys
import time
import redis
import pickle
import random
import traceback
from datetime import datetime as dt
import multiprocessing as mp
from zc_spider.weibo_utils import RedisException
from zc_spider.weibo_config import (
    MANUAL_COOKIES,
    WEIBO_ACCOUNT_PASSWD,
    WEIBO_URLS_CACHE, WEIBO_INFO_CACHE,  # weibo:blog:urls, weibo:blog:info
    QCLOUD_MYSQL, OUTER_MYSQL,
    LOCAL_REDIS, QCLOUD_REDIS
)
from weibo_blogs_spider_v3 import WeiboBlogsSpider
from weibo_blogs_writer import WeiboBlogsWriter
reload(sys)
sys.setdefaultencoding('utf-8')

if os.environ.get('SPIDER_ENV') == 'test':
    print "*"*10, "Run in Test environment"
    USED_DATABASE = OUTER_MYSQL
    USED_REDIS = LOCAL_REDIS
elif 'centos' in os.environ.get('HOSTNAME'): 
    print "*"*10, "Run in Qcloud environment"
    USED_DATABASE = QCLOUD_MYSQL
    USED_REDIS = QCLOUD_REDIS
else:
    raise Exception("Unknown Environment, Check it now...")


test_curls = {
    "ranji3890527@163.com": "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'Cookie: SUB=_2A251fEq1DeTxGeNH41EQ9CjMwjiIHXVWn1b9rDV6PUJbkdBeLXLukW0xruiqxhhEC26dXTDYOyScr7k5lw..; SUHB=08JrGttv8xy2ZA; SCF=AlkT3qmVha4C8fCTk2WCUzIfJardIlLfEytjpiIIDCmn67MOsIH7A_HqZL5XU_kVpC1p-L3Lj1IjbA3aCwc1JmY.; SSOLoginState=1484274405; _T_WM=d13870ebd0b8e244a4126ff1bdb489b7; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26featurecode%3D20000180%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    "zaisi264082@163.com": "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'Cookie: SUB=_2A251fEv_DeTxGeNH41MY9S3NzTuIHXVWn1W3rDV6PUJbkdBeLU3dkW0wtP27DurNRpzATBBddbffU6EeQw..; SUHB=0laVvOXWgD0LPv; SCF=As3SXkjzU4wMas_KCgMb2cyz41-MEW5ldXULrPBANM1BJjeJTo7rvmkARvMteIOum0ux8aSbpdKQA9xqb5sTb08.; SSOLoginState=1484274607; _T_WM=e256e443c8447f7472cd06a4cc77e00e; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26featurecode%3D20000180%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    "chengtezhuiji@163.com": "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'Cookie: SUB=_2A251fExwDeTxGeNH41YT9CzEzjWIHXVWn1Q4rDV6PUJbkdBeLVrRkW1A9EJ5Yhac_D_PSuFjZe1QaD6vlg..; SUHB=0xnSrzu3RohCdX; SCF=AqfnSjEslpg0uymVO2jG8T2q7q51UJX1bzlbtZ78RF83Oc2E8aofo-2aDUbHb1cHO-yr5TGfr1L0SYleyOS3paQ.; SSOLoginState=1484274720; _T_WM=88cc5879995ab9001b8011c57edef76e; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26featurecode%3D20000180%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    "gangji3814901@163.com": "curl 'http://m.weibo.cn/container/getIndex?containerid=230530100808164036950f29ba5f42a35a027240eaa0__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&featurecode=20000180&lfid=100808164036950f29ba5f42a35a027240eaa0_-_ext_intro&luicode=10000011&retcode=6102&since_id' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: SUB=_2A251fE23DeTxGeNH41cS8ijJzzuIHXVWn1P_rDV6PUJbkdBeLUXHkW2HLxpg9Va3By_f5mhTF1kos-cXvQ..; SUHB=0sqRVGxWaPVsMf; SCF=Airp-PgpAoccQlAdTBc0uq26kgwk7dgk_y8lnqq4dqtMk0WzLvqANe8ZOwZIhvUBWMKaLslm0HX7Ipd5JO_0moY.; SSOLoginState=1484275175; _T_WM=61030f86cc5de0853eddd626b93044de; M_WEIBOCN_PARAMS=featurecode%3D20000180%26luicode%3D10000011%26lfid%3D100808164036950f29ba5f42a35a027240eaa0_-_ext_intro%26fid%3D230530100808164036950f29ba5f42a35a027240eaa0__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Connection: keep-alive' --compressed",
}

def xhrize_topic_url(url):
    # Given a topic url like "http://weibo.com/p/100808289ad9eb102dffb586f850ab6b4183f4"
    if 'getIndex' in url:
        return url
    xhr_url = 'http://m.weibo.cn/container/getIndex?containerid=230530{topic}__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&luicode=10000011&lfid={topic}_-_ext_intro&featurecode=20000180'
    topic_id = url.split('/')[-1]
    return xhr_url.format(topic=topic_id)


def single_process():
    cache = redis.StrictRedis(**USED_REDIS)
    all_account = test_curls.keys()
    dao = WeiboBlogsWriter(USED_DATABASE)
    account = random.choice(all_account)
    # while True:
    #     job = cache.blpop(WEIBO_URLS_CACHE, 0)[1]
    job = "http://weibo.com/p/" + "1008087da2d6c9e74a5a22d510812b82a08c21"
    xhr_url = xhrize_topic_url(job)
    spider = WeiboBlogsSpider(xhr_url, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
    spider.use_abuyun_proxy()
    spider.add_request_header()
    spider.use_cookie_from_curl(test_curls.get(account))
    status = spider.gen_html_source(raw=True)
    if status == 404:
        return 
    res = spider.parse_tweet_list(cache)
    if len(res) == 3:
        import ipdb; ipdb.set_trace()
        blogs = res['blogs']
        users = res['users']
        topic = res['topic']
        if blogs:
            print blogs
            dao.insert_blogs_into_db(blogs)
        if users:
            print users
            dao.update_user_info(users)
        if topic and len(topic) == 4:
            print topic
            dao.update_topic_info(topic)

if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Tweets" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    single_process()
    print "*"*10, "Totally Scraped Weibo New Tweets Time Consumed : %d seconds" % (time.time() - start), "*"*10
