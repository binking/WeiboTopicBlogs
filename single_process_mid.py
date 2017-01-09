#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------
import re
import os
import sys
import time
import redis
import random
import traceback
import multiprocessing as mp
from datetime import datetime as dt
from zc_spider.weibo_config import (
    MANUAL_COOKIES,
    WEIBO_ACCOUNT_PASSWD,
    WEIBO_URL, WEIBO_MID,
    QCLOUD_MYSQL, OUTER_MYSQL,
    LOCAL_REDIS, QCLOUD_REDIS
)
from weibo_mid_spider import WeiboMidSpider
from weibo_mid_writer import WeiboMidWriter
from zc_spider.weibo_utils import RedisException
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


def single_process_mid():
    cache = redis.StrictRedis(**USED_REDIS)
    dao = WeiboMidWriter(USED_DATABASE)
    for _ in range(10):
        job = cache.blpop(WEIBO_URL, 0)[1]
        all_account = cache.hkeys(MANUAL_COOKIES)
        account = random.choice(all_account)
        spider = WeiboMidSpider(job, account, WEIBO_ACCOUNT_PASSWD, timeout=20)
        spider.use_abuyun_proxy()
        spider.add_request_header()
        spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
        status = spider.gen_html_source()
        mid = spider.get_weibo_mid()
        if len(mid) == 16:
            dao.update_url_to_mid(mid)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    single_process_mid()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
