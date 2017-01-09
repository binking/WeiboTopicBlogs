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


def generate(cache):
    """
    Producer for urls and topics, Consummer for topics
    """
    error_count = 0
    cp = mp.current_process()
    while True:
        res = {}
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate MID Process pid is %d" % (cp.pid)
        job = cache.blpop(WEIBO_URL, 0)[1]  # job is user card
        try:
            if error_count > 999:
                print '>'*10, 'Exceed 1000 times of GEN errors', '<'*10
                break
            all_account = cache.hkeys(MANUAL_COOKIES)
            account = random.choice(all_account)
            spider = WeiboMidSpider(job, account, WEIBO_ACCOUNT_PASSWD, timeout=20)
            spider.use_abuyun_proxy()
            spider.add_request_header()
            spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
            status = spider.gen_html_source()
            mid = spider.get_weibo_mid()
            if len(mid) == 16:
                cache.rpush(WEIBO_MID, mid)
        except RedisException as e:
            print str(e); break
        except Exception as e:  # no matter what was raised, cannot let process died
            traceback.print_exc()
            print 'Failed to parse job: ', job
            cache.rpush(WEIBO_URL, job) # put job back
            error_count += 1
        

def write_data(cache):
    """
    Consummer for topics
    """
    error_count = 0
    cp = mp.current_process()
    dao = WeiboMidWriter(USED_DATABASE)
    while True:
        if error_count > 999:
            print '>'*10, 'Exceed 1000 times of write errors', '<'*10
            break
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Update MID Process pid is %d" % (cp.pid)
        mid = cache.blpop(WEIBO_MID, 0)[1]
        try:
            dao.update_url_to_mid(mid)
        except Exception as e:  # won't let you died
            error_count += 1
            print 'Failed to write result: ', mid
            cache.rpush(WEIBO_MID, mid)


def add_jobs(cache):
    dao = WeiboMidWriter(USED_DATABASE)
    for url in dao.read_new_user_from_db():
        cache.rpush(WEIBO_URL, url)

def run_all_worker():
    r = redis.StrictRedis(**USED_REDIS)
    add_pool = mp.Pool(processes=1,
        initializer=add_jobs, initargs=(r, ))
    job_pool = mp.Pool(processes=1,
        initializer=generate, initargs=(r, ))
    result_pool = mp.Pool(processes=1, 
        initializer=write_data, initargs=(r, ))

    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
    try:
        job_pool.close(); result_pool.close()
        job_pool.join(); result_pool.join()
        print "+"*10, "jobs' length is ", r.llen(WEIBO_URL) 
        print "+"*10, "results' length is ", r.llen(WEIBO_MID)
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in run all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"
        print "+"*10, "jobs' length is ", r.llen(WEIBO_URLO) 
        print "+"*10, "results' length is ", r.llen(WEIBO_MID)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
