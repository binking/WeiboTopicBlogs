# -*- coding: utf-8 -*-
# --------  话题48992  爬取一个话题下的所有微博  --------
import os
import sys
import time
import redis
import random
import pickle
import traceback
from datetime import datetime as dt
import multiprocessing as mp
from zc_spider.weibo_utils import RedisException
from zc_spider.weibo_config import (
    WEIBO_COOKIES, WEIBO_ACCOUNT_PASSWD,
    WEIBO_URLS_CACHE, WEIBO_INFO_CACHE,  # weibo:blog:urls, weibo:blog:info
    QCLOUD_MYSQL, LOCAL_REDIS, QCLOUD_REDIS
)
from weibo_blog_spider_v4 import WeiboBlogSpider
from weibo_blogs_writer import WeiboBlogsWriter

if os.environ.get('SPIDER_ENV') == 'test':
    USED_REDIS = LOCAL_REDIS
elif 'centos' in os.environ.get('HOSTNAME'):
    USED_REDIS = QCLOUD_REDIS
else:
    raise Exception("Unknown Environment, Check it now...")
USED_DATABASE = QCLOUD_MYSQL


def xhrize_topic_url(url):
    # Given a topic url like "http://weibo.com/p/100808289ad9eb102dffb586f850ab6b4183f4"
    if 'getIndex' in url:
        return url
    xhr_url = 'http://m.weibo.cn/container/getIndex?containerid=230530{topic}__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&luicode=10000011&lfid={topic}_-_ext_intro&featurecode=20000180'
    topic_id = url.split('/')[-1]
    return xhr_url.format(topic=topic_id)


def generate_info(cache):
    """
    Producer for urls and topics, Consummer for topics
    """
    error_count = 0
    loop_count = 0
    cp = mp.current_process()
    while True:
        res = {}
        loop_count += 1
        if error_count > 9999:
            print '>' * 10, 'Exceed 10000 times of gen errors', '<' * 10
            break
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Blogs Process pid is %d" % (cp.pid)
        job = cache.blpop(WEIBO_URLS_CACHE, 0)[1]
        xhr_url = xhrize_topic_url(job)
        try:
            cookies = cache.hvals(WEIBO_COOKIES)
            if len(cookies) == 0:
                time.sleep(loop_count*2)
                continue
            cookie = random.choice(cookies)
            spider = WeiboBlogSpider()
            status = spider.send_request_to_weibo(xhr_url, cookie)
            if status:
                res = spider.parse_tweet_list()
                if len(res) == 4:
                    cache.rpush(WEIBO_INFO_CACHE, pickle.dumps(res))
                    if len(res['next_url']) > 1:
                        cache.rpush(WEIBO_URLS_CACHE, res['next_url'])
            else:
                cache.rpush(WEIBO_URLS_CACHE, job)
        except Exception as e:  # no matter what was raised, cannot let process died
            traceback.print_exc()
            error_count += 1
            print 'Failed to parse job: ', job
            cache.rpush(WEIBO_URLS_CACHE, job)  # put job back
        time.sleep(2)


def write_data(cache):
    """
    Consummer for topics
    """
    error_count = 0
    cp = mp.current_process()
    dao = WeiboBlogsWriter(USED_DATABASE)
    while True:
        if error_count > 999:
            print '>' * 10, 'Exceed 1000 times of write errors', '<' * 10
            break
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Blogs Process pid is %d" % (cp.pid)
        res = cache.blpop(WEIBO_INFO_CACHE, 0)[1]
        info = pickle.loads(res)
        try:
            blogs = info['blogs']
            users = info['users']
            # topic = info['topic']
            if blogs:
                dao.insert_blogs_into_db(blogs)
            if users:
                dao.update_user_info(users)
                # if topic and len(topic) == 5:
                #     dao.update_topic_info(topic)
        except Exception as e:  # won't let you died
            traceback.print_exc()
            time.sleep(10)
            error_count += 1
            print 'Failed to write result: ', info
            cache.rpush(WEIBO_INFO_CACHE, res)


def run_all_worker():
    r = redis.StrictRedis(**USED_REDIS)
    print "Redis has %d records in cache" % r.llen(WEIBO_URLS_CACHE)
    job_pool = mp.Pool(processes=4,
                       initializer=generate_info, initargs=(r,))
    result_pool = mp.Pool(processes=2,
                          initializer=write_data, initargs=(r,))

    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
    try:
        job_pool.close()
        result_pool.close()
        job_pool.join()
        result_pool.join()
        print "+" * 10, "jobs' length is ", r.llen(WEIBO_URLS_CACHE)
        print "+" * 10, "results' length is ", r.llen(WEIBO_INFO_CACHE)
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in run all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"
        print "+" * 10, "jobs' length is ", r.llen(WEIBO_URLS_CACHE)
        print "+" * 10, "results' length is ", r.llen(WEIBO_INFO_CACHE)


if __name__ == "__main__":
    print "\n\n" + "%s Began Scraped Weibo Tweets" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    print "*" * 10, "Totally Scraped Weibo Tweets Time Consumed : %d seconds" % (time.time() - start), "*" * 10
