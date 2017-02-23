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
    WEIBO_COOKIES,
    WEIBO_ACCOUNT_PASSWD,
    WEIBO_URLS_CACHE, WEIBO_INFO_CACHE,  # weibo:blog:urls, weibo:blog:info
    QCLOUD_MYSQL, OUTER_MYSQL,
    LOCAL_REDIS, QCLOUD_REDIS
)
from weibo_blogs_spider_v2 import WeiboBlogsSpider
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
    'yijipiaoliao@163.com': "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'Cookie: SUB=_2A251cMr6DeTxGeNH41cS8SvNwz2IHXVWmtayrDV6PUJbkdBeLXP3kW1GQSV2TN29WYOzXiCzy_7bcN9ucg..; SUHB=0vJYC7zGfOUBmj; SCF=Ah6MUIBoeOpv_szSLd4RZTsSkMMcJ33C94QiwgBGDaKniU3I0zIccn3xOF8rPAP9WcZTdNmp99ySurQb58GuHQ0.; SSOLoginState=1484044970; _T_WM=15062623c58522a28021e8e2f676910c; H5_INDEX=2; H5_INDEX_TITLE=%E5%B1%91%E5%91%98%E6%95%91%E6%96%99%E6%85%95; M_WEIBOCN_PARAMS=featurecode%3D20000180%26oid%3D4062311339496883%26luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    'shayuan1686@163.com': "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'Cookie: SUB=_2A251cMvIDeTxGeNH41cS8SvPwz6IHXVWmtWArDV6PUJbkdBeLWfkkW2eEFJeNIPzWaaZRRxSw2e0iSimPw..; SUHB=0tK6oCwrNrVWkA; SCF=AiXKaoWe6yB71aZ3Oe31cVpQNFRxaIbS5aR8KEVhAfX6xfcUXE8pRskvxmq4Q63wxqoRoDa3vrcV8n3OwhucGzM.; SSOLoginState=1484045208; _T_WM=2a87c9e7f774fa68e4844fdf91138d88; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26featurecode%3D20000180%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    'shangou8477621@163.com': "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'Cookie: _T_WM=7af0501c47c617371398f06d35dc8171; SUB=_2A251cMuKDeRxGeNH41EQ8ybJyjqIHXVWmtXCrDV6PUJbkdBeLRmjkW09pD-bd4nOsJ-US_at4qhSqXtqEg..; SUHB=0u5RFiOYHc6UI8; SCF=Aq2_vEYgIUq6QNxcXjLgeYiBH0U9z8iXlH7HdMEFWWZ669WA-cfjpROxMC_nH7Vq9qN0u7h9FT9bamS98QjIjeQ.; SSOLoginState=1484045274; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26featurecode%3D20000180%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    'caiji951594@163.com': "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'Cookie: SUB=_2A251cMxADeTxGeNH41cS8SjJyzSIHXVWmtQIrDV6PUJbkdBeLW3GkW0DxNQ-40FU2qjAm1phCyCe68dfSQ..; SUHB=0cS3ek0uartsRo; SCF=Ardni18oom-kE5BEnPhHF8aDTNZzMHNT1x-sT6hlt0GMpiQxnqQ2mZk1hoPrfWyYIO2qCX3NGzq3BDsvdBPUusY.; SSOLoginState=1484045329; _T_WM=b9d81e3a93d7ccaa8f673e7dd8365583; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26featurecode%3D20000180%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
}


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
    cp = mp.current_process()
    while True:
        res = {}
        if error_count > 999:
            print '>'*10, 'Exceed 1000 times of gen errors', '<'*10
            break
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Generate Blogs Process pid is %d" % (cp.pid)
        job = cache.blpop(WEIBO_URLS_CACHE, 0)[1]
        xhr_url = xhrize_topic_url(job)
        try:
            all_account = cache.hkeys(WEIBO_COOKIES)
            # all_account = test_curls.keys()
            account = random.choice(all_account)
            spider = WeiboBlogsSpider(xhr_url, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
            spider.use_abuyun_proxy()
            spider.add_request_header()
            spider.use_cookie_from_curl(cache.hget(WEIBO_COOKIES, account))
            # spider.use_cookie_from_curl(test_curls.get(account))
            status = spider.gen_html_source(raw=True)
            if status == 404:
                continue
            elif status in [20003, -404]:  # blocked or abnormal account
                cache.rpush(WEIBO_URLS_CACHE, job)
            res = spider.parse_tweet_list(cache)
            if len(res) == 2:
                cache.rpush(WEIBO_INFO_CACHE, pickle.dumps(res))
        except ValueError as e:
            print e  # print e.message
            error_count += 1
            cache.rpush(WEIBO_URLS_CACHE, job)
        except RedisException as e:
            print str(e); break
        except Exception as e:  # no matter what was raised, cannot let process died
            traceback.print_exc()
            error_count += 1
            print 'Failed to parse job: ', job
            cache.rpush(WEIBO_URLS_CACHE, job) # put job back
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
            print '>'*10, 'Exceed 1000 times of write errors', '<'*10
            break
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Write Blogs Process pid is %d" % (cp.pid)
        res = cache.blpop(WEIBO_INFO_CACHE, 0)[1]
        info = pickle.loads(res)
        try:
            blogs = info['blogs']
            users = info['users']
            print 'Blogs\' length', len(blogs)
            print 'Users\' length', len(users)
            if blogs:
                dao.insert_blogs_into_db(blogs)
            if users:
                dao.update_user_info(users)
        except Exception as e:  # won't let you died
            error_count += 1
            print 'Failed to write result: ', info
            cache.rpush(WEIBO_INFO_CACHE, res)


def add_jobs(target):
    todo = 0
    dao = WeiboBlogsWriter(USED_DATABASE)
    for job in dao.read_urls_from_db():  # iterate
        todo += 1
        target.rpush(WEIBO_URLS_CACHE, job)
    print 'There are totally %d jobs to process' % todo
    return todo


def run_all_worker():
    r = redis.StrictRedis(**USED_REDIS)
    if not r.llen(WEIBO_URLS_CACHE):
        add_jobs(r)
        print "Add jobs DONE, and I quit..."
        return 0
    else:
        print "Redis has %d records in cache" % r.llen(WEIBO_URLS_CACHE)
    # init_current_account(r)
    job_pool = mp.Pool(processes=4,
        initializer=generate_info, initargs=(r, ))
    result_pool = mp.Pool(processes=2, 
        initializer=write_data, initargs=(r, ))

    cp = mp.current_process()
    print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Run All Works Process pid is %d" % (cp.pid)
    try:
        job_pool.close(); result_pool.close()
        job_pool.join(); result_pool.join()
        print "+"*10, "jobs' length is ", r.llen(WEIBO_URLS_CACHE) 
        print "+"*10, "results' length is ", r.llen(WEIBO_INFO_CACHE)
    except Exception as e:
        traceback.print_exc()
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Exception raise in run all Work"
    except KeyboardInterrupt:
        print dt.now().strftime("%Y-%m-%d %H:%M:%S"), "Interrupted by you and quit in force, but save the results"
        print "+"*10, "jobs' length is ", r.llen(WEIBO_URLS_CACHE) 
        print "+"*10, "results' length is ", r.llen(WEIBO_INFO_CACHE)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo Tweets" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Totally Scraped Weibo Tweets Time Consumed : %d seconds" % (time.time() - start), "*"*10

"""
{
    "ok": 0, 
    "errno": "200002", 
    "msg": "帐号异常已被冻结", 
    "extra": "{"level":8, "type":8,"ok":1}"
}
"""