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
    TWEETS_COOKIES,
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
    "mupiaotao@163.com": "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'Cookie: SUB=_2A251cxrLDeTxGeNH41cS8SfIzjmIHXVWn6aDrDV6PUJbkdBeLRfjkW1c3jPnbjulH9DAOkSq7KOOYQZQaQ..; SUHB=06ZVcjQOYVmqxL; SCF=Ar7CZL9C2fuvO3ecC8cfeishTZ_PJG4kcAdg_DtTHpIury40L9hQvevEbp8BkVd4L_fq0J3I1JNTUsHvmgnpJHU.; SSOLoginState=1484221083; _T_WM=a3c8a35b63c45070987c8a18b9fcefa5; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26featurecode%3D20000180%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    "shanshun59607@163.com": "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'Cookie: SUB=_2A251cxz0DeTxGeNH41cS8i7FyD2IHXVWn6S8rDV6PUJbkdBeLWz-kW01SQXet_wFKPhe4lR9MXRq_MTU5g..; SUHB=0D-Vsi2J6AtDx8; SCF=AtvWh3APEyfDu0-j9pC5U0mTLLDVpKTdgjG7rAWFmLicGIKDEpv1AnMpvI10_tdRoOWkz97xhPd6PdyJC1YqJE4.; SSOLoginState=1484221604; _T_WM=ea8d1d5927fb3ec2eb7a8acf253bcece; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26featurecode%3D20000180%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    "leizi377759@163.com": "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180&since_id=%7B%22last_since_id%22%3A4063182282295810%2C%22res_type%22%3A1%2C%22next_since_id%22%3A4063125407623537%7D' -H 'Cookie: SUB=_2A251cx0FDeTxGeNH41cS8izNzjmIHXVWn6NNrDV6PUJbkdBeLWj3kW0ED0AYonDqWFVnepbaAuQX3yZi_Q..; SUHB=0XD15bVp5jq6EN; SCF=ApNc6B_S43uVxitpjseGiGgFJkJ34ttrcoanDO6nRi_e_4g8004ocJof2HopPZpKbUbrSfVqoZiVWzbbNvg0ESg.; SSOLoginState=1484221781; _T_WM=1d069611f0a0507c6016daaf3354f9d6; M_WEIBOCN_PARAMS=featurecode%3D20000180%26luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    "ri8974212xiongb@163.com": "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'Cookie: SUB=_2A251cx5ODeRxGeNH41cS8izPyzWIHXVWn6IGrDV6PUJbkdBeLVPAkW2YWeAKCUCy6di6i2xCSgZ8GnqxPA..; SUHB=0xnCrzsoZohCcq; SCF=AgzgB78FxCfw0wRP5Bsi6wC0ovFlUQnKFMujzXFSVt-GzRu9n9g6_ePRZp3MwW54RyHpJkfR5Ch8gGm5HBZ_6Eg.; SSOLoginState=1484221982; _T_WM=083e87a98976e4b477f32ffaf1f81e05; M_WEIBOCN_PARAMS=featurecode%3D20000180%26luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
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
            all_account = cache.hkeys(TWEETS_COOKIES)
            # all_account = test_curls.keys()
            account = random.choice(all_account)
            spider = WeiboBlogsSpider(xhr_url, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
            spider.use_abuyun_proxy()
            spider.add_request_header()
            spider.use_cookie_from_curl(cache.hget(TWEETS_COOKIES, account))
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
            topic = info['topic']
            print 'Blogs\' length', len(blogs)
            print 'Users\' length', len(users)
            if blogs:
                dao.insert_blogs_into_db(blogs)
            if users:
                dao.update_user_info(users)
            if topic and len(topic) == 4:
                dao.update_topic_info(topic)
        except Exception as e:  # won't let you died
            traceback.print_exc()
            time.sleep(10)
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