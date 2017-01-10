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


# test_curls = {
#     'yufan772684295@163.com': "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483766502129' -H 'Cookie: ALF=1486358494; SUB=_2A251dAqNDeTxGeNH41cS8i_IzjiIHXVWlpbFrDV8PUJbkNANLUzckW14YmCroOeDxnfPnnnM_cIYVcGpPA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhzsxSIhbxXhNIH0pOJ49oQ5JpX5oz75NHD95Qf1Knfe0zpSh-XWs4Dqcj_i--ciKyhiKnNi--fiKn4i-8Wi--RiKnXiKysi--Ri-i2i-i2i--Ri-zfi-zX; _T_WM=75cec17c859070fe50a23a83b2f3b128; YF-Page-G0=0acee381afd48776ab7a56bd67c2e7ac; YF-Ugrow-G0=169004153682ef91866609488943c77f' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
#     'tuocheng6322566@163.com':"curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483766553281' -H 'Cookie: ALF=1486358543; SUB=_2A251dAtfDeTxGeNH41MY9S_IyjSIHXVWlpUXrDV8PUJbkNANLRTWkW2htWfN-1z_q0dU-w9V9DkR1HId_g..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFY5TyF2EgOd9Ih3BWBgIlf5JpX5oz75NHD95Qf1Knp1K-pSh2RWs4Dqcj_i--RiKnEi-2Ei--4i-24i-zXi--fiK.fiKyWi--NiKnfi-2Ni--RiKnpiKLW; _T_WM=bc1c8c97c91bcb0c51bca94de22c3dfe; YF-Page-G0=0dccd34751f5184c59dfe559c12ac40a; YF-Ugrow-G0=169004153682ef91866609488943c77f' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
#     'furaotanghuang@163.com': "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483859931495' -H 'Cookie: ALF=1486451923; SUB=_2A251dZeDDeTxGeNH41MY9SzMyziIHXVWmTnLrDV8PUJbkNBeLRPukW0iT_pyS_a4eF835naOcEsUUZYzMA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFucw7Tumv51AnGlOccmcn65JpX5oz75NHD95Qf1Knp1K-Eeh5XWs4Dqcj_i--fi-ihiKn7i--NiKyhi-88i--Xi-iFiKyWi--4iKL8i-27i--fi-z7iKys; _T_WM=add2f32705b059b9876672771aa49e85; YF-Page-G0=140ad66ad7317901fc818d7fd7743564; YF-Ugrow-G0=ea90f703b7694b74b62d38420b5273df' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
#     'leyan6054392@163.com': "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483860006785' -H 'Cookie: ALF=1486451999; SUB=_2A251dZhwDeTxGeNH41MY9SzJyj-IHXVWmTg4rDV8PUJbkNANLUbwkW2PbDRkSwX7dwQ4TQUPF1dZV9_iYw..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhZf6aScMTBzBiF3SI9anTx5JpX5oz75NHD95Qf1Knp1K-ESK20Ws4Dqcj_i--ci-zpi-8Wi--Ni-ihi-27i--4i-zRiKLsi--fiK.0iKnXi--ciKnEiKLs; _T_WM=0015e37ddb739cbe72bca783e769258e; YF-Page-G0=46f5b98560a83dd9bfdd28c040a3673e; YF-Ugrow-G0=ad83bc19c1269e709f753b172bddb094' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
#     'di82358502@163.com': "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483860091024' -H 'Cookie: ALF=1486452089; SUB=_2A251dZgpDeTxGeNH41YT9C3MzTqIHXVWmThhrDV8PUJbkNBeLWvBkW0UiN0apbelnEMDZ1pG7xp99ogWdA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5pKmw65bJuBolewMYB6c_y5JpX5oz75NHD95Qf1KnXeoB0ehqcWs4Dqcj_i--Ri-i8iKn4i--Xi-ihiKL8i--RiKnEi-27i--Ri-8siKnci--ci-82i-2f; _T_WM=2324a93889f9aa9980ad5d32a948a5bc; YF-Page-G0=35f114bf8cf2597e9ccbae650418772f; YF-Ugrow-G0=ad83bc19c1269e709f753b172bddb094' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
#     'yunluao954128@163.com': "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483860156874' -H 'Cookie: ALF=1486452150; SUB=_2A251dZjmDeTxGeNH41EQ9S7KyzuIHXVWmTiurDV8PUJbkNBeLXLXkW2dAsjR9BS8DcMH9RC0zEMhjOWtIg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhW4gbPHppK4xd7wL19.o-p5JpX5oz75NHD95Qf1Kn0eK-7So5NWs4DqcjdSh-0eoMR1K5Ei--NiK.Xi-2Ri--ciKnRi-zN; _T_WM=e710d0db2aa512f4e4b0d1540d535545; YF-Page-G0=0acee381afd48776ab7a56bd67c2e7ac; YF-Ugrow-G0=57484c7c1ded49566c905773d5d00f82' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
# }
test_curls = {
    "binking": "curl 'http://m.weibo.cn/container/getIndex?containerid=1008089e50aa0184d42b2f4dde38deaf43cd4b' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cookie: _T_WM=3ba14b38ec55ecdada6c00655da0e0be; H5_INDEX=2; H5_INDEX_TITLE=%E6%97%A0%E6%88%91%E4%B9%8B%E9%98%B3%E6%98%8E%E5%B0%91%E5%B9%B4; browser=d2VpYm9mYXhpYW4%3D; ALF=1486609618; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEEAtegq1jcG9Td1LMOG9cCKsOs5-Uof6Rnu5HD-L_i7Z0.; SUB=_2A251cD-_DeTxGeNG71EX8ybKwj6IHXVWm0H3rDV6PUJbktBeLXnykW1xTV4f5mp7D7Qeomfo4czAqxGHog..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5o2p5NHD95Qf1hB0SoeRSo.EWs4Dqcj6i--ciK.Ni-27i--ciKnRiK.pi--Xi-z4iKyFi--4iK.Ri-z0i--ciK.RiKy8i--fi-z7iK.pi--fi-z4i-zX; SUHB=0y0JooghY76aiR; SSOLoginState=1484017647; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D1073039e50aa0184d42b2f4dde38deaf43cd4b_-_ext_intro%26fid%3D1008089e50aa0184d42b2f4dde38deaf43cd4b%26uicode%3D10000011' -H 'Connection: keep-alive' --compressed",
}


def xhrize_topic_url(url):
    # Given a topic url like "http://weibo.com/p/100808289ad9eb102dffb586f850ab6b4183f4"
    if 'getIndex' in url:
        return url
    xhr_url = 'http://m.weibo.cn/container/getIndex?containerid='
    topic_id = url.split('/')[-1]
    return xhr_url + topic_id


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
            all_account = cache.hkeys(MANUAL_COOKIES)
            # all_account = test_curls.keys()
            account = random.choice(all_account)
            spider = WeiboBlogsSpider(xhr_url, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
            spider.use_abuyun_proxy()
            spider.add_request_header()
            spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
            # spider.use_cookie_from_curl(test_curls.get(account))
            status = spider.gen_html_source(raw=True)
            if status == 404:
                continue
            elif status in [20003, -404]:  # blocked or abnormal account
                cache.rpush(WEIBO_URLS_CACHE, job)
            res = spider.parse_tweet_list(cache)
            if len(res) == 2:
                cache.rpush(WEIBO_INFO_CACHE, pickle.dumps(res))
            time.sleep(1)
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
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    run_all_worker()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
