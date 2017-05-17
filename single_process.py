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
    WEIBO_ACCOUNT_PASSWD,
    WEIBO_URLS_CACHE, WEIBO_INFO_CACHE,  # weibo:blog:urls, weibo:blog:info
    QCLOUD_MYSQL, LOCAL_REDIS, QCLOUD_REDIS
)
from weibo_blogs_spider_v3 import WeiboBlogsSpider
from weibo_blogs_writer import WeiboBlogsWriter
reload(sys)
sys.setdefaultencoding('utf-8')

if os.environ.get('SPIDER_ENV') == 'test':
    print "*"*10, "Run in Test environment"
    USED_REDIS = LOCAL_REDIS
elif 'centos' in os.environ.get('HOSTNAME'): 
    print "*"*10, "Run in Qcloud environment"
    USED_REDIS = QCLOUD_REDIS
else:
    raise Exception("Unknown Environment, Check it now...")
USED_DATABASE = QCLOUD_MYSQL
test_curls = {
    # "13874117403": "curl 'http://d.weibo.com/' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cookie: YF-Page-G0=046bedba5b296357210631460a5bf1d2; SUB=_2A250H4sqDeRhGeBP71sW8CvKyzqIHXVXbPvirDV8PUJbmtANLRnQkW-gv8TaApsanFgWAhDPrW1tMgF8yw..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5jO9DN_2YvvgJIBqHGiUyw5JpX5o2p5NHD95QceKB4S05fSo5cWs4Dqcjqi--RiK.0iKLsi--ci-8hi-2NqH2Rqg-R; SUHB=08JL6cYdExecZw; SSOLoginState=1495006074' -H 'Connection: keep-alive' --compressed",
    # "18373273114": "curl 'http://d.weibo.com/' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cookie: YF-Page-G0=fc0a6021b784ae1aaff2d0aa4c9d1f17; SUB=_2A250H4tpDeRhGeBP7lIR8yjEzz2IHXVXbPuhrDV8PUJbmtANLWuhkW91VMdmxEM6EdmNt41tlcO8CjKx4Q..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWijk-VXGVmHWzvwcnpoOS25JpX5o2p5NHD95QceK-7ehec1hBpWs4Dqcjqi--fi-88iKyFi--fi-i2i-829.7fwhef; SUHB=0FQrgQjmg8q5XT; SSOLoginState=1495006009' -H 'Connection: keep-alive' --compressed",
    "binking": "curl 'https://m.weibo.cn/container/getIndex?containerid=230530100808e08305258b94e88031b9fc1c3c081aa1__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&luicode=10000011&lfid=100808e08305258b94e88031b9fc1c3c081aa1_-_ext_intro&featurecode=20000180' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: browser=d2VpYm9mYXhpYW4%3D; ALF=1497494828; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEE464H3H6FSoD4EotizRMQEkqmXu8cs8y4XIJCwiMapZA.; SUB=_2A250H4mDDeRhGeVG7FYT8i_OzzWIHXVX4xfLrDV6PUNbktBeLU_5kW0O7kSz5TxvupzMTuyFzSwEKp7ZYQ..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhxM.AD9EjGmQSc51FnJvMU5JpX5KMhUgL.FoeRS0BEeo2ESh.2dJLoIEBLxK.L1KnLBoeLxKqL1KnL12-LxK-LBo5L1K2LxK-LBo.LBoBt; SUHB=0mxUQ6qkGZtXU4; SSOLoginState=1495005651; _T_WM=ac17c1eff6bb6e8a3318d6ab37351a69; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D100808e08305258b94e88031b9fc1c3c081aa1_-_ext_intro%26featurecode%3D20000180%26oid%3D4093120361217109%26fid%3D230530100808e08305258b94e88031b9fc1c3c081aa1__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Connection: keep-alive' --compressed",
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
    job = "http://weibo.com/p/" + "100808e08305258b94e88031b9fc1c3c081aa1"
    xhr_url = xhrize_topic_url(job)
    spider = WeiboBlogsSpider(xhr_url, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
    spider.use_abuyun_proxy()
    spider.add_request_header()
    spider.use_cookie_from_curl(test_curls.get(account))
    import ipdb; ipdb.set_trace()
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
            # print blogs
            dao.insert_blogs_into_db(blogs)
        if users:
            # print users
            dao.update_user_info(users)
        if topic and len(topic) == 4:
            print topic
            dao.update_topic_info(topic)

if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Tweets" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    single_process()
    print "*"*10, "Totally Scraped Weibo New Tweets Time Consumed : %d seconds" % (time.time() - start), "*"*10
