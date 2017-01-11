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

test_curls = {
    'yufan772684295@163.com': "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483766502129' -H 'Cookie: ALF=1486358494; SUB=_2A251dAqNDeTxGeNH41cS8i_IzjiIHXVWlpbFrDV8PUJbkNANLUzckW14YmCroOeDxnfPnnnM_cIYVcGpPA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhzsxSIhbxXhNIH0pOJ49oQ5JpX5oz75NHD95Qf1Knfe0zpSh-XWs4Dqcj_i--ciKyhiKnNi--fiKn4i-8Wi--RiKnXiKysi--Ri-i2i-i2i--Ri-zfi-zX; _T_WM=75cec17c859070fe50a23a83b2f3b128; YF-Page-G0=0acee381afd48776ab7a56bd67c2e7ac; YF-Ugrow-G0=169004153682ef91866609488943c77f' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    'tuocheng6322566@163.com':"curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483766553281' -H 'Cookie: ALF=1486358543; SUB=_2A251dAtfDeTxGeNH41MY9S_IyjSIHXVWlpUXrDV8PUJbkNANLRTWkW2htWfN-1z_q0dU-w9V9DkR1HId_g..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFY5TyF2EgOd9Ih3BWBgIlf5JpX5oz75NHD95Qf1Knp1K-pSh2RWs4Dqcj_i--RiKnEi-2Ei--4i-24i-zXi--fiK.fiKyWi--NiKnfi-2Ni--RiKnpiKLW; _T_WM=bc1c8c97c91bcb0c51bca94de22c3dfe; YF-Page-G0=0dccd34751f5184c59dfe559c12ac40a; YF-Ugrow-G0=169004153682ef91866609488943c77f' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    'furaotanghuang@163.com': "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483859931495' -H 'Cookie: ALF=1486451923; SUB=_2A251dZeDDeTxGeNH41MY9SzMyziIHXVWmTnLrDV8PUJbkNBeLRPukW0iT_pyS_a4eF835naOcEsUUZYzMA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WFucw7Tumv51AnGlOccmcn65JpX5oz75NHD95Qf1Knp1K-Eeh5XWs4Dqcj_i--fi-ihiKn7i--NiKyhi-88i--Xi-iFiKyWi--4iKL8i-27i--fi-z7iKys; _T_WM=add2f32705b059b9876672771aa49e85; YF-Page-G0=140ad66ad7317901fc818d7fd7743564; YF-Ugrow-G0=ea90f703b7694b74b62d38420b5273df' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    'leyan6054392@163.com': "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483860006785' -H 'Cookie: ALF=1486451999; SUB=_2A251dZhwDeTxGeNH41MY9SzJyj-IHXVWmTg4rDV8PUJbkNANLUbwkW2PbDRkSwX7dwQ4TQUPF1dZV9_iYw..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhZf6aScMTBzBiF3SI9anTx5JpX5oz75NHD95Qf1Knp1K-ESK20Ws4Dqcj_i--ci-zpi-8Wi--Ni-ihi-27i--4i-zRiKLsi--fiK.0iKnXi--ciKnEiKLs; _T_WM=0015e37ddb739cbe72bca783e769258e; YF-Page-G0=46f5b98560a83dd9bfdd28c040a3673e; YF-Ugrow-G0=ad83bc19c1269e709f753b172bddb094' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    'di82358502@163.com': "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483860091024' -H 'Cookie: ALF=1486452089; SUB=_2A251dZgpDeTxGeNH41YT9C3MzTqIHXVWmThhrDV8PUJbkNBeLWvBkW0UiN0apbelnEMDZ1pG7xp99ogWdA..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5pKmw65bJuBolewMYB6c_y5JpX5oz75NHD95Qf1KnXeoB0ehqcWs4Dqcj_i--Ri-i8iKn4i--Xi-ihiKL8i--RiKnEi-27i--Ri-8siKnci--ci-82i-2f; _T_WM=2324a93889f9aa9980ad5d32a948a5bc; YF-Page-G0=35f114bf8cf2597e9ccbae650418772f; YF-Ugrow-G0=ad83bc19c1269e709f753b172bddb094' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    'yunluao954128@163.com': "curl 'http://weibo.com/aj/v6/comment/big?ajwvr=6&id=4060589758164164&from=singleWeiBo&__rnd=1483860156874' -H 'Cookie: ALF=1486452150; SUB=_2A251dZjmDeTxGeNH41EQ9S7KyzuIHXVWmTiurDV8PUJbkNBeLXLXkW2dAsjR9BS8DcMH9RC0zEMhjOWtIg..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhW4gbPHppK4xd7wL19.o-p5JpX5oz75NHD95Qf1Kn0eK-7So5NWs4DqcjdSh-0eoMR1K5Ei--NiK.Xi-2Ri--ciKnRi-zN; _T_WM=e710d0db2aa512f4e4b0d1540d535545; YF-Page-G0=0acee381afd48776ab7a56bd67c2e7ac; YF-Ugrow-G0=57484c7c1ded49566c905773d5d00f82' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/1767076672/EpqdpyfS4?type=comment' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
    # {"code":"100003","msg":"","data":"http:\/\/weibo.com\/sorry?userblock&code=20003"}
}

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
            dao.update_url_to_mid(mid, job)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    single_process_mid()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
