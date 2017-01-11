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
    # 'yijipiaoliao@163.com': "curl 'http://m.weibo.cn/container/getIndex?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'Cookie: SUB=_2A251cMr6DeTxGeNH41cS8SvNwz2IHXVWmtayrDV6PUJbkdBeLXP3kW1GQSV2TN29WYOzXiCzy_7bcN9ucg..; SUHB=0vJYC7zGfOUBmj; SCF=Ah6MUIBoeOpv_szSLd4RZTsSkMMcJ33C94QiwgBGDaKniU3I0zIccn3xOF8rPAP9WcZTdNmp99ySurQb58GuHQ0.; SSOLoginState=1484044970; _T_WM=15062623c58522a28021e8e2f676910c; H5_INDEX=2; H5_INDEX_TITLE=%E5%B1%91%E5%91%98%E6%95%91%E6%96%99%E6%85%95; M_WEIBOCN_PARAMS=featurecode%3D20000180%26oid%3D4062311339496883%26luicode%3D10000011%26lfid%3D1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro%26fid%3D2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Accept: application/json, text/plain, */*' -H 'Referer: http://m.weibo.cn/p/index?containerid=2305301008087da2d6c9e74a5a22d510812b82a08c21__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&uid=5985315181&luicode=10000011&lfid=1073037da2d6c9e74a5a22d510812b82a08c21_-_ext_intro&featurecode=20000180' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed",
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


def single_process():
    cache = redis.StrictRedis(**USED_REDIS)
    all_account = test_curls.keys()
    dao = WeiboBlogsWriter(USED_DATABASE)
    account = random.choice(all_account)
    job = "http://weibo.com/p/10080830f87ddf8125400be816dcdacfec66a8"
    xhr_url = xhrize_topic_url(job)
    print xhr_url
    for i in range(2, 10):
        spider = WeiboBlogsSpider(xhr_url + "&page=%d" % i, account, WEIBO_ACCOUNT_PASSWD, timeout=20, delay=3)
        spider.use_abuyun_proxy()
        spider.add_request_header()
        # spider.use_cookie_from_curl(cache.hget(MANUAL_COOKIES, account))
        spider.use_cookie_from_curl(test_curls.get(account))
        status = spider.gen_html_source(raw=True)
        if status == 404:
            return 
        res = spider.parse_tweet_list(cache)
        if len(res) == 2:
            blogs = res['blogs']
            users = res['users']
            if blogs:
                print blogs
                dao.insert_blogs_into_db(blogs)
            if users:
                print users
                dao.update_user_info(users)


if __name__=="__main__":
    print "\n\n" + "%s Began Scraped Weibo New Users" % dt.now().strftime("%Y-%m-%d %H:%M:%S") + "\n"
    start = time.time()
    single_process()
    print "*"*10, "Totally Scraped Weibo New Users Time Consumed : %d seconds" % (time.time() - start), "*"*10
