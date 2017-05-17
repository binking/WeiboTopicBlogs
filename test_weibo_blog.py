import requests
from zc_spider.weibo_utils import gen_abuyun_proxy, extract_post_data_from_curl

test_curls = {
    "13874117403": "curl 'http://d.weibo.com/' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cookie: YF-Page-G0=046bedba5b296357210631460a5bf1d2; SUB=_2A250H4sqDeRhGeBP71sW8CvKyzqIHXVXbPvirDV8PUJbmtANLRnQkW-gv8TaApsanFgWAhDPrW1tMgF8yw..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5jO9DN_2YvvgJIBqHGiUyw5JpX5o2p5NHD95QceKB4S05fSo5cWs4Dqcjqi--RiK.0iKLsi--ci-8hi-2NqH2Rqg-R; SUHB=08JL6cYdExecZw; SSOLoginState=1495006074' -H 'Connection: keep-alive' --compressed",
    "18373273114": "curl 'http://d.weibo.com/' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cookie: YF-Page-G0=fc0a6021b784ae1aaff2d0aa4c9d1f17; SUB=_2A250H4tpDeRhGeBP7lIR8yjEzz2IHXVXbPuhrDV8PUJbmtANLWuhkW91VMdmxEM6EdmNt41tlcO8CjKx4Q..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWijk-VXGVmHWzvwcnpoOS25JpX5o2p5NHD95QceK-7ehec1hBpWs4Dqcjqi--fi-88iKyFi--fi-i2i-829.7fwhef; SUHB=0FQrgQjmg8q5XT; SSOLoginState=1495006009' -H 'Connection: keep-alive' --compressed",
    "binking": "curl 'https://m.weibo.cn/container/getIndex?containerid=230530100808e08305258b94e88031b9fc1c3c081aa1__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&luicode=10000011&lfid=100808e08305258b94e88031b9fc1c3c081aa1_-_ext_intro&featurecode=20000180' -H 'Accept-Encoding: gzip, deflate, sdch, br' -H 'Accept-Language: zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4' -H 'Upgrade-Insecure-Requests: 1' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.36' -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8' -H 'Cache-Control: max-age=0' -H 'Cookie: browser=d2VpYm9mYXhpYW4%3D; ALF=1497494828; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEE464H3H6FSoD4EotizRMQEkqmXu8cs8y4XIJCwiMapZA.; SUB=_2A250H4mDDeRhGeVG7FYT8i_OzzWIHXVX4xfLrDV6PUNbktBeLU_5kW0O7kSz5TxvupzMTuyFzSwEKp7ZYQ..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhxM.AD9EjGmQSc51FnJvMU5JpX5KMhUgL.FoeRS0BEeo2ESh.2dJLoIEBLxK.L1KnLBoeLxKqL1KnL12-LxK-LBo5L1K2LxK-LBo.LBoBt; SUHB=0mxUQ6qkGZtXU4; SSOLoginState=1495005651; _T_WM=ac17c1eff6bb6e8a3318d6ab37351a69; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D100808e08305258b94e88031b9fc1c3c081aa1_-_ext_intro%26featurecode%3D20000180%26oid%3D4093120361217109%26fid%3D230530100808e08305258b94e88031b9fc1c3c081aa1__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011' -H 'Connection: keep-alive' --compressed",
}


query_parameter = {
    "containerid": "230530100808e08305258b94e88031b9fc1c3c081aa1__timeline__mobile_info_-_pageapp%3A23055763d3d983819d66869c27ae8da86cb176",
    "luicode": "10000011",
    "lfid": "100808e08305258b94e88031b9fc1c3c081aa1_-_ext_intro",
    "featurecode": "20000180",
}


request_header = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, sdch, br",
    "Accept-Language": "zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Cookie": "browser=d2VpYm9mYXhpYW4%3D; ALF=1497494828; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEE464H3H6FSoD4EotizRMQEkqmXu8cs8y4XIJCwiMapZA.; SUB=_2A250H4mDDeRhGeVG7FYT8i_OzzWIHXVX4xfLrDV6PUNbktBeLU_5kW0O7kSz5TxvupzMTuyFzSwEKp7ZYQ..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhxM.AD9EjGmQSc51FnJvMU5JpX5KMhUgL.FoeRS0BEeo2ESh.2dJLoIEBLxK.L1KnLBoeLxKqL1KnL12-LxK-LBo5L1K2LxK-LBo.LBoBt; SUHB=0mxUQ6qkGZtXU4; SSOLoginState=1495005651; _T_WM=ac17c1eff6bb6e8a3318d6ab37351a69; M_WEIBOCN_PARAMS=luicode%3D10000011%26lfid%3D100808e08305258b94e88031b9fc1c3c081aa1_-_ext_intro%26featurecode%3D20000180%26oid%3D4093120361217109%26fid%3D230530100808e08305258b94e88031b9fc1c3c081aa1__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176%26uicode%3D10000011",
    "Host": "m.weibo.cn",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.96 Safari/537.36",
}

# url = "https://m.weibo.cn/container/getIndex?containerid=230530100808e08305258b94e88031b9fc1c3c081aa1__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&luicode=10000011&lfid=100808e08305258b94e88031b9fc1c3c081aa1_-_ext_intro&featurecode=20000180"
url = "http://m.weibo.cn/container/getIndex?containerid=230530100808f14fa77e241c9c281dbbb144f98c0125__timeline__mobile_info_-_pageapp%253A23055763d3d983819d66869c27ae8da86cb176&luicode=10000011&lfid=100808f14fa77e241c9c281dbbb144f98c0125_-_ext_intro&featurecode=20000180"

for key in test_curls:
    r = requests.get(url, 
        headers=extract_post_data_from_curl(test_curls[key]), 
        proxies=gen_abuyun_proxy())
    print r.text