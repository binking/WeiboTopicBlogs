#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------
import os
import re
import json
import time
from bs4 import BeautifulSoup as bs
# source : http://weibo.com/p/{topic_id}
# http://weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=100808
# &from=page_100808&mod=TAB&pagebar=0&tab=emceercd
# &current_page=1&since_id=14
# &pl_name=Pl_Third_App__45&id={topic_id}&script_uri=/p/{topic_id}/emceercd
# &feed_type=1&page=1&pre_page=1&domain_op=100808&__rnd=1483524602908
# http://m.weibo.cn/status/{m_ids}
curl = "curl 'http://weibo.com/p/aj/v6/mblog/mbloglist?ajwvr=6&domain=100808&from=page_100808&mod=TAB&pagebar=1&tab=emceercd&current_page={current_page}&since_id={since_id}&pl_name=Pl_Third_App__11&id={topic_id}&script_uri=/p/{topic_id}/emceercd&feed_type=1&page={page}&pre_page={prepage}&domain_op=100808&__rnd={timestamp}' -H 'Cookie: SINAGLOBAL=7912212257618.43.1478585959985; wb_publish_fist100_5843638692=1; wvr=6; _T_WM=03e781554acf9dd24f1be01327a60a32; YF-Page-G0=d0adfff33b42523753dc3806dc660aa7; _s_tentry=-; Apache=9751347814485.37.1483668519299; ULV=1483668519511:25:3:3:9751347814485.37.1483668519299:1483508239455; YF-Ugrow-G0=8751d9166f7676afdce9885c6d31cd61; WBtopGlobal_register_version=c689c52160d0ea3b; SCF=Ap11mp4UEZs9ZcoafG0iD1wVDGjdyuPuLY8BpwtpvSEEvUHF2uToKM-7WlBpLkmhZ8RBzBoq6rkGPr6RQnLxkPM.; SUB=_2A251aoy0DeTxGeNG71EX8ybKwj6IHXVWAfl8rDV8PUNbmtANLXbhkW-Ca4XWBrg6Mlj9Y8JHL6ezeBXp4A..; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9W5HA7SsRPVzLQ_q6ucc2n_c5JpX5K2hUgL.Fo-RShece0nc1Kz2dJLoI0YLxKqL1KMLBK5LxKqL1hnL1K2LxKBLBo.L12zLxK.L1KnLBoeLxKqL1KnL12-LxK-LBo5L1K2LxK-LBo.LBoBt; SUHB=0sqRRqxSCPeB1B; ALF=1484273507; SSOLoginState=1483668708; un=jiangzhibinking@outlook.com; YF-V5-G0=a9b587b1791ab233f24db4e09dad383c; UOR=,,zhiji.heptax.com' -H 'Accept-Encoding: gzip, deflate, sdch' -H 'Accept-Language: zh-CN,zh;q=0.8' -H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36' -H 'Content-Type: application/x-www-form-urlencoded' -H 'Accept: */*' -H 'Referer: http://weibo.com/p/1008084dfec7719459d5de25cdda4e2a705e28?k=%E8%99%8E%E9%B9%A4%E4%B9%8B%E4%BA%A4&from=501&_from_=huati_topic' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' --compressed --silent"

content = ''
def weibo_blogs_parser(page):
    global content
    continued = True
    temp = json.loads(page)
    if u'下一页' not in temp['data']:
        print 'This is last page'
        continued = False
    parser = bs(temp['data'], 'html.parser')
    divs = parser.find_all('div', attrs={'action-type': 'feed_list_item'})
    import ipdb; ipdb.set_trace()
    blogs_list = []
    for div in divs:
        info = {}
        info['mid'] = div.get('mid')
        sub_date_link = div.find('a', attrs={'node-type': 'feed_list_item_date'})
        info['date'] = sub_date_link.get('title') if sub_date_link else ''
        info['m_url'] = sub_date_link.get('href') if sub_date_link else ''
        nick_name_link = div.find('a', attrs={'class': 'W_f14 W_fb S_txt1'})
        info['nickname'] = nick_name_link.get('nick-name') if nick_name_link else ''
        info['usercard'] = nick_name_link.get('usercard') if nick_name_link else ''
        info['user_url'] = nick_name_link.get('href') if nick_name_link else ''
        text_div = div.find('div', attrs={'node-type': "feed_list_content"})
        info['text'] = text_div.text if text_div else ''
        content += info['text']
        forward_tag = div.find(attrs={'node-type': 'forward_btn_text'})
        forward_rexp = re.search(r'(\d+)', forward_tag.text) if forward_tag else None
        info['forward'] = forward_rexp.group(1) if forward_rexp else 0
        comment_tag = div.find(attrs={'node-type': 'comment_btn_text'})
        comment_rexp = re.search(r'(\d+)', comment_tag.text) if comment_tag else None
        info['comment'] = comment_rexp.group(1) if comment_rexp else 0
        like_tag = div.find(attrs={'node-type': 'like_status'})
        like_rexp = re.search(r'(\d+)', like_tag.text) if like_tag else None
        info['like'] = like_rexp.group(1) if like_rexp else 0
        for k, v in info.items():
            print '(Key)%s : (Value)%s' % (k, v)
        blogs_list.append(info)
    return continued, blogs_list


def main():
    since_id = 0
    url = "http://weibo.com/p/1008084dfec7719459d5de25cdda4e2a705e28"
    topic_id = url.split('/')[-1]
    timestamp = 1483523126129
    for page in range(1, 10):
        for sub_index in range(3):
            current_page = sub_index + (page-1) * 3
            # print curl.format(page=page, prepage=page, current_page=current_page, since_id=since_id, topic_id=topic_id, timestamp=timestamp)
            html = os.popen(curl.format(page=page, prepage=page, current_page=current_page, since_id=since_id, topic_id=topic_id, timestamp=timestamp)).read()
            print html
            # import ipdb; ipdb.set_trace()
            is_continuned, res = weibo_blogs_parser(html)
            if not is_continuned:  # last page
                return 
            since_id += len(res)
            print "Since id is ", since_id
    print >>open('multiple_texts.text', 'w'), content.encode('utf8')

if __name__=='__main__':
    main()
