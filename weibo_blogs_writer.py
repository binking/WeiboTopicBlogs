#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------
import traceback
from datetime import datetime as dt
from template.weibo_writer import DBAccesor, database_error_hunter


class WeiboBlogsWrite(DBAccesor):

    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)

    def connect_database(self):
        return DBAccesor.connect_database(self)

    @database_error_hunter
    def insert_blogs_into_db(self, two_info):
        uri = info_dict['uri']; fullpath = uri
        realpath = uri; middle = 'second'; bucketName = '微博话题'
        theme = '新浪微博_话题python'
        insert_blog_sql = """
            INSERT INTO Weibo (fullpath, realpath, theme,  middle, 
            createdate, pageno, bucketName,uri, 
            weibo_author_nickname, weibo_author_id, weibo_author_url, 
            weibo_author_portrait, weibo_url, weibo_content, sub_date, device, 
            weibo_forward_num, weibo_comment_num, weibo_thumb_up_num, topic_url)
            SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s 
            FROM DUAL WHERE NOT EXISTS(
            SELECT * FROM Weibo where weibo_url = %s 
            OR weibo_mid = %s)
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        if cursor.execute(insert_new_user_sql,(
                )):
            print '$'*10, "1. Insert %s SUCCEED." % uri
        conn.commit(); cursor.close(); conn.close()
        return True

    @database_error_hunter
    def read_urls_from_db(self):
        select_sql = """
            SELECT DISTINCT topic_url FROM topicinfo t
            -- TopicInfo 没有爬过的Topic数据
            WHERE 1 = 1 AND createdate > date_sub(NOW(), INTERVAL '2' DAY )
            -- AND theme LIKE '新浪微博_热门话题%'
            AND not exists (
            SELECT * FROM topicweiborelation 
            WHERE topic_url = t.topic_url)
            UNION  -- TopicInfo 微博数量太少的话题
            SELECT t.topic_url, max(t.createdate) AS createdate 
            FROM topicinfo t, topicweiborelation tw
            WHERE t.topic_url = tw.topic_url
            AND createdate > date_sub(NOW(), INTERVAL '7' DAY )
            GROUP BY topic_url
            HAVING count(*) < 20 
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(select_sql)
        for url in cursor.fetchall():
            yield url[0]