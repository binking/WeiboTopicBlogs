#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------
import traceback
from datetime import datetime as dt
from zc_spider.weibo_writer import DBAccesor, database_error_hunter


class WeiboBlogsWriter(DBAccesor):

    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)

    def connect_database(self):
        return DBAccesor.connect_database(self)

    @database_error_hunter
    def insert_blogs_into_db(self, mblogs):
        middle = 'second'; bucketName = '微博话题'
        theme = '新浪微博_话题python'
        insert_blog_sql = """
            INSERT INTO Weibo (fullpath, realpath, theme,  middle, 
            createdate, bucketName, uri, weibo_mid,
            weibo_author_nickname, weibo_author_id, weibo_author_url, 
            weibo_author_portrait, weibo_url, weibo_content, sub_date, device, 
            weibo_forward_num, weibo_comment_num, weibo_thumb_up_num, topic_url)
            SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s 
            FROM DUAL WHERE NOT EXISTS(
            SELECT * FROM Weibo where weibo_url = %s)
        """
        insert_relation_sql = """
            INSERT INTO TopicWeiboRelation(topic_url, weibo_url)
            SELECT %s, %s FROM DUAL WHERE not exists (
            SELECT * FROM TopicWeiboRelation WHERE topic_url = %s AND weibo_url = %s)
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        for mblog in mblogs:
            topic_url = mblog['topic_url']
            xhr_url = mblog['xhr_url']
            if cursor.execute(insert_blog_sql,(
                xhr_url, xhr_url, theme, middle, mblog['date'],
                bucketName, topic_url, mblog['mid'], mblog['u_name'],
                mblog['u_id'], mblog['u_url'], mblog['u_img'],
                mblog['url'], mblog['text'], mblog['sub_date'],
                mblog.get('device', ''), mblog['reposts'], mblog['comments'], 
                mblog['likes'],topic_url, mblog['url']
                )):
                print '$'*10, "1. Insert MBlog %s SUCCEED." % mblog['url']
            if cursor.execute(insert_relation_sql, (
                topic_url, mblog['url'], topic_url, mblog['url']
                )):
                print '$'*10, "2. Insert Relation %s SUCCEED." % mblog['url']
        conn.commit(); cursor.close(); conn.close()
        return True

    @database_error_hunter
    def update_user_info(self, users):
        """
        Update users' information, cuz no label
        """
        update_user_sql = """
            UPDATE WeiboUser
            SET nickname=%s, focus_num=%s, fans_num=%s, weibo_num=%s, weibo_user_card=%s
            WHERE weibo_user_url = %s
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        for user in users:
            if cursor.execute(update_user_sql,(
                user['name'], user['follows'], user['fans'], user['blogs'], user['usercard'],
                'http://weibo.com/' + user['usercard']
            )):
                print '$'*10, "3. Update User %s SUCCEED." % user['name']
        conn.commit(); cursor.close(); conn.close()
        return True

    @database_error_hunter
    def update_topic_info(self, topic):
        update_info_sql = """
            UPDATE topicinfo 
            SET read_num=%s, read_num_dec=%s, discussion_num=%s, fans_num=%s, 
            WHERE topic_url=%s
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        if cursor.execute(update_info_sql,(
            topic['read_num'], topic['read_num_dec'], topic['disc_num'], 
            topic['like_num'], topic['url']
            )):
            print '$'*10, "4. Update Topic %s SUCCEED." % topic['url']
            conn.commit(); 
        cursor.close(); conn.close()

    @database_error_hunter
    def read_urls_from_db(self):
        select_sql = """
            SELECT DISTINCT topic_url, createdate FROM topicinfo t
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

    def repair_data(self):
        select_sql = """
            SELECT topic_url, weibo_url
            FROM weibo
            WHERE createdate > '2017-01-10 00:00:00' 
            AND theme LIKE '%python%';
        """
        insert_relation_sql = """
            INSERT INTO TopicWeiboRelation(topic_url, weibo_url)
            SELECT %s, %s FROM DUAL WHERE not exists (
            SELECT * FROM TopicWeiboRelation WHERE topic_url = %s AND weibo_url = %s)
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(select_sql)
        for res in cursor.fetchall():
            if cursor.execute(insert_relation_sql, (
                res[0], res[1], res[0], res[1]
                )):
                print '$'*10, "1. Insert Relation %s SUCCEED." % res[1]
            conn.commit()
        cursor.close(); conn.close()