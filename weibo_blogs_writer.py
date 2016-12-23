#-*- coding: utf-8 -*-
#--------  话题48992  爬取一个话题下的所有微博  --------
import traceback
from datetime import datetime as dt
from template.weibo_writer import DBAccesor, database_error_hunter


class WeiboUserWriter(DBAccesor):

    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)

    def connect_database(self):
        return DBAccesor.connect_database(self)

    @database_error_hunter
    def insert_new_user_into_db(self, info_dict):
        uri = info_dict['uri']; fullpath = uri
        realpath = uri; middle = 'second'; bucketName = '微博话题'
        theme = '新浪微博_话题48992'
        insert_blog_sql = """
            INSERT INTO Weibo (fullpath, realpath, theme,  middle, 
            createdate, pageno, bucketName,uri, 
            weibo_author_nickname, weibo_author_id, weibo_author_url, 
            weibo_author_portrait, weibo_url, weibo_content, sub_date, device, 
            weibo_forward_num, weibo_comment_num, weibo_thumb_up_num, topic_url)
            SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s 
            FROM DUAL WHERE NOT EXISTS(SELECT * FROM Weibo where weibo_url = %s )
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        if cursor.execute(insert_new_user_sql,(
                )):
            print '$'*10, "1. Insert %s SUCCEED." % uri
        conn.commit(); cursor.close(); conn.close()
        return True

    @database_error_hunter
    def read_new_user_from_db(self):
