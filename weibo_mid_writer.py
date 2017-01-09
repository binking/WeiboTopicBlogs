#-*- coding: utf-8 -*-
import traceback
from datetime import datetime as dt
from zc_spider.weibo_writer import DBAccesor, database_error_hunter


class WeiboMidWriter(DBAccesor):

    def __init__(self, db_dict):
        DBAccesor.__init__(self, db_dict)

    def connect_database(self):
        return DBAccesor.connect_database(self)

    @database_error_hunter
    def update_url_to_mid(self, mid, uri):
        update_mid_sql = """
            UPDATE Weibo SET weibo_mid=%s WHERE weibo_url=%s
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        if cursor.execute(update_mid_sql,(mid, uri)):
            print '$'*10, "1. Update %s SUCCEED." % uri
        conn.commit(); cursor.close(); conn.close()
        return True

    @database_error_hunter
    def read_new_user_from_db(self):
        select_sql = """
            SELECT weibo_url FROM Weibo 
            WHERE weibo_mid IS NULL
            # ORDER BY id
            # LIMIT 10
        """
        conn = self.connect_database()
        cursor = conn.cursor()
        cursor.execute(select_sql)
        for uri in cursor.fetchall():
            yield uri[0]