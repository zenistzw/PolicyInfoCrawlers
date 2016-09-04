# -*- coding: utf-8 -*-
import logging
import pymysql

logging.basicConfig(level=logging.DEBUG,  
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',  
                    datefmt='%a, %d %b %Y %H:%M:%S',  
                    filename='mof.log',
                    filemode='a')


class MySQL(object):

    def __init__(self, username, password, host='localhost', port=3306, charset='utf8'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.charset = charset
        self.conn = object

    def connect_to_database_server(self):
        try:
            self.conn = pymysql.connect(host=self.host, port=self.port, user=self.username, passwd=self.password, charset=self.charset)
            logging.info('数据库服务器连接成功')
        except pymysql.OperationalError as e:
            logging.error('status code: {0}, reason: {1}'.format(e[0], e[1]))
            logging.error('数据库服务器连接失败，请检查用户信息')

    def change_database(self, db):
        try:
            self.conn.select_db(db)
            logging.info('连接到{0}数据库'.format(db))
        except pymysql.OperationalError as e:
            logging.error('status code: {0}, reason: {1}'.format(e[0], e[1]))
            logging.error('数据库不存在')  

    def commit_to_database(self, sql, data_list):
        cur = self.conn.cursor()
        logging.info('正在提交数据至目标数据表...')
        try:
            cur.executemany(sql, data_list)
            self.conn.commit()
            logging.info('数据提交完毕')
        except pymysql.ProgrammingError as e: 
            self.conn.rollback()
            logging.error('数据提交失败')
        finally:
            cur.close()

    def query_from_database(self, sql):
        q = []
        cur = self.conn.cursor()
        logging.info('正在提取数据表内的目标数据...')
        try:
            cur.execute(sql)
            q = cur.fetchall()
            logging.info('数据提取完毕')
        except pymysql.ProgrammingError as e:
            logging.error('status code: {0}, reason: {1}'.format(e[0], e[1]))
            logging.error('数据提取失败')
        finally:
            cur.close()
        return q

    def disconnect_from_database_server(self):
        self.conn.close()
        logging.info('断开与数据库服务器的连接')
