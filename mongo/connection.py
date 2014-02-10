from pymongo import MongoClient
import MySQLdb
import db_info as DB

class dbConnection(object):

    def __init__(self):
       self.m_connections = {}
       self.sql_connections = {}

    def create_mongo_connections(self,options=[]):
        if 'boston' in options:
            self.m_connections['boston'] = MongoClient(host=DB.mongo['host']).boston.tweets

        if 'new_boston' in options:
            self.m_connections['new_boston'] = MongoClient(host=DB.mongo['host']).new_boston.tweets

    def create_sql_connections(options=[]):
        if 'boston' in options:
            sql_db = MySQLdb.connect(host=DB.sql['host'],
                                     user=DB.sql['user'],
                                     passwd=DB.sql['password'],
                                     db=DB.sql['db'])
            self.sql_connections = sql_cursor = sql_db.cursor()
