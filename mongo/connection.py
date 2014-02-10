from pymongo import MongoClient
import MySQLdb

class dbConnection(object):

    def __init__(self):
       self.m_connections = {}
       self.sql_connections = {}

    def create_mongo_connections(self,options=[]):
        if 'boston' in options:
            self.m_connections['boston'] = MongoClient(host='10.60.0.51').boston.tweets

        if 'new_boston' in options:
            self.m_connections['new_boston'] = MongoClient(host='10.60.0.51').new_boston.tweets

    def create_sql_connections(options=[]):
        if 'boston' in options:
            sql_db = MySQLdb.connect(host="localhost",
                                     user="jim",
                                     passwd="sidewinder11",
                                     db="boston")
            self.sql_connections = sql_cursor = sql_db.cursor()
