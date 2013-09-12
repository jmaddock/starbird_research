import MySQLdb
from pymongo import MongoClient
from datetime import datetime
import counter,re

#connect to mongo db
client = MongoClient()
mongo_db = client.boston
collection = mongo_db.tweets
tweets = mongo_db.tweets

#connect to sql db
sql_db = MySQLdb.connect(host="localhost",
					 user="root",
					 passwd="",
					 db="girl_running")
sql_cursor = sql_db.cursor()

#sql db query
sql_cursor.execute("select id,code from tweets_girl_running limit 10")
for x in sql_cursor.fetchall():
	print x