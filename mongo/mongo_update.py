import MySQLdb
from pymongo import MongoClient

#connect to mongo db
client = MongoClient()
mongo_db = client.boston
collection = mongo_db.tweets
tweets = mongo_db.tweets

#connect to sql db
sql_db = MySQLdb.connect(host="localhost",
					 user="root",
					 passwd="",
			                 db="craft_seals")
sql_cursor = sql_db.cursor()

written_ids = open('written_ids_seals_craft.txt','w')
#sql db query
sql_cursor.execute("select id,code from tweets_seals")
for x in sql_cursor.fetchall():
	query = str(x[0])
	value = str(x[1])
	print query,value
	written_ids.write('"%s","%s"\n' % (query,value))
	tweets.update({'user.id':query},{'$push':{'codes':{'rumor':'seals/craft','code':value}}})
