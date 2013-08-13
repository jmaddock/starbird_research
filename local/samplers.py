import db_info, MySQLdb, random

db = MySQLdb.connect(host=db_info.host, # your host, usually localhost
                     user=db_info.user, # your username
                      passwd=db_info.passwd, # your password
                      db=db_info.db) # name of the data base

cur = db.cursor() 			#cursor object for mysql query

def old_sampler(cur):
	f = open('data/#boston_tweet_sample.csv', 'w')	#create new csv
	f.write('Author,Time,Text/Content,Place\n')
	high = 12304132
	low = 4211778
	for i in range(0,500):
		id = random.randint(low, high)
		cur.execute("select author,time,text,place from tweets where id = " + str(id) + " and text like '%#boston %'")
		result = cur.fetchall()[0]
		print result
		f.write('"%s",%s,"%s","%s"\n' % (result[0],result[1],result[2],result[3]))

def sampler(cur):
	f = open('data/#boston_tweet_sample.csv', 'w')	#create new csv
	f.write('Author,Time,Text/Content,Place\n')
	cur.execute("select author,time,text,place from tweets where (id between 4211778 and 12304132) and text like ('%boston %') order by rand() limit 500")
	for result in cur.fetchall():
		print result
		f.write('"%s",%s,"%s","%s"\n' % (result[0],result[1],result[2],result[3]))

def unique_sampler(cur):
	f = open('data/#boston_tweet_sample_no_RT.csv', 'w')	#create new csv
	f.write('Author,Time,Text/Content,Place\n')
	cur.execute("select author,time,text,place from tweets where (id between 4211778 and 12304132) and text like ('%boston %') and text not like ('%RT @%') or ('%via @%') or ('%\\\"@%') order by rand() limit 500")
	for result in cur.fetchall():
		print result
		f.write('"%s",%s,"%s","%s"\n' % (result[0],result[1],result[2],result[3]))

if __name__ == "__main__":
    write_test()