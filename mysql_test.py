import db_info
import MySQLdb
import collections
from string import punctuation

db = MySQLdb.connect(host=db_info.host, # your host, usually localhost
                     user=db_info.user, # your username
                      passwd=db_info.passwd, # your password
                      db=db_info.db) # name of the data base

cur = db.cursor() 			#cursor object for mysql query
twitter_punct = punctuation.replace('#','')	

# calculate the number of tweets for each hour and write to a .csv
def tweets_vs_time(cur):
	f = open('data/data.csv', 'w')	#create new csv
	f.write('time,count\n')

	for i in range(15,23):
		for j in range(0,24):
			dateStart = '2013-04-%02d %02d'  % (i,j)
			print "query: select count(*) from tweets where time like '" + dateStart + "%'"
			cur.execute("select count(*) from tweets where time like '" + dateStart + "%'")
			for row in cur.fetchall():
				dataset.append({'time':dateStart, 'count':row})
				result = (str(dateStart) + ',' + '%s\n') % (row[0],)
				f.write(result)

	print 'done'
 
#calculate most common words
def word_frequency(cur):
	f = open('data/word_frequency.csv', 'w')	#create new csv
	f.write('word,count\n')
	count = collections.Counter()
	stopwords = "a,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,yesterday,today".split(',')
	#punctuation = ["'",":", ",",".","!","?",'"',';'] 

	cur.execute("select text from tweets")
	print 'query done'
	for (i,row) in enumerate(cur.fetchall()):
		s = row[0].split()
		result = [x.lower() for x in s if not x in stopwords]
			#if x in stopwords:
			#	s.remove(x)
		count.update(result)
		print 'row: %d' % i

	for x in count.most_common(1000):
		result = '%s,%s\n' % (x[0],x[1])
		f.write(result)

#calculate most common hashtags or mentions
def hashtag_frequency(cur):
	f = open('data/attag_frequency.csv', 'w')	#create new csv
	f.write('user,count\n')
	count = collections.Counter()
	#stopwords = "a,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,yesterday,today".split(',')

	cur.execute("select text from tweets where text like '%@%'")
	print 'query done'
	for (i,row) in enumerate(cur.fetchall()):
		s = row[0].split()
		result = [x.lower() for x in s if x.find('@') > -1]
			#if x in stopwords:
			#	s.remove(x)
		count.update(result)
		print 'row: %d' % i

	for x in count.most_common(1000):
		result = '"%s","%s"\n' % (x[0],x[1])
		f.write(result)	

#calculate most common unique hashtags (not retweeted, filtered with RT)
def hashtag_histogram(cur):
	f = open('data/hashtag_histogram.csv', 'w')	#create new csv
	f.write('hashtag,number of tweets,number of unique tweets,number of unique authors\n')
	count = collections.Counter()
	unique_count = collections.Counter()
	authors = {}
	#stopwords = "a,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,yesterday,today".split(',')

	cur.execute("select text,author from tweets where text like '%#%'")
	print 'query done'
	for (i,row) in enumerate(cur.fetchall()):
		s = row[0].split()
		result = [x.lower().strip(twitter_punct) for x in s if (x.find('#') > -1 and len(x) > 2)]
		count.update(result)
		if not ('RT @' or 'via @' or '\"@') in s:
			unique_result = [z.lower().strip(twitter_punct) for z in s if (z.find('#') > -1 and len(z) > 2)]
			print unique_result
			print row
			unique_count.update(unique_result)
			for y in unique_result:
				if not y in authors:
					authors[y] = [row[1]]
				elif row[1] not in authors[y]:
					authors[y].append(row[1])
		print 'row: %d' % i

	for x in count.most_common(1000):
		result = '"%s","%s","%s","%s",\n' % (x[0],x[1],unique_count[x[0]],len(authors[x[0]])) #TODO: get count of item from unique counter
		f.write(result)	

#most common hashtags, but by unique author and without retweets
def unique_author_hashtag_frequency(cur):
	f = open('data/unique_author_hashtag_frequency.csv', 'w')	#create new csv
	f.write('hashtag,number of authors\n')
	authors = {}
	#count = collections.Counter()
	#stopwords = "a,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,yesterday,today".split(',')

	cur.execute("select text,author from tweets where text like '%#%'")
	print 'query done'
	for (i,row) in enumerate(cur.fetchall()):
		s = row[0].split()
		if not 'RT' in s:
			result = [x.lower() for x in s if x.find('#') > -1]
			for y in result:
				if not y in authors:
					authors[y] = [row[1]]
				elif row[1] not in authors[y]:
					authors[y].append(row[1])
				#if x in stopwords:
				#	s.remove(x)
			#count.update(result)
		print 'row: %d' % i

	for x in sorted(authors.items(), key=lambda x: len(x[1]), reverse=True):
		result = '"%s","%s"\n' % (x[0],len(x[1]))
		f.write(result)	

# get time series data for a particular hashtag
def hashtag_frequency_over_time(cur):
	tag = "#tgdn"

	title = "data/%s_over_time.csv" % tag
	f = open(title, 'w')	#create new csv
	f.write('time,%s\n' % tag) 
	print tag
	for i in range(15,23):		#15-23 (day)
		for j in range(0,24):	#0-24 (hour)
			dateStart = '2013-04-%02d %02d'  % (i,j)
			print "time: %s" % dateStart
			print "tag: " + tag
			cur.execute("select count(*) from tweets where (text like '%" + tag + "%' and time like '" + dateStart + "%')")
			for row in cur.fetchall():
				result = '"%s",%d\n' % (dateStart,row[0])
				f.write(result)

#calculate hashtag use over time
#TODO: output formatting could use work
def top_hashtag_frequency_over_time(cur, tag):
	f = open('data/hashtag_frequency_over_time.csv', 'w')	#create new csv
	f.write('hashtag,time,count\n')
	count = collections.Counter()
	#stopwords = "a,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,yesterday,today".split(',')

	cur.execute("select text from tweets where text like '%#%'")
	for (i,row) in enumerate(cur.fetchall()):
		s = row[0].split()
		result = [x.lower() for x in s if x.find('#') > -1]
			#if x in stopwords:
			#	s.remove(x)
		count.update(result)
		print 'row: %d' % i

	for x in count.most_common(10):
		print x
		for i in range(15,23):		#15-23 (day)
			for j in range(0,24):	#0-24 (hour)
				dateStart = '2013-04-%02d %02d'  % (i,j)
				print "time: %s" % dateStart
				print "tag: " + str(x[0])
				cur.execute("select count(*) from tweets where (text like '%" + str(x[0]) + "%' and time like '" + dateStart + "%')")
				for row in cur.fetchall():
					result = '"%s","%s",%d\n' % (x[0],dateStart,row[0])
					f.write(result)

#ignores mentions in retweets and multiple mentions by the same author
def unique_mention_frequency(cur):
	f = open('data/unique_mention_frequency.csv', 'w')	#create new csv
	f.write('hashtag,number of unique mentions\n')
	authors = {}
	#count = collections.Counter()
	#stopwords = "a,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,yesterday,today".split(',')

	cur.execute("select text,author from tweets where text like '%@%'")
	print 'query done'
	for (i,row) in enumerate(cur.fetchall()):
		s = row[0].split()
		if not 'RT' in s:
			result = [x.lower() for x in s if x.find('@') > -1]
			for y in result:
				if not y in authors:
					authors[y] = [row[1]]
				elif row[1] not in authors[y]:
					authors[y].append(row[1])
				#if x in stopwords:
				#	s.remove(x)
			#count.update(result)
		print 'row: %d' % i

	for x in sorted(authors.items(), key=lambda x: len(x[1]), reverse=True)[:1000]:
		result = '"%s","%s"\n' % (x[0],len(x[1]))
		f.write(result)	

def place(cur):
	f = open('data/place.csv', 'w')	#create new csv
	f.write('none,latlong,place,both\n')
	count = collections.Counter({'none':0,'latlong':0,'place':0,'both':0})

	cur.execute("select meta_gps_lat,meta_gps_long,place from tweets")
	for row in cur.fetchall():
		print row
		if row[0] and row[1] and row[2]:
			count['both'] += 1
		elif row[2]:
			count['place'] += 1
		elif row[0] and row[1]:
			count['latlong'] += 1
		else:
			count['none'] += 1

	f.write(('%d,%d,%d,%d') % (count['none'],count['latlong'],count['place'],count['both']))

#most common places tweets are comming from
def place_frequency(cur):
	f = open('data/place_frequency.csv', 'w')	#create new csv
	f.write('place,total tweets,tweets from unique users\n')
	count = collections.Counter()
	#stopwords = "a,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,yesterday,today".split(',')

	cur.execute('select place,meta_gps_lat,meta_gps_long from tweets where (place not like "" or meta_gps_lat not like "")')
	print 'query done'
	for (i,row) in enumerate(cur.fetchall()):
		if row[0]:
			count.update([row[0]])
		else:
			count.update(['%s,%s' % (row[1],row[2])])

		print 'row: %d' % i

	for x in count.most_common(1000):
		result = '%s,%s\n' % (x[0],x[1])
		f.write(result)

def unique_place_frequency(cur):
	f = open('data/unique_place_frequency.csv', 'w')	#create new csv
	f.write('place,state,tweets from unique users\n')
	authors = {}
	#stopwords = "a,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,yesterday,today".split(',')

	cur.execute('select place,meta_gps_lat,meta_gps_long,author from tweets where (place not like "" or meta_gps_lat not like "")')
	print 'query done'
	for (i,row) in enumerate(cur.fetchall()):
		if row[0]:
			if not row[0] in authors:
				authors[row[0]] = [row[3]]
			elif row[3] not in authors[row[0]]:
				authors[row[0]].append(row[3])
		else:
			latlong = ('%s,%s' % (row[1],row[2]))
			if not latlong in authors:
				authors[latlong] = [row[3]]
			elif row[3] not in authors[latlong]:
				authors[latlong].append(row[3])
							
		print 'row: %d' % i

	for x in sorted(authors.items(), key=lambda x: len(x[1]), reverse=True)[:1000]:
		result = '%s,%s\n' % (x[0],len(x[1]))
		f.write(result)

#creates a network of hashtags used together, listing nodes (hashtag,count) and edges (hashtag,to-hashtag,count)
def hashtag_network(cur):
	f = open('data/hashtag_network.csv', 'w')	#create new csv
	f.write('NODES\n')
	count = collections.Counter()
	#stopwords = "a,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,yesterday,today".split(',')

	cur.execute("select text from tweets where text like '%#%'")
	print 'query done'
	for (i,row) in enumerate(cur.fetchall()):
		s = row[0].split()
		result = [x.lower().strip(twitter_punct) for x in s if (x.find('#') > -1 and len(x) > 2)]
			#if x in stopwords:
			#	s.remove(x)
		count.update(result)
		#print 'row: %d' % i

	for x in count.most_common(1000):
		result = '"%s","%s"\n' % (x[0],x[1])
		f.write(result)

	f.write('EDGES\n')
	tags = {z[0]:0 for z in count.most_common(1000)}
	for x in count.most_common(1000):
		edge_counter = collections.Counter(tags)	#add count(100) with function to get only counts of existing hashtags
		cur.execute("select text from tweets where text like '%" + x[0] + "%'")
		for (i,row) in enumerate(cur.fetchall()):
			s = row[0].split()
			result = [y.lower().strip(twitter_punct) for y in s if (y.find('#') > -1 and len(y) > 2)]
			print result
			edge_counter.update(result)
			#print 'row: %d' % i

		for y in edge_counter.most_common(1000): 
			result = '"%s","%s","%s"\n' % (x[0],y[0],y[1])
			f.write(result)	 

def single_node_network(cur):
	node = "#tcot"
	title = "data/%s_node_network.csv" % node
	f = open(title, 'w')	#create new csv
	count = collections.Counter()
	#stopwords = "a,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,yesterday,today".split(',')

	cur.execute("select count(*) from tweets where text like '%" + node + "%'")
	f.write("NODE\n")
	f.write('"%s","%s"\n' % (node,cur.fetchall()[0][0]))
	
	f.write('EDGES\n')
	edge_counter = collections.Counter()	#add count(100) with function to get only counts of existing hashtags
	cur.execute("select text from tweets where text like '%" + node + "%'")
	for (i,row) in enumerate(cur.fetchall()):
		s = row[0].split()
		print s
		result = [y.lower().strip(twitter_punct) for y in s if (y.find('#') > -1 and len(y) > 2)]
		edge_counter.update(result)
		#print 'row: %d' % i

	for y in edge_counter.most_common(100): 
		result = '"%s","%s","%s"\n' % (node,y[0],y[1])
		f.write(result)	 


if __name__ == "__main__":
    single_node_network(cur)