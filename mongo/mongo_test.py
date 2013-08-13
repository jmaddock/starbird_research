from pymongo import MongoClient

client = MongoClient()
db = client.boston
collection = db.tweets
tweets = db.tweets

f = open('data/top_urls.csv', 'w')	#create new csv
f.write('url,count\n')
count = collections.Counter()
#stopwords = "a,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your,yesterday,today".split(',')
#punctuation = ["'",":", ",",".","!","?",'"',';'] 

data = tweets.find({"counts.urls":{$gt:0}},{"entities.urls":1}) #this is more or less the correct query
print 'query done'
for (i,data) in enumerate(cur.fetchall()): # TODO: this must be updated for json formatting
	s = row[0].split()
	result = [x.lower() for x in s if not x in stopwords] #stopwords probably don't matter here
		#if x in stopwords:
		#	s.remove(x)
	count.update(result)
	print 'row: %d' % i

for x in count.most_common(1000):
	result = '%s,%s\n' % (x[0],x[1])
	f.write(result)