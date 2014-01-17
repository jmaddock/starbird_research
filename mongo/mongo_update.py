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
                         db="jfk_and_proposal")
sql_cursor = sql_db.cursor()

def code_import():
        written_ids = open('written_ids_proposal.txt','w')
        #sql db query
        sql_cursor.execute("select id,code from tweets_proposal")
        for x in sql_cursor.fetchall():
                query = str(x[0])
                value = str(x[1])
                print query,value
                written_ids.write('"%s","%s"\n' % (query,value))
                tweets.update({'user.id':query},
                              {'$push':{'codes':{'rumor':'proposal',
                                                 'code':value}}})

def create_rumor_collection():

        # enter new collection info
        # assumes collection has already been created
        collection2 = mongo_db.proposal
        proposal = mongo_db.proposal

        #logging
        written_ids = open('log_proposal.txt','w')

        #query sql, pull document from tweets collection, and add document to
        #new collection
        sql_cursor.execute("select id from tweets_proposal")
        for i,x in enumerate(sql_cursor):
                print i
                written_ids.write('"%s"\n' % (x[0]))
                document = tweets.find_one({'user.id':str(x[0])})
                proposal.insert(document)

if __name__ == "__main__":
    create_rumor_collection()
