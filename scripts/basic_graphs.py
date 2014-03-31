from connection import dbConnection
from collections import Counter
from datetime import datetime
import utils

def rumor_over_time(db_name,rumor,gran):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "%s_over_time.csv" % rumor.replace('/','_')
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    if gran:
        f.write('time,misinfo,correction,speculation,hedge,question,unrelated/neutral/other\n')
    else:
        f.write('time,misinfo,correction,unrelated/neutral/other\n')

    for i in range(15,23):      #15-23 (day)
        for j in range(0,24):   #0-24 (hour)
            for k in range(0,60,10):
                count = Counter()
                dateStart = datetime(2013,04,i,j,k)
                dateEnd = datetime(2013,04,i,j,(k+9),59)
                #print "time: %s,%s" % (dateStart,dateEnd)

                raw_data = db.m_connections[db_name].find({
                    "created_ts":{
                        "$gte":dateStart,
                        "$lte":dateEnd
                    },
                    "codes.rumor":rumor
                },{
                    "codes.code":1
                })
                result = ''
                for x in raw_data:
                    count.update([x['codes'][0]['code']])

                    if gran:
                        misinfo = count['misinfo']
                        speculation = count['speculation']
                        hedge = count['hedge']
                        correction = count['correction']
                        question = count['question']
                        other = count['unrelated'] + count['other/unclear/neutral'] + count['unclear'] + count[''] + count['discussion - justifying'] + count['discussion - question'] + count['other'] + count['discussion']
                        result = '"%s",%d,%d,%d,%d,%d,%d\n' % (dateStart,
                                                               misinfo,
                                                               correction,
                                                               speculation,
                                                               hedge,
                                                               question,
                                                               other)
                    else:
                        misinfo = count['misinfo'] + count['speculation'] + count['hedge']
                        correction = count['correction'] + count['question']
                        other = count['unrelated'] + count['other/unclear/neutral'] + count['unclear'] + count[''] + count['discussion - justifying'] + count['discussion - question'] + count['other'] + count['discussion']
                        result = '"%s",%d,%d,%d\n' % (dateStart,
                                                      misinfo,
                                                      correction,
                                                      other)

                f.write(result)

def top_hashtags(db_name):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "iconf_top_hashtags.csv"
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('name,count\n')

    data = db.m_connections[db_name].find({
        'counts.hashtags':{
            '$gt':0
        }
    })

    count = Counter()

    for x in data:
        count.update(x['hashtags'])

    for x in count.most_common(100):
        print x

def top_urls(db_name):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "iconf_top_hashtags.csv"
    fpath = utils.write_to_data(path=title)
    f = open(fpath, 'w')
    f.write('name,count\n')

    data = db.m_connections[db_name].find({
        'counts.urls':{
            '$gt':0
        }
    })

    count = Counter()

    for x in data:
        for y in x['entities']['urls']:
            count.update([y['expanded_url']])

    for x in count.most_common(100):
        print x

def main():
    rumors = ['girl running','sunil','seals/craft','cell phone','proposal','jfk']
    top_urls(db_name='iconference')

if __name__ == "__main__":
    main()
