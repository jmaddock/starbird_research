from connection import dbConnection
from collections import Counter
from datetime import datetime

def rumor_over_time(db_name,rumor):
    db = dbConnection()
    db.create_mongo_connections(mongo_options=[db_name])

    title = "data/%s_over_time.csv" % rumor
    f = open(title, 'w')

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
                for x in raw_data:
                    count.update([x['codes'][0]['code']])
                misinfo = count['misinfo'] + count['speculation'] + count['hedge']
                correction = count['correction'] + count['question']
                other = count['unrelated'] + count['other/unclear/neutral'] + count['unclear'] + count[''] + count['discussion - justifying'] + count['discussion - question'] + count['other'] + count['discussion']
                result = '"%s",%d,%d,%d\n' % (dateStart,misinfo,correction,other)
                f.write(result)

def main():
    rumors = ['girl running','sunil','seals/craft','cell phone','proposal','jfk']
    for x in rumors:
        rumor_over_time(db_name='new_boston',rumor=x)

if __name__ == "__main__":
    main()
