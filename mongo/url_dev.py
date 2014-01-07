from pymongo import MongoClient
from datetime import datetime
import collections,re

client = MongoClient()
db = client.boston
collection = db.tweets
tweets = db.tweets

def counts_entities_match():
    count = 0
    for x in range(7):
        query = 'entities.urls.%i' % (x)
        data = tweets.find({'counts.urls':(x+1),query:{'$exists':'true'}}).count()
        count += data
    print count

def url_regex_count():
    count = 0
    uncaught = 0
    good = 0
    f = open('data/url_matches.csv','w')
    for x in range(7):
        reg_tag = re.compile('http', re.IGNORECASE)
        data = tweets.find({'counts.urls':(x),'text':reg_tag})
        for y in data:
            urls = y['text'].count('http')
            if urls > y['counts']['urls']:
                count += 1
                try:
                    f.write('"%s","%s"\n' % (y['user']['id'],y['text']))
                except:
                    f.write('"%s","%s"\n' % (y['user']['id'],'decode error'))
                print y['text'],y['counts']['urls'],urls
            elif urls < y['counts']['urls']:
                uncaught += 1
            else:
                good += 1
    print 'uncounted urls: %i' % count
    print 'uncaught urls: %i' % uncaught
    print 'matched urls: %i' % good

def unique_urls():
    url_count = collections.Counter()
    title_count = collections.Counter()
    domain = tweets.distinct('entities.urls.domain')
    data = tweets.find({'counts.urls':{'$gt':0}})
    #title = tweets.distinct('entities.urls.title')
    print 'domain: ' + str(len(domain))
    for x in data:
        for y in x['entities']['urls']:
            try:
                url_count.update([y['long-url']])
            except:
                print 'no url'
            try:
                title_count.update([y['title']])
            except:
                print 'no title'
    print len(title_count)
    print len(domain)
    #print 'url: ' + str(len(url))
    #print 'title: ' + str(len(title))

def url_repair():
    find_link = re.compile('http.*?\s', re.IGNORECASE)
    f = open('data/url_matches.csv','w')
    count = collections.Counter()
    for x in range(7):
        data = tweets.find({'counts.urls':(x)}).limit(1000)
        #data = tweets.find({'user.id':'4863301'})
        for y in data:
            urls = y['text'].count('http')
            if urls > y['counts']['urls']:
                links = find_link.findall(y['text'])
                links = [z.strip('\n').strip(' ').strip('...') for z in links]
                known_links = [z['expanded_url'] for z in y['entities']['urls']]
                result = [z for z in links if z not in known_links]
                if len(links) - len(known_links) == len(result):
                    print 'text: %s' % y['text']
                    new_data = tweets.find_one({'text':(re.compile(y['text'])),
                                                'counts.urls':len(links)})
                    try:
                        print 'matches: %s' % new_data['text']
                    except:
                        print 'matches: %s' % new_data
                    text = y['text']
                    links_strip = [z for z in links]
                    start_text = None
                    while new_data == None and start_text is not text:
                        start_text = text
                        text = text.rstrip('...').strip()
                        if len(links_strip) > 0:
                            text = text.rstrip(links_strip[-1]).strip().strip('...')
                            links_strip.pop()
                        text = re.sub('RT .*?:','',text).strip()
                        new_data = tweets.find_one({'text':(re.compile(text)),
                                                    'counts.urls':len(links)})
                        try:
                            print 'matches: %s, %s' % (new_data['text'], text)
                        except:
                            print 'matches: %s, %s' % (new_data, text)
                    if new_data == None:
                        count.update(['unmatched'])
                    else:
                        count.update(['matched'])
    print count

if __name__ == "__main__":
    url_repair()
