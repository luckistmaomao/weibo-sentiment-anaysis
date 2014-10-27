#coding:utf-8

'''
@author: yuzt
'''

from pymongo import MongoClient
from collections import defaultdict
import datetime
import jieba
import time
import os

curpath=os.path.normpath( os.path.join( os.getcwd(), os.path.dirname(__file__) ) ) 
DATA_SOURCE = ['np','nw','weibo']
client = MongoClient('121.40.193.156')
user_sentiment_collection = 'user_sentiment'
sentiment_tags = ['negative','neutral','positive']

class Timer(object):                                                                                                                                   
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start

def load_dict():
    neg_dict = set()
    pos_dict = set()
    with open(curpath+'/neg.dict') as f:
        for line in f:
            neg_word = line.strip().decode('utf-8')
            neg_dict.add(neg_word)
    with open(curpath+'/pos.dict') as f:
        for line in f:
            pos_word = line.strip().decode('utf-8')
            pos_dict.add(pos_word)
    return neg_dict,pos_dict

def get_user_list_from_dB():
    newdb = client['all_users_db']
    dataSet = newdb['all_users_info']
    user_list = [user['u_name'] for user in dataSet.find() if user['u_is_confirmed']] 
    return user_list

neg_dict,pos_dict = load_dict()

def get_sentiment_tag(content):
    neg_degree,pos_degree = 0,0
    words = jieba.cut(content)
    for word in words:
        if word in neg_dict:
            neg_degree += 1
        elif word in pos_dict:
            pos_degree += 1
    if neg_degree > pos_degree:
        return 'negative'
    elif neg_degree == pos_degree:
        return 'neutral'
    else:
        return 'positive'

def init_sentiment_distribution(user_dbname):
    userdb = client[user_dbname] 
    time_dict = dict()
    for source in DATA_SOURCE:
	print source
        collection_name = 'user_%s' % (source,)
        source_collection = userdb[collection_name]
        if source in ['bbs','weibo']:
            time_name = 'time'
        else:
            time_name = 'publish_time'
        for item in source_collection.find():
            content = item['content']
            time = item[time_name][:10]
            if time not in time_dict:
                time_dict[time] = defaultdict(dict)
                for content_source in DATA_SOURCE:
                    for tag in sentiment_tags:
                        time_dict[time][content_source][tag] = 0
            sentiment_tag = get_sentiment_tag(content)
            time_dict[time][source][sentiment_tag] += 1

    today = datetime.date.today()
    for time,sentiment_dict in time_dict.items():
        if time == str(today):
            continue
        sentiment_dict['time'] = time
        userdb['user_sentiment'].insert(sentiment_dict)
         
def update_sentiment_distribution(user_dbname,target_time):
    userdb = client[user_dbname]
    sentiment_dict = defaultdict(dict)
    for source in DATA_SOURCE:
        for tag in sentiment_tags:
            sentiment_dict[source][tag] = 0
    for source in DATA_SOURCE:
        collection_name = 'user_%s' % (source,)
        source_collection = userdb[collection_name]
        if source in ['bbs','weibo']:
            time_name = 'time'
            documents = source_collection.find({'time' : {'$regex' : '^'+str(target_time)}})
        else:
            time_name = 'publish_time'
            documents = source_collection.find({'publish_time':str(target_time)})
        for document in documents:
            content = document['content']
            sentiment_tag = get_sentiment_tag(content)
            sentiment_dict[source][sentiment_tag] += 1
    sentiment_dict['time'] = str(target_time)
    if userdb['user_sentiment'].find({'time':target_time}).count() == 0:
        userdb['user_sentiment'].insert(sentiment_dict)
    #    print user_dbname,target_time,sentiment_dict

def calculate_sentiment_distribution():
    user_list = get_user_list_from_dB()
    print user_list
    for user_name in user_list:
        user_dbname = 'user_%s_database' % (user_name,)
        user_collections = client[user_dbname].collection_names()
#        if user_sentiment_collection in user_collections:
#            target_time = datetime.date.today() - datetime.timedelta(days=1)
#            update_sentiment_distribution(user_dbname,str(target_time))
#        else:
#            init_sentiment_distribution(user_dbname)
        target_time = datetime.date.today() - datetime.timedelta(days=1)
        update_sentiment_distribution(user_dbname,str(target_time))
        
if __name__ == "__main__":
    print 'start',time.ctime()
    with Timer() as t:
        calculate_sentiment_distribution()
    #print t.interval
    print 'end',time.ctime()
