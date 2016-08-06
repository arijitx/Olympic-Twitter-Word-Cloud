import tweepy
import sys,re
import time
import json,numpy
from collections import Counter,defaultdict
from httplib import IncompleteRead
import string
from sklearn.feature_extraction.text import CountVectorizer

table = string.maketrans("","")
word_freq_map=defaultdict(int)
count_vect = CountVectorizer(analyzer='word',stop_words='english')

CONSUMER_KEY = 'jrkcjINAfIEqoGpQ7tb13DIhk'
CONSUMER_SECRET = 'AXval8wJ4f0GKlYAUGsx9TgGOOZNUf24Z37ROpmdZ833h584gi'#keep the quotes, replace this with your consumer secret key
ACCESS_KEY = '558579036-xm8Nav9bQ82difJgpDkbMIWzeKrgCASz9P5fkzjV'#keep the quotes, replace this with your access token
ACCESS_SECRET = '2uKoxZGiGqIynj7B4tUsN0YIrSLvyCbLo1sijqUsmEM7j'#keep the quotes, replace this with your access token secret

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
api = tweepy.API(auth)
special_stopwords=['http','olympic','rt','rio']

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)

def update_word_freq(freq):
    global word_freq_map
    word_freq_map=dict(Counter(freq)+Counter(word_freq_map))
    with open("word_freq.json", "w") as outfile:
        json.dump(word_freq_map, outfile,cls=MyEncoder, indent=4)

def cloud_map(text):
    global table
    try:
        text=text.translate(table, string.punctuation)
        for w in special_stopwords:
            text=re.sub(w+".*","",text.lower())
        X = count_vect.fit_transform(text.split('\n'))
        vocab = list(count_vect.get_feature_names())
        counts = X.sum(axis=0).A1
        freq_distribution = Counter(dict(zip(vocab, counts)))
        update_word_freq(freq_distribution)
    except:
        pass

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        #print status.author.name
        status._json['text']=status._json['text'].encode('ascii','ignore')
        #print status._json['text']
        cloud_map(status._json['text'])
            # db.tweets.insert_one(status._json)
        # tw["prof_img"]=tweet.author.profile_image_url

        # with open("tweet_data.json", "w") as outfile:
        #     json.dump(tweets, outfile, indent=4)


    def on_error(self, status_code):
        if status_code == 420:
            print 'Streaming Failed . . .'
            #returning False in on_data disconnects the stream
            return False


def startStreaming(q):
    t=time.time()
    print 'Starting Stream Listener. . . '
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
    myStream.filter(track=[q])

print 'Streaming . . .'
while True:
    try:
        print 'start in try '
        startStreaming(sys.argv[1])
    except IncompleteRead:
        print 'Err IncompleteRead'
        continue
    except KeyboardInterrupt:
        # Or however you want to exit this loop
        break
    except:
        continue

