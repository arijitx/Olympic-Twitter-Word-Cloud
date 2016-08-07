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
word_freq_next=defaultdict(int)
count_vect = CountVectorizer(analyzer='word',stop_words='english')

CONSUMER_KEY = 'jrkcjINAfIEqoGpQ7tb13DIhk'
CONSUMER_SECRET = 'AXval8wJ4f0GKlYAUGsx9TgGOOZNUf24Z37ROpmdZ833h584gi'#keep the quotes, replace this with your consumer secret key
ACCESS_KEY = '558579036-xm8Nav9bQ82difJgpDkbMIWzeKrgCASz9P5fkzjV'#keep the quotes, replace this with your access token
ACCESS_SECRET = '2uKoxZGiGqIynj7B4tUsN0YIrSLvyCbLo1sijqUsmEM7j'#keep the quotes, replace this with your access token secret

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)
api = tweepy.API(auth)
special_stopwords=['http.* ','http.*$','olympic','rt','rio','games','2016']
counter=0
set_t=False
t=time.time()
interval=900
write_thresh=50

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
    global word_freq_next
    global counter
    global t
    global set_t

    counter+=1

    
    word_freq_map=dict(Counter(freq)+Counter(word_freq_map))
    if not set_t and ((time.time()-t))>interval:
        set_t=True
        t=time.time()



    if set_t:
        word_freq_next=dict(Counter(freq)+Counter(word_freq_next))

    if set_t and ((time.time()-t))>interval :
        with open("word_freq_at_"+str(int(time.time()))+".json", "w") as outfile:
            json.dump(word_freq_map, outfile,cls=MyEncoder, indent=4)
        print 'Swaping Update . . . '
        word_freq_map=word_freq_next
        word_freq_next=defaultdict(int)
        set_t=False
        t=time.time()

    
    if counter==write_thresh:
        print 'JSON Update . . '
        counter=0
        with open("word_freq.json", "w") as outfile:
            json.dump(word_freq_map, outfile,cls=MyEncoder, indent=4)

def cloud_map(text):
    global table
    try:
        text=text.translate(table, string.punctuation)
        for w in special_stopwords:
            text=re.sub(w,"",text.lower())
            #print w+'_moved_    ------ >>>   '+text
        #print "After : "+text
        X = count_vect.fit_transform(text.split('\n'))
        vocab = list(count_vect.get_feature_names())
        counts = X.sum(axis=0).A1
        freq_distribution = Counter(dict(zip(vocab, counts)))
        update_word_freq(freq_distribution)
    except:
        print sys.exc_info()[0]
        raise

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        #print status.author.name
        status._json['text']=status._json['text'].encode('ascii','ignore')
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
# startStreaming(sys.argv[1])
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

