import tweepy
import socket
import re
import time
import warnings

warnings.filterwarnings("ignore")


#Twitter API keys
ACCESS_TOKEN = "921993390220292096-5XYTTxYKzGDfBankGQEPQzPEbt2tdih"
ACCESS_SECRET = "JeP77Uml6eF02nxuIOXEAmqlL8Bw9ndK27T7s2vrTcBXi"
CONSUMER_KEY = "MZDaWI6izE4c0LpvUo11Fwmht"
CONSUMER_SECRET = "HBFcTpSvtLKYNj6YpgNPBTub2r3oOGzkMSvejPOEu8fJiwitDm"

#API authorization
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

#Input Hashtag to find tweets
hashtag = '#covid19'

TCP_IP = 'localhost'
TCP_PORT = 9001

#Removal of non ASCII characters to filter emojis, symbols from raw tweet and location text
def preprocessing(tweet):
    tweet = ''.join([chars for chars in tweet if ord(chars) < 128])
    return tweet

#getting tweet raw location and text
def getTweet(status):
    tweet = ""
    location = ""
    location = status.user.location
    if hasattr(status, "retweeted_status"):  # Check if Retweet
        try:
            tweet = status.retweeted_status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet["full_text"]
        except AttributeError:
            tweet = status.text
    time.sleep(1)
    if location != None:
        location = preprocessing(location)
    return location, preprocessing(tweet)


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()

class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        location, tweet = getTweet(status)
        if (location != None and tweet != None):
            tweetLocation = location + "::" + tweet+"\n"
            print(status.text)
            conn.send(tweetLocation.encode('utf-8'))
        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False
        else:
            print(status_code)

#Streaming Tweets
myStream = tweepy.Stream(auth = auth, listener = MyStreamListener())
myStream.filter(track = [hashtag], languages = ["en"])


