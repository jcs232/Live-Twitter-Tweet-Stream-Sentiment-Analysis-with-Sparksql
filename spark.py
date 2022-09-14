from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
import time
import geopy
from geopy.geocoders import Nominatim
from textblob import TextBlob
import json
import unidecode
import warnings

warnings.filterwarnings("ignore")

TCP_IP = 'localhost'
TCP_PORT = 9001

#Processing tweets, extracting location info and sentiment
def processTweet(tweet):
    tweetData = tweet.split("::")
    if len(tweetData) > 1:
        text = tweetData[1]
        rawLocation = tweetData[0]
  	#sentiment classifcation
        if float(TextBlob(text).sentiment.polarity) > 0.06:
            sentiment = "Positive"
        elif float(TextBlob(text).sentiment.polarity) < 0.03:
            sentiment = "Negative"
        else:
          sentiment = "Neutral"

	#using geopy to get location data from raw location
        try:
            geolocator = Nominatim(user_agent="4371proj")
            print("\n\n=========================\ntweet: ", text)
            print("Raw location from tweet status: ", rawLocation)
            loc = geolocator.geocode(rawLocation, language = "english", addressdetails = True)
            time.sleep(1)
            longi = loc.longitude
            lati = loc.latitude
            state = loc.raw['address']['state']
            country = loc.raw['address']['country']
            if not all(char.isalpha() or char.isspace() for char in country):
                country = None
            print("sentiment: ", sentiment)
            print("lat: ", loc.latitude)
            print("lon: ", loc.longitude)
            state = unidecode.unidecode(state)
            print("state: ", state)
            country = unidecode.unidecode(country)
            print("country: ", country)
            
        except:
            lati = None
            longi = None
            state = None
            country = None

	#Storing tweets with valid address and sentiment information and their data into a json file 
        if lati != None and sentiment != None and country != None:
            global data
            temp = {}
            temp = {"text": text, "lat": lati, "long": longi, "state": state, "country": country, "sentiment": sentiment}
            data.update(temp)
            with open('tweetdata.json', 'a') as f:
                json.dump(data, f)
                f.write("\n")
                f.close()
        

#Create pyspark configuration
conf = SparkConf()
conf.setAppName('TwitterApp')
conf.setMaster('local[2]')

#Spark Context from pyspark configuration
sc = SparkContext(conf=conf)

#create the Streaming Context from spark context with interval size 4 seconds
ssc = StreamingContext(sc, 4)
ssc.checkpoint("checkpoint_TwitterApp")

#dictionary to store tweet data
data = {}

#read data from port 900
dataStream = ssc.socketTextStream(TCP_IP, TCP_PORT)

dataStream.foreachRDD(lambda rdd: rdd.foreach(processTweet))

ssc.start()
ssc.awaitTermination()
