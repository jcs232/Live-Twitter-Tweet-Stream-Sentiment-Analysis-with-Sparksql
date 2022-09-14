from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark
import json

#formatting JSON file from spark.py to normal JSON format so spark can read it
dataList = []
with open('tweetdata.json', 'r') as f:
    for line in f:
       dataList.append(line[:-1])
spacer = ",\n"
formattedList = "[" + spacer.join(dataList) + "]"


#Initializing Spark session
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession.builder.appName('tweetDataSummary').getOrCreate()
rddTweets = sc.parallelize([formattedList])
df = spark.read.json(rddTweets)

#Summary of all Tweet data
print("Tweet dataframe schema:")
df.printSchema()
print("Tweet dataframe")
df.show()

#Summarized Results
df.createOrReplaceTempView("a")
#Sample of 1 tweet's data
print("Example tweet data")
print(df.collect()[1])

print("Number of tweets by country")
countryTweetCount = df.groupBy("country").count()
countryTweetCount.sort("count", ascending = False).show()

print("Number of negative tweets by country")
NegativeCounts = spark.sql("SELECT country, COUNT(sentiment) as negative_Count FROM a WHERE sentiment = 'Negative' GROUP BY country")
NegativeCounts.sort("negative_Count", ascending = False).show()

print("Number of positive tweets by country")
PositiveCounts = spark.sql("SELECT country, COUNT(sentiment) as positive_Count FROM a WHERE sentiment = 'Positive' GROUP BY country")
PositiveCounts.sort("positive_Count", ascending = False).show()

print("Number of neutral tweets by country")
NeutralCounts = spark.sql("SELECT country, COUNT(sentiment) as neutral_Count FROM a WHERE sentiment = 'Neutral' GROUP BY country")
NeutralCounts.sort("neutral_Count", ascending = False).show()

