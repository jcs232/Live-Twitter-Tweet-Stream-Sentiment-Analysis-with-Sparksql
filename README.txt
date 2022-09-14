CS 4371 Final Project
Team members: Jay Shah (jcs180000) and Aatifa Khan (axk180019)

The project contains 3 files, stream.py gets tweets using API keys from twitter based on an hashtag that can be set in the file, preprocesses them and streams them to spark.py

spark.py processes the tweets for sentiment analysis and location data, as well as doing further filtering and processing on the tweets. It then writes the tweet data to tweetdata.json (sample of this file is also attached)

Finally, sparksql.py reads the tweet data in tweetdata.json, processes it to analyze the data with spark sql and displays some data summaries of the tweet data.

The project files were ran by the following commands:

cd Documents
python3 stream.py

cd Documents
python3 spark.py
(ran simultaneously in another terminal window)

cd Documents
python3 sparksql.py
(ran afterwards, uses the generated json file by spark.py)