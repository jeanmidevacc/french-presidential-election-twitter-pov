# french-presidential-election-twitter-pov
Python script to collect the data from the twitter stream API about the french presidential election

The collection of the data from twitter is done by listening.py script
To collect the data you need to create:
- a Twitter application to define a tweepy object
- install tweepy pip install tweepy

The python running during 20 minutes every hour basically between XX:20 and XX:40 , i stop the script with a bash script and send the file wrote in a S3 bucket that contains a a folder for every day since the 18/03/2017.

To create the sqlite files i used make_sqlitedb.py

This script is collecting the dtaa from my S3 bucket and update the sqlite database for each new file in the bucket.




