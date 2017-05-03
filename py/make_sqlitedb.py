"""
Script

Script to collect the data store in S3 in a txt files in different folder ( one folder per day one file per hour)
"""

import os
import json
from time import time
from pprint import pprint
import re
import pandas as pd
from boto3 import client
import boto3
from sqlalchemy import create_engine
import datetime

#Functions part
def find_word_in_text(list_word,text):
    text = text.lower()
    for word in list_word:
        word = word.lower()
#         match = re.search(word, text)
        if text.find(word)>=0:
            return True
    return False



tic=time()

#Connection to the backend (S3 in this case)
s3 = boto3.resource('s3')
bucket = s3.Bucket('jeanmidev.twitterpres2017')
s3_client = boto3.client('s3')

#The tags of the different candidates
candidates_tags={
    "Hamon":["@benoithamon","@AvecHamon2017","@partisocialiste"],
    "Le Pen":["@MLP_officiel","@FN_officiel"],
    "Macron":["@EmmanuelMacron","@enmarchefr"],
    "Mélenchon":["@JLMelenchon","@jlm_2017"],
    "Fillon":["@FrancoisFillon","@Fillon2017_fr","@lesRepublicains"],
    "Dupont-Aignan":["@dupontaignan","@DLF_Officiel"],
    "Cheminade":["@JCheminade"],
    "Arthaud":["@n_arthaud","@LutteOuvriere"],
    "Asselineau":["@UPR_Asselineau","@UPR_Officiel"],
    "Poutou":["@PhilippePoutou","@NPA_officiel"],
    "Lassalle":["@jeanlassalle"]
}

#Setup the keywords that has been used for the filter
candidates_keywords={}
for candidate in candidates_tags:
    list_twitter_accounts=candidates_tags[candidate]
    candidates_keywords[candidate]=list_twitter_accounts+[candidate]
pprint(candidates_keywords)

current_date = datetime.datetime.now()

startdate=datetime.datetime.strptime("2017-03-18","%Y-%m-%d")#To limit the number for the extract you cn modify the start date
enddate = datetime.datetime.now()

nbr_days=(enddate-startdate).days
print(startdate,enddate,nbr_days)

for day in range(nbr_days+1):
    #for each day try to find the S3 folder
    date=startdate+datetime.timedelta(days=day)
    print(date.strftime("%Y-%m-%d"))
    bucket = s3.Bucket('jeanmidev.twitterpres2017')
    s3_client = boto3.client('s3')
    day_objects= bucket.objects.filter(Prefix="{}/".format(date.strftime("%Y%m%d")))



    #For each file in the folder
    for obj in day_objects:
        #Make a local copy in tamp.txt
        key = obj.key
        print(key)
        keysplit=key.split("_")
        print(keysplit)
        tweets_data = []
        s3_client.download_file('jeanmidev.twitterpres2017', key, 'tamp.txt')
        tweets_file=open('tamp.txt')

        #Read each lines of the files and the store the line in the list tweets_data
        for line in tweets_file:
            try:
                tweet = json.loads(line)
                # pprint(tweet)
                tweets_data.append(tweet)
            except:
                continue
        tweets_file.close()

        #Make a dict varaible that will contains all the data for each line of the file
        dict_df = {
            "user":[],
            "location":[],
            "text": [],
            "lang": [],
            "timestampms":[],
            "quoted/retweeted?":[],
            "quoted/retweeted user":[],
            "quoted/retweeted full_text":[]
        }
        for candidate in candidates_tags:
            dict_df["mention_" + candidate] = []

        #for each line analyse the content of the response of the twitter api
        for tweet in tweets_data:
            #if there is the miminum elements in the line text lang and timestamp_ms
            if "text" in tweet and "lang" in tweet and "timestamp_ms" in tweet:
                #         Classic data
                dict_df["user"].append(tweet["user"]["name"])
                dict_df["location"].append(tweet["user"]["location"])
                dict_df["text"].append(tweet["text"])
                dict_df["lang"].append(tweet["lang"])
                dict_df["timestampms"].append(tweet["timestamp_ms"])
                # dict_df["raw_APIresult"].append(tweet)

                text_tomention = tweet["text"] + " "

                #Check the informations in the case of a retweet
                if "quoted_status" in tweet:
                    dict_df["quoted/retweeted?"].append(True)
                    if 'extended_tweet' in tweet["quoted_status"]:
                        dict_df["quoted/retweeted user"].append(tweet["quoted_status"]['user']['name'])
                        dict_df["quoted/retweeted full_text"].append(
                            tweet["quoted_status"]['extended_tweet']['full_text'])
                        text_tomention = text_tomention + tweet["quoted_status"]['extended_tweet']['full_text']
                    else:
                        dict_df["quoted/retweeted user"].append(None)
                        dict_df["quoted/retweeted full_text"].append(None)

                elif 'retweeted_status' in tweet:
                    dict_df["quoted/retweeted?"].append(True)
                    if 'extended_tweet' in tweet['retweeted_status']:
                        dict_df["quoted/retweeted user"].append(tweet['retweeted_status']['user']['name'])
                        dict_df["quoted/retweeted full_text"].append(
                            tweet['retweeted_status']['extended_tweet']['full_text'])
                        text_tomention = text_tomention + tweet['retweeted_status']['extended_tweet']['full_text']
                    else:
                        dict_df["quoted/retweeted user"].append(None)
                        dict_df["quoted/retweeted full_text"].append(None)

                else:
                    dict_df["quoted/retweeted?"].append(False)
                    dict_df["quoted/retweeted user"].append(None)
                    dict_df["quoted/retweeted full_text"].append(None)

                #Try to find the mention in the text of the differents candidates or their accounts
                for candidate in candidates_keywords:
                    dict_df["mention_" + candidate].append(find_word_in_text(candidates_keywords[candidate], text_tomention))

        #Store the data in a dataframe
        df_tweets = pd.DataFrame(dict_df)
        #Add a columns in the dataframe about the day and hour of the tweets
        date_file=date.strftime("%Y-%m-%d")
        df_tweets["day"] = [date_file] * len(df_tweets)
        df_tweets["hour"] = [keysplit[2].split(".")[0]] * len(df_tweets)
        print(df_tweets.head())
        print(date.year, date.isocalendar()[1], date.weekday()/2)

        #Store the data in a sqlite file (a file per part of the week and week) limitation by kaggle
        backup_engine = create_engine('sqlite:///database_{}_{}.sqlite'.format(date.isocalendar()[1],int(date.weekday()/2)))
        df_tweets.to_sql('data', backup_engine, if_exists='append')


print(startdate,enddate)
toc=time()-tic
print("ES(s):{}".format(toc))
print("END")