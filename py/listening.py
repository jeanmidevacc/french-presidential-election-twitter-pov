"""
listening.py

This script is filtering the twitter streaming by the nqmz of the candidate and their twitter accounts associated.

You need to create an application on twitter to be connect to the stream 5see the documentation (https://apps.twitter.com/)

This code is mostly based on the script explained in this article http://adilmoujahid.com/posts/2014/07/twitter-analytics/
"""
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from pprint import pprint

#configuration of the variable that contain all the information of your api
dict_config={"api_key":"",
             "api_secret":"",
             "access_token":"",
             "access_token_secret":""
             }


#Variables that contains the user credentials to access Twitter API
access_token = dict_config["access_token"]
access_token_secret =dict_config["access_token_secret"]
consumer_key = dict_config["api_key"]
consumer_secret =dict_config["api_secret"]


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)



#The candidates and the twitter account associated
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



if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #Create list tags
    list_tags=[]
    for candidate in candidates_tags:
        list_tags.append(candidate)
        for tag in candidates_tags[candidate]:
            list_tags.append(tag)



    #print(list_tags)
    #This line filter Twitter Streams to capture data by the keywords
    stream.filter(track=list_tags)