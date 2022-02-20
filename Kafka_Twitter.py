"""
This is the initial script to our Apache kafka data pipeline. Here we gather twitter data, which is then formatted to include the desired information.
The data is then fed into an Apache Kafka server via the producer structure.
The start of the pipeline.
"""

import tweepy, time, re, json, os
import pandas as pd
from datetime import datetime, date
from kafka import KafkaConsumer, KafkaProducer
import snscrape.modules.twitter as TwitterScraper

def setup_API():
    """Get twitter API Credentials"""

    consumerKey = ''
    consumerSecret = ''
    accessToken = ''
    accessSecret = ''

    auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessSecret)

    return tweepy.API(auth, wait_on_rate_limit=True)


def get_past_tweets(api, query, producer, topic, num_tweets):
    """Gathers all past tweets based on the given query"""

    until = date.today()
    since = '2022-01-08'

    #check_URLS(since, until, num_tweets, query)

    tweets = "snscrape --format '{date!r} /%/ {content!r}'" + f" --since {since} --max-results {num_tweets} twitter-search '{query} until:{until} lang:en' > tweets.txt"
    os.system(tweets)
    if os.stat('tweets.txt').st_size == 0:
        print('No Tweets Found')
    else:
        # try to read file line by line
        f = open('tweets.txt', 'r')
        for i in f.readlines():
            # Clean the data
            date_txt, tweet_txt  = i.split(' /%/ ')
            formated_date = date(int(date_txt[18:22]), int(date_txt[23:25]), int(date_txt[26:29]))
            tweet_txt = tweet_txt.strip("''\n" )
            tweet_txt = tweet_txt.replace('\\n\\n', ' ')
            cleaned_tweet = clean(tweet_txt)
            
            # Organize the data to be sent to Kafka
            record = '' + str(formated_date) + ' : ' + cleaned_tweet
            producer.send(topic, str.encode(record))

def get_live_tweets(api, query, producer, topic):
    """Gathers all live tweets based on the given query"""

    tweets = api.search_tweets(q= query, lang= 'en', tweet_mode= 'extended')
    for tweet in tweets:
        record = '' + str(tweet.created_at) + ': ' + str(clean(tweet.full_text))
        producer.send(topic, str.encode(record))

def main():
    api = setup_API()
    producer = KafkaProducer(bootstrap_servers= 'localhost:9092')   #Producer API allows applications to send streams of data to topics in the Kafka cluster
    topic = 'Crypto-Tweets'
    query = '#bitcoin -filter:retweets'
    
    num_tweets = 10
    get_past_tweets(api, query, producer, topic, num_tweets)

    # while True:
    #     get_live_tweets(api, query, producer, topic)
    #     time.sleep(60 * 0.1)

if __name__== "__main__":
    main()
