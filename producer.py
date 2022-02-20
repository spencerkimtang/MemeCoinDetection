""" 
This script is used to setup/configure the Kafaka producer that will feed data into the Kafka topic.
Sourced from: https://www.youtube.com/watch?v=Q4XA5nUpLeo 
"""

from kafka import KafkaProducer
import time
from get_twitter_data import *
from helper import json_serializer

# --------------------Config------------------------------------#
producer = KafkaProducer(bootstrap_servers= ['localhost:9092'],
                         key_serializer= json_serializer,
                         value_serializer= json_serializer)
topic = 'Crypto-Tweets'
# --------------------------------------------------------------#

def main():
    get_past_tweets()

    file = open('past_tweets.txt', 'r')
    for past_tweet in file:
        print(past_tweet)
        producer.send(topic, past_tweet)

    while 1 == 1:
        tweet = get_live_tweets()
        print(tweet)
        producer.send(topic, tweet)
        time.sleep(4)


if __name__ == '__main__':
    main()   