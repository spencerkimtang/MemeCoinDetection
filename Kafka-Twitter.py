import tweepy
import time
import re
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

def setup_API():
    # Get twitter API Credentials
    consumerKey = ''
    consumerSecret = ''
    accessToken = ''
    accessSecret = ''

    auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessSecret)

    return tweepy.API(auth, wait_on_rate_limit=True)

def normalize_timestamp(ts):
    # Normalize the timestamp for consistency
    newTS = ts.split(' ')[0] 
    myTS = datetime.strptime(newTS, '%Y-%m-%d')
    return (myTS.strftime('%Y-%m-%d'))

def clean(tweet):
    # Cleans the data
    # Currently this will remove crypto coin abreviations, which we might want to keep such as #BTC for bitcoin
    tweet = re.sub('#bitcoin', 'bitcoin', tweet)
    tweet = re.sub('#Bitcoin', 'Bitcoin', tweet)
    tweet = re.sub('#[A-Za-z0-9]+', '', tweet)      # Removes uncorrelated hashtags
    tweet = re.sub('\\n', '', tweet)                # Removes \n' 
    tweet = re.sub('#https?:\/\/\S+', '', tweet)    # Removes hyperlinks

    return tweet

def get_tweets(api, query, producer, topic):  # sourcery skip: extract-method
    # Gathers all tweets based on the given query
    tweets = api.search_tweets(q= query, lang= 'en', tweet_mode= 'extended')

    """I want to use the following as the metadata per tweet: full_text, lang, favorite_count, retweet_count
                                                               user.followers_count, created_at """
    for i in tweets:
        record = '' + str(i.created_at) + ': ' + str(clean(i.full_text))
        #record.append(str(i.lang))
        #record.append(str(i.user.followers_count))
        #record.append(str(i.favorite_count))
        #record.append(str(i.retweet_count))
        #print(record, '\n')
        # print(i.full_text)
        producer.send(topic, str.encode(record))

def main():
    api = setup_API()
    producer = KafkaProducer(bootstrap_servers= 'localhost:9092')   #Producer API allows applications to send streams of data to topics in the Kafka cluster
    topic = 'Crypto-Tweets'
    query = '#bitcoin -filter:retweets'     #prevents duplicate tweets from appearing
    
    while True:
        get_tweets(api, query, producer, topic)
        time.sleep(60 * 0.1)

if __name__== "__main__":
    main()
