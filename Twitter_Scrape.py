#from os import access
import re
#from matplotlib.collections import _CollectionWithSizes
import tweepy
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#from bs4 import BeautifulSoup
from textblob import TextBlob

plt.style.use('seaborn-darkgrid')

def setup_API():
    # Get twitter API Credentials
    consumerKey = ''
    consumerSecret = ''
    accessToken = ''
    accessSecret = ''

    auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessSecret)

    return tweepy.API(auth, wait_on_rate_limit=True)

def get_tweets(api, query, num_tweets):
    # Gathers all tweets based on the given query
    tweets = tweepy.Cursor(api.search_tweets, q= query, lang= 'en', tweet_mode= 'extended').items(num_tweets)
    tweet_text = [tweet._json["full_text"] for tweet in tweets]

    return pd.DataFrame(tweet_text, columns= ['Tweets'])

def clean(tweet):
    # Cleans the data
    tweet = re.sub('#bitcoin', 'bitcoin', tweet)
    tweet = re.sub('#Bitcoin', 'Bitcoin', tweet)
    tweet = re.sub('#[A-Za-z0-9]+', '', tweet)      # Removes uncorrelated hashtags
    tweet = re.sub('\\n', '', tweet)                # Removes \n' 
    tweet = re.sub('#https?:\/\/\S+', '', tweet)    # Removes hyperlinks

    return tweet

def get_subjectivity(tweet):
    return TextBlob(tweet).sentiment.subjectivity

def get_polarity(tweet):
    return TextBlob(tweet).sentiment.polarity

def main():
    api = setup_API()

    # Gather Tweets about Bitcoin
    query = '#bitcoin -filter:retweets since:2021-06-01'     #prevents duplicate tweets from appearing
    num_tweets = 10
    tweets_Df = get_tweets(api, query, num_tweets)

    # Clean the dataframe
    tweets_Df['Cleaned_Tweets'] = tweets_Df['Tweets'].apply(clean)

    # Calculate Subjectivity and Polarity
    tweets_Df['Subjectivity'] = tweets_Df['Cleaned_Tweets'].apply(get_subjectivity)
    tweets_Df['Polarity'] = tweets_Df['Cleaned_Tweets'].apply(get_polarity)

    print(tweets_Df.head(5))

if __name__== "__main__":
    main()
