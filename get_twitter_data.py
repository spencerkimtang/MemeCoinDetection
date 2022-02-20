import snscrape.modules.twitter as TwitterScraper
from helper import check_URLS
from datetime import date, timedelta
import os, tweepy

#------------------------------------#
query = '#bitcoin -filter:retweets'
num_tweets = 10
live = date.today()
until = date.today() - timedelta(days= 1)
since = '2022-01-08'
#------------------------------------#

def get_past_tweets():
    """
    Gathers all past tweets based on the given query
    This can also be used to gather live tweets
    If so, we need to check for duplicate records 
    """

    tweets = f"snscrape --jsonl --max-results {num_tweets} --since {since} twitter-search '{query} until:{until} lang:en' > past_tweets.txt"

    #tweets = "snscrape --format '{date!r} /%/ {content!r}'" + f" --since {since} --max-results {num_tweets} twitter-search '{query} until:{until} lang:en' > past_tweets.txt"
    
    os.system(tweets)
    if os.stat('past_tweets.txt').st_size == 0:
        print('No Tweets Found!')
    else:
        print('Producing past tweets to kafka...')

# I WANT TO RE-DO THIS FOR SNSCRAPE IF POSSIBLE
def get_live_tweets():

    tweets = f"snscrape --jsonl --max-results {1} --since {until} twitter-search '{query} until:{live} lang:en' > live_tweets.txt"
    
    os.system(tweets)
    if os.stat('live_tweets.txt').st_size == 0:
        print('No Tweets Found!')
    else:
        print('Producing live tweets to kafka...')
        file = open('live_tweets.txt', 'r')
        for live_tweet in file:
            return live_tweet

if __name__ == '__main__':
    get_live_tweets()