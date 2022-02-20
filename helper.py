import re, os, json
import pandas as pd

def json_serializer(data):
    return json.dumps(data).encode('utf-8')
    
def normalize_timestamp(ts):
    """Normalize the timestamp for consistency"""

    newTS = ts.split(' ')[0] 
    myTS = datetime.strptime(newTS, '%Y-%m-%d')
    return (myTS.strftime('%Y-%m-%d'))

def clean(tweet):
    """Cleans the data"""
    
    # Currently this will remove crypto coin abreviations, which we might want to keep such as #BTC for bitcoin
    tweet = re.sub('#bitcoin', 'bitcoin', tweet)
    tweet = re.sub('#Bitcoin', 'Bitcoin', tweet)
    tweet = re.sub('#[A-Za-z0-9]+', '', tweet)      # Removes uncorrelated hashtags
    tweet = re.sub('\\n', '', tweet)                # Removes \n' 
    tweet = re.sub('#https?:\/\/\S+', '', tweet)    # Removes hyperlinks (THIS NEEDS TO BE FIXED, CURRENTLY NOT WORKING)

    return tweet

def check_URLS(since, until, num_tweets, query):
    """A function to simply validate the tweets by URL"""

    os.system(f"snscrape --since {since} --max-results {num_tweets} twitter-search '{query} until:{until}' > result-tweets.txt")
    if os.stat('result-tweets.txt').st_size == 0:
        counter = 0
    else:
        df = pd.read_csv('result-tweets.txt', names=['link'])
        counter = df.size

    print('Number of tweets: ' + str(counter))

def setup_API():
    """Get twitter API Credentials"""

    consumerKey = ''
    consumerSecret = ''
    accessToken = ''
    accessSecret = ''

    auth = tweepy.OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessSecret)

    return tweepy.API(auth, wait_on_rate_limit=True)




"""
This was the original data cleaning that implemented with the web scraping of past twitter data.
I no longer want to perform these task together as in hindsight it is more beneficial to 
have the original data, that can later be transformed for analysis.


# Clean the data
date_txt, tweet_txt  = i.split(' /%/ ')
formated_date = date(int(date_txt[18:22]), int(date_txt[23:25]), int(date_txt[26:29]))
tweet_txt = tweet_txt.strip("''\n" )
tweet_txt = tweet_txt.replace('\\n\\n', ' ')
cleaned_tweet = clean(tweet_txt)

# Organize the data to be sent to Kafka
record = '' + str(formated_date) + ' : ' + cleaned_tweet
"""