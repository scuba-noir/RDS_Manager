import re 
import numpy as np
import tweepy 
from tweepy import OAuthHandler 
from textblob import TextBlob 
import matplotlib.pyplot as plt
import pandas as pd
from wordcloud import WordCloud
from better_profanity import profanity

import warnings
warnings.filterwarnings('ignore')

# Remember to keep your Keys and Tokens a secret!

consumer_key = '51aRWNOMsANW2OCdghwOQ4MKt'
consumer_secret = 'eHcBhFw2nJoyLuEIZlK4D5nry1R73BZnfwab2f3gFVoFEilBhW' 
access_token = '3391228701-jbPhBGegPdydLOr4HUwAi7QlxlNHT4zfbMc5Vme'
access_token_secret = 'nuDJM0aLc9Vo3WFOKbjRAud8NUyp9jxOoOYt5fLlBOwWq'

# Create a function to clean the tweets. Remove profanity, unnecessary characters, spaces, and stopwords.

def clean_tweet(tweet):
    if type(tweet) == np.float:
        return ""
    r = tweet.lower()
    #r = profanity.censor(r)
    r = re.sub("'", "", r) # This is to avoid removing contractions in english
    r = re.sub("@[A-Za-z0-9_]+","", r)
    r = re.sub("#[A-Za-z0-9_]+","", r)
    r = re.sub(r'http\S+', '', r)
    r = re.sub('[()!?]', ' ', r)
    r = re.sub('\[.*?\]',' ', r)
    r = re.sub("[^a-z0-9]"," ", r)
    r = r.split()
    stopwords = ["for", "on", "an", "a", "of", "and", "in", "the", "to", "from", "petrobras"]
    r = [w for w in r if not w in stopwords]
    r = " ".join(word for word in r)
    return r

def main():

    #topics = ['sugar', 'sugar prices', 'sugar exports', 'wheat', 'wheat prices', 'wheat exports', 'corn, corn prices', 'corn exports', 'soybeans', 'soybean prices', 'soybean exports', 'coffee', 'coffee prices', 'coffee exports']
    topics = ['sugar', 'wheat', 'corn', 'soybeans', 'coffee']
    # Access Twitter Data
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    data_ls = []
    final_df = pd.DataFrame()
    date_ls = pd.date_range(start="2022-10-27",end="2022-11-23").to_pydatetime().tolist()
    # Input a query from the user
    for items in topics:
        query = items
  
        for i in range(0, len(date_ls)-1):
            temp_ls = []
            temp_ls.append(items)
            date_i = date_ls[i]
            date_f = date_ls[i+1]
            temp_ls.append(date_f)
            # In this case, we will input the query as 'Elon Musk'
            # Filter the query to remove retweets
            filtered = query + " until:" + str(date_f) + " -filter:retweets"
            # Generate the latest tweets on the given query 
            tweets = tweepy.Cursor(api.search_tweets, 
                                    q=filtered,
                                    lang="en").items(100)

            list1 = [[tweet.text, tweet.user.screen_name, tweet.user.location] for tweet in tweets]
            # Convert the list into a dataframe
            df = pd.DataFrame(data=list1, 
                                columns=['tweets','user', "location"])
            # Convert only the tweets into a list
            tweet_list = df.tweets.to_list()
            cleaned = [clean_tweet(tw) for tw in tweet_list]
            # Define the sentiment objects using TextBlob
            sentiment_objects = [TextBlob(tweet) for tweet in cleaned]
            sentiment_objects[0].polarity, sentiment_objects[0]
            # Create a list of polarity values and tweet text
            sentiment_values = [[tweet.sentiment.polarity, str(tweet)] for tweet in sentiment_objects]
            # Print the value of the 0th row.
            sentiment_values[0]
            # Print all the sentiment values
            sentiment_values[0:99]
            # Create a dataframe of each tweet against its polarity
            sentiment_df = pd.DataFrame(sentiment_values, columns=["polarity", "tweet"])
            # Save the polarity column as 'n'.
            n=sentiment_df["polarity"]
            m=pd.Series(n)
            pos=0
            neg=0
            neu=0
            # Create a loop to classify the tweets as Positive, Negative, or Neutral.
            # Count the number of each.
            for items in m:
                if items>0:
                    pos=pos+1
                elif items<0:
                    neg=neg+1
                else:
                    neu=neu+1

            temp_ls.append(pos)
            temp_ls.append(neg)
            temp_ls.append(neu)
            print("%f percent of twitter users feel positive about %s"%(pos,query))
            print("%f percent of twitter users feel negative about %s"%(neg,query))
            print("%f percent of twitter users feel neutral about %s"%(neu,query))
            print('-----------------------------')
            data_ls.append(temp_ls)
            temp = pd.DataFrame(data_ls)
            temp.to_csv("final_twitter_data_"+str(topics)+".csv", index=False)
    
    return data_ls

def main_thiago():

    data_df = pd.read_excel("NLP.xlsx", sheet_name='Sheet2', index_col=False)
    data_ls = []
    final_df = pd.DataFrame()
    final_ls = []
    final_subj = []
    for items, values in data_df.iterrows():
           
        #Define the sentiment objects using TextBlob
        #sentiment_objects = clean_tweet(values.Headline)
        sentiment_objects = [TextBlob(sentiment_objects)]
        #Create a list of polarity values and tweet text
        sentiment_values = [nice.sentiment.polarity for nice in sentiment_objects]
        subj_values = [tweet.sentiment.subjectivity for tweet in sentiment_objects]
        sent_ass = [tweet.sentiment_assessments for tweet in sentiment_objects]
        #print(sent_ass)
        tweet = [tweet for tweet in sentiment_objects]
        #Print the value of the 0th row.
        final_ls.append(sentiment_values[0])
        final_subj.append(subj_values[0])
        continue

    data_df['polarity'] = final_ls 
    data_df['subjectivity'] = final_subj
    return data_df

df = main_thiago()
df.to_excel("score_list_new.xlsx", index=False)
print(df)

