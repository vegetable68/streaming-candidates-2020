import tweepy
import sys
import json
import datetime
import logging
import argparse
import os
import traceback
import time
import socket
import re
import smtplib
import pandas as pd
from update import TwitterUpdates
from tendo import singleton
from dotenv import load_dotenv

load_dotenv()

me = singleton.SingleInstance()
CREDENTIALS = os.getenv("TWITTER_CREDENTIALS")
KEYWORDS_PATH = "data/keywords"
PROJECT = os.getenv("GCP_PROJECT")

TIMEFORMAT = "%a %b %d %H:%M:%S %z %Y"

class StreamListener(tweepy.StreamListener):

  def on_data(self, raw_data):
    data = json.loads(raw_data)
    
    if 'delete' in data:
      # Deletion of tweets
      try:
        deleted_id = data['delete']['status']['id']
        deletion_user = data['delete']['status']['user_id'] 
      except:
        logging.error("Missing Fields in deletion " + str(raw_data))
      try:
        updator.update_deletion(deleted_id, deletion_user)
      except:
        logging.error("Deletion Error for tweet {} by user {}".format(deleted_id, deletion_user))
    elif 'retweeted_status' in data:
      # Retweet of other tweets 
      try:
        retweeted_id = data['retweeted_status']['id']
        retweet_user = data['user']
      except:
        logging.error("Missing Fields in retweet " + str(raw_data)) 
      updator.update_retweet(retweeted_id, data)
    elif 'limit' in data:
      logging.error("Rate Limit Notice: " + str(raw_data))
      # Limit notices
      if self.on_limit(data['limit']['track']) is False:
          return False
    elif 'disconnect' in data:
      logging.debug("Disconnect: " + str(raw_data))
      # Disconnect messages
      if self.on_disconnect(data['disconnect']) is False:
          return False
    elif 'warning' in data:
      logging.error("Stall warning: " + data['warning'])
      # Stall warnings
      if self.on_warning(data['warning']) is False:
        return False
    else:
      # Standard tweet data  
      # Candidate posts / reply to candidates
      #try:
      updator.update_tweet(data)
      #except: 
      # Unknown data type
      #  logging.error("Unknown message type: " + str(raw_data))
      #  return False

  def on_status(self, status):
    logging.info(status.text)
  
  def on_error(self, status_code):
    return False
    #if status_code == 420:
    #  logging.error("Hit Rate Limit")
    #  time.sleep(backoff_counter)
    #  backoff_counter *= 2
    #  return False

if __name__ == '__main__':
  parser = argparse.ArgumentParser()

  logDate = datetime.datetime.now()
  logging.basicConfig(level=logging.INFO)
  logging.info('Register Crendentials')
  
  configs = []
  with open(CREDENTIALS) as f:
    for ind, line in enumerate(f):
      configs.append(json.loads(line)) 
  credential = 0
  config = configs[credential]
 
  # Configure API
  auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
  auth.set_access_token(config['access_token'], config['access_token_secret'])
  api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
  keywords = []
  with open(KEYWORDS_PATH, "r") as r:
    for line in r:
      keywords.append(line.strip())
  
  logging.info("Start streaming at {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
  stream_listener = StreamListener()
  stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
  updator = TwitterUpdates(PROJECT)
  lasttime = None
  while True:
      stream.filter(track=keywords,stall_warnings=True)
  logging.errorr('error at {}.'.format(curtime.strftime("%Y-%m-%d %H:%M:%S")))

