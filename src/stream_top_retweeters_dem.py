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

me = singleton.SingleInstance()
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/yiqing/credentials/service_account.json"

TIMEFORMAT = "%a %b %d %H:%M:%S %z %Y"
LOGS_DIR = "../logs/"

EMAIL = "/home/yiqing/candidates-on-twitter/private/email"

with open(EMAIL, "r") as f:
  email = json.load(f)

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

  party = 'dem'
  pid = os.getpid()
  with open("/home/yiqing/candidates-on-twitter/data/pid_{}".format(party), 'w') as w:
    json.dump(pid, w) 


  logDate = datetime.datetime.now()
  if not os.path.exists(LOGS_DIR):
    os.mkdir(LOGS_DIR)

  logging.basicConfig(level=logging.INFO)
  logging.info('Register Crendentials')
  CREDENTIALS = "/home/yiqing/candidates-on-twitter/private/credentials"
  
  configs = []
  with open(CREDENTIALS) as f:
    for ind, line in enumerate(f):
      configs.append(json.loads(line)) 
  credential = 2
  config = configs[credential]

  # Configure API
  auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
  auth.set_access_token(config['access_token'], config['access_token_secret'])
  api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
  userids = []
  candidates = [] 
  
  with open('/home/yiqing/candidates-on-twitter/data/top_5000_{}.json'.format(party), "r") as r:
    userids = json.load(r)
  userids = [str(x) for x in userids]

  logging.info("Start streaming at {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
  stream_listener = StreamListener()
  stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
  updator = TwitterUpdates('top-retweeters-stream', service_account='/home/yiqing/credentials/top_retweeters_stream.json')
  backoff_counter = 60 * 15
  lasttime = None
  while True:
    stream.filter(follow=userids,
            stall_warnings=True)
  logging.errorr('error at {}.'.format(curtime.strftime("%Y-%m-%d %H:%M:%S")))

