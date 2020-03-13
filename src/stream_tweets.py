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

TIMEFORMAT = "%a %b %d %H:%M:%S %z %Y"
LOGS_DIR = "../logs/"

EMAIL = "../private/email"

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
  parser = argparse.ArgumentParser()
  parser.add_argument('--credential', default=0, type=int)
  parser.add_argument('--handles', default=0, type=int)
  args = parser.parse_args()

  logDate = datetime.datetime.now()
  if not os.path.exists(LOGS_DIR):
    os.mkdir(LOGS_DIR)

  logging.basicConfig(level=logging.INFO)
  logging.info('Register Crendentials')
  CREDENTIALS = "../private/credentials"
  
  configs = []
  with open(CREDENTIALS) as f:
    for ind, line in enumerate(f):
      configs.append(json.loads(line)) 
  config = configs[args.credential]
  with open("../data/candidates.csv", "r") as r:
    df = pd.read_csv(r)
  usernames = [u[1:] for u in df['twitter handle'].values]
 
  # Configure API
  auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
  auth.set_access_token(config['access_token'], config['access_token_secret'])
  api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
  userids = []
  track_mentions = []
  candidates = [] 
  if os.path.exists("../data/candidates-{}".format(args.handles)):
    with open('../data/candidates-{}'.format(args.handles), "r") as r:
      candidates = json.load(r)
    for candidate in candidates:
      userids.append(str(candidate['id'])) 
  else:
    for username in usernames:
      print(username)
      user = api.get_user(username)._json
      track_mentions.append('@{}'.format(username))
      candidate = {"handle": username}
      for attr in ['name', 'protected', 'followers_count', 'friends_count', 'verified', 'id']:
        candidate[attr] = user[attr]
      for attr in ['location', 'url', 'description']:
        candidate[attr] = user[attr] if attr in user else None
      candidate['created_at'] = datetime.datetime.strptime(user['created_at'], TIMEFORMAT).strftime("%Y-%m-%d %H:%M:%S")
      candidates.append(candidate)
      userids.append(user['id_str'])
    with open('../data/candidates-{}'.format(args.handles), "w") as w:
      json.dump(candidates, w)
  
  logging.info("Start streaming at {}".format(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
  stream_listener = StreamListener()
  stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
  updator = TwitterUpdates('yiqing-2020-twitter')
  backoff_counter = 60 * 15
  while True:
    try:
      stream.filter(follow=userids, #track=track_mentions,
              stall_warnings=True)
    except:
      logging.error("Hit Rate Limit")
      time = datetime.datetime.now()
      Email = {'TO':email['to'], 'FROM':email['user'],
               'SUBJECT':'Hit Rate limit',
               'BODY':'hit rate limit at {}, retry in {} seconds.'.format(backoff_counter,
                   time.strftime("%Y-%m-%d %H:%M:%S"))}

      thisHost = socket.gethostname()
      thisIP = socket.gethostbyname(thisHost)


      Email['ALL'] = 'From: %s\nTo: %s\nSubject: %s\n\n%s' % (Email['FROM'], Email['TO'],
              Email['SUBJECT'], Email['BODY'])
      server = smtplib.SMTP_SSL(email['server'])
      #server.starttls()
      #server.connect(email['server'], 465)
      #server.ehlo()
      server.login(email['user'], email['password'])
      server.sendmail(Email['FROM'], Email['TO'], Email['ALL'])
      server.quit()

      time.sleep(backoff_counter)
      backoff_counter *= 2

      auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
      auth.set_access_token(config['access_token'], config['access_token_secret'])
      api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

      stream_listener = StreamListener()
      stream = tweepy.Stream(auth=api.auth, listener=stream_listener)

      logging.info("Reconnect")
      continue

