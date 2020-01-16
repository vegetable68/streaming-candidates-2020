import tweepy
import json
import datetime
import logging
import time
import os
from google.cloud import datastore
import sys
import argparse
import traceback
import socket
import re
import smtplib

PROJECT = "yiqing-2020-twitter"
CREDENTIALS = "../private/credentials"
LOGINTERVAL = 10
LOGS_DIR = "../logs/"

class friendshipSearch:

  def __init__(self, project_id, config): 
    # Connect to database
    self.client = datastore.Client(project_id)
    self.TIMEFORMAT = "%a %b %d %H:%M:%S %z %Y" 
    self.buffer = []
    self.THERESHOLD = 5000
    logging.info("Database {} created.".format(project_id))
    # Configure API
    auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
    #auth.set_access_token(config['access_token'], config['access_token_secret'])
    self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

  def update_user(self, user_id):
    ret = {}
    kind = 'users'
    key = self.client.key(kind, user_id)
    entity = self.client.get(key)
    if entity is not None: 
      entity['followed_cnts'] += 1
      self.buffer.append(ret)
      if len(self.buffer) >= self.THERESHOLD:
        self.client.put_multi(self.buffer)
        self.buffer = []
      return
    ret = datastore.Entity(key=key)
    ret['cnts'] = 0
    ret['friends'] = None
    ret['followed_cnts'] = 1
    self.buffer.append(ret)
    if len(self.buffer) >= self.THERESHOLD:
      self.client.put_multi(self.buffer)
      self.buffer = []

  def search(self): 
    query = self.client.query(kind='users')
    query.add_filter('friends', '=', None) 
    query.order = ['-cnts']
    userList = list(query.fetch(limit=5))
    cnts = 0
    for cnts, user in enumerate(userList):
      total = 0
      user_id = user.id
      logging.debug("User {} has {} friends, start fetching.".format(user_id, user['friends_count'] if 'friends_count' in user else 'unknown'))
      backoff_counter = 0
      while True:
        try:
          followingList = self.api.friends_ids(user_id)
          break
        except tweepy.TweepError as e:
          logging.error(e.reason)
          try:
            if e.reason[0]['code'] == 34:
              followingList = "NOT EXISTED" 
              break
          except:
            pass
          if "Not authorized." in e.reason:
            followingList = "PROTECTED" 
            break
          time.sleep(60*backoff_counter)
          backoff_counter += 1
          continue
      #logging.info("{} fetched.".format(len(followingList)))
      #key_buffer = []
      #for p in followingList:
      #  kind = 'users'
      #  key = self.client.key(kind, p)
      #  key_buffer.append(key)
      #missingList = []
      #entities = self.client.get_multi(key_buffer, missing=missingList) 
      #entity_buffer = []
      #logging.info("{} queried.".format(len(followingList)))
      #logging.info("{} are missing.".format(len(missingList)))
      #for entity in missingList:
      #  entity['cnts'] = 0
      #  entity['friends'] = None
      #  entity['followed_cnts'] = 1
      #  entity_buffer.append(entity)
      #for entity in entities:
      #  entity['followed_cnts'] += 1 
      #  entity_buffer.append(entity)
      #logging.info("{} updated.".format(len(entity_buffer)))
      #self.client.put_multi(entity_buffer)
      #followingList = list(set(followingList))
      #logging.debug("User {} has {} friends, friend cnt {}.".format(user_id, total, user['friends_count'] if 'friends_count' in user else 'unknown'))
      user['friends'] = followingList 
      self.client.put(user)
    return cnts
  
if __name__ == '__main__':
  logDate = datetime.datetime.now()
  if not os.path.exists(LOGS_DIR):
    os.mkdir(LOGS_DIR)

  #logging.basicConfig(filename='{}/friendship-search-{}.log'.format(LOGS_DIR, logDate.strftime("%Y-%m-%d %H:%M:%S")), level=logging.DEBUG)
  logging.basicConfig(level=logging.INFO)

  parser = argparse.ArgumentParser()
  parser.add_argument('--credential', default=0, type=int)
  args = parser.parse_args()

  credentials = []
  with open(CREDENTIALS) as f:
    for line in f:
      credentials.append(json.loads(line))

  searcher = friendshipSearch(PROJECT, credentials[args.credential])
  searchCnts = searcher.search()
  increment = searchCnts
  while increment >= 4: 
    increment = searcher.search()
    searchCnts += increment
    if searchCnts % LOGINTERVAL == 0:
      logging.info("{} user friendship fetched.".format(searchCnts))
    
