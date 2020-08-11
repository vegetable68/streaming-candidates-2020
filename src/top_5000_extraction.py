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
from datetime import date

PROJECT = "yiqing-2020-twitter"
CREDENTIALS = "../private/credentials"
LOGINTERVAL = 10
LOGS_DIR = "../logs/"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/yiqing/credentials/service_account.json"



fields = ['Democratic_retweet', 'Republican_retweet', 'Democratic_tweet', 'Republican_tweet', 'retweet', 'tweet']

class friendshipSearch:

  def __init__(self, project_id): 
    # Connect to database
    self.client = datastore.Client(project_id)
    self.TIMEFORMAT = "%a %b %d %H:%M:%S %z %Y" 
    self.buffer = []
    self.THERESHOLD = 5000
    logging.info("Database {} created.".format(project_id))

  def search(self): 
    ret = {'dem': [], 'rep': []}
    query = self.client.query(kind='users')
    query.order = ['-Democratic_retweet']
    userList = list(query.fetch(limit=5000))
    democrat_retweeters  = [user.id for user in userList]
    for user in userList:
      cur = {'id': user.id}
      for f in fields:
        cur[f] = user[f]
      ret['dem'].append(cur)

    query = self.client.query(kind='users')
    query.order = ['-Republican_retweet']
    userList = list(query.fetch(limit=5000))
    republican_retweeters  = [user.id for user in userList]
    for user in userList:
      cur = {'id': user.id}
      for f in fields:
        cur[f] = user[f]
      ret['rep'].append(cur)
    return ret, democrat_retweeters, republican_retweeters

  
if __name__ == '__main__':
  logDate = datetime.datetime.now()
  if not os.path.exists(LOGS_DIR):
    os.mkdir(LOGS_DIR)

  #logging.basicConfig(filename='{}/friendship-search-{}.log'.format(LOGS_DIR, logDate.strftime("%Y-%m-%d %H:%M:%S")), level=logging.DEBUG)
  logging.basicConfig(level=logging.INFO)

  searcher = friendshipSearch(PROJECT)
  ret, dem, rep= searcher.search()
  timestamp = date.today().strftime("%Y-%m-%d")
  ret['timestamp'] = timestamp
  with open("/home/yiqing/candidates-on-twitter/data/top_5000_{}.json".format(timestamp), "w") as w:
    json.dump(ret, w)
  with open("/home/yiqing/candidates-on-twitter/data/top_5000_dem.json", "w") as w:
    json.dump(dem, w)
  with open("/home/yiqing/candidates-on-twitter/data/top_5000_rep.json", "w") as w:
    json.dump(rep, w)

  os.system("gsutil cp /home/yiqing/candidates-on-twitter/data/top_5000_{}.json gs://twitter_2020_top_retweeters_week/records/".format(timestamp)) 
  os.system("rm /home/yiqing/candidates-on-twitter/data/top_5000_{}.json".format(timestamp)) 
  logging.info("extracted on {}".format(timestamp))
