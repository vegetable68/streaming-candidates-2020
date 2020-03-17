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
LOGINTERVAL = 10
LOGS_DIR = "../logs/"

class imageSearch:

  def __init__(self, project_id): 
    # Connect to database
    self.client = datastore.Client(project_id)
    self.TIMEFORMAT = "%a %b %d %H:%M:%S %z %Y" 
    self.buffer = []
    self.THERESHOLD = 50
    logging.info("Database {} created.".format(project_id))

  def search(self): 
    query = self.client.query(kind='media')
    query.add_filter('type', '=', 'photo') 
    imagelist = list(query.fetch(limit=50))
    cnts = 0
    for cnts, image in enumerate(imagelist):
      total = 0
      imageId = image.id
      imageUrl = image['media_url']
      cmd = "wget -O ../tmp/{}.jpg {}".format(image.id, image['media_url'])
      rt = os.system(cmd)
      size = os.path.getsize('../tmp/{}.jpg'.format(image.id))
      if size == 0:
        os.system('rm ../tmp/{}.jpg'.format(image.id))
        rt = 'failed'
      image['type'] = 'photo-{}'.format(rt)
      self.client.put(image)
    if cnts > 0: 
      os.system("gsutil cp ../tmp/* gs://yiqing-2020-images/")
      os.system("rm ../tmp/*")
    return cnts
  
if __name__ == '__main__':
  logDate = datetime.datetime.now()
  if not os.path.exists(LOGS_DIR):
    os.mkdir(LOGS_DIR)

  logging.basicConfig(level=logging.INFO)

  searcher = imageSearch(PROJECT)
  searchCnts = searcher.search()
  increment = searchCnts
  while increment >= 49: 
    increment = searcher.search()
    searchCnts += increment
    if searchCnts % LOGINTERVAL == 0:
      logging.info("{} images fetched.".format(searchCnts))
    
