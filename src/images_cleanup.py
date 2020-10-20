import tweepy
import json
import datetime
import logging
import time
import os
from google.cloud import datastore
from google.cloud import storage
import sys
import argparse
import traceback
import socket
import re
import smtplib

from tendo import singleton

me = singleton.SingleInstance()
PROJECT = "yiqing-2020-twitter"
LOGINTERVAL = 10
service_account = "/home/yiqing/credentials/service_account.json"

class imageSearch:

  def __init__(self, service_account): 
    # Connect to database
    self.client = datastore.Client.from_service_account_json(service_account)
    self.TIMEFORMAT = "%a %b %d %H:%M:%S %z %Y" 
    self.buffer = []
    self.THERESHOLD = 50
    logging.info("Database {} created.".format(self.client.project))

  def search(self): 
    query = self.client.query(kind='media')
    query.add_filter('type', '=', 'photo-processed') 
    imagelist = list(query.fetch(limit=50))
    fetched_images = []
    cnts = 0
    for cnts, image in enumerate(imagelist):
      total = 0
      #imageId = image.id
      #if imageId in uploaded:
      #  continue
      #imageUrl = image['media_url']
      #cmd = "wget -O /home/yiqing/tmp/{}.jpg {}".format(image.id, image['media_url'])
      #rt = os.system(cmd)
      #size = os.path.getsize('/home/yiqing/tmp/{}.jpg'.format(image.id))
      #if size == 0:
      #  os.system('rm /home/yiqing/tmp/{}.jpg'.format(image.id))
      #  rt = 'failed'
      #else:
      #  try: 
      #    os.system("gsutil cp /home/yiqing/tmp/{} gs://yiqing-2020-candidates-images/".format(image.id))
      #    os.system("rm /home/yiqing/tmp/{}".format(image.id))
      #    with open("/home/yiqing/uploaded_images", "a") as w:
      #      w.write(json.dumps(imageId) + '\n')
      #  except:
      #    pass
      image['type'] = 'photo'
      self.client.put(image)
    return cnts
  
if __name__ == '__main__':
  logDate = datetime.datetime.now()
  logging.basicConfig(level=logging.INFO)

  #uploaded = set()
  #with open("/home/yiqing/uploaded_images" , "r") as r:
  #  for line in r:
  #    uploaded.add(json.loads(line))
  storage_client = datastore.Client.from_service_account_json(service_account)
  searcher = imageSearch(service_account)
  searchCnts = searcher.search()
  increment = searchCnts

  while increment >= 49: 
    increment = searcher.search()
    searchCnts += increment
    if searchCnts % LOGINTERVAL == 0:
      logging.info("{} images fetched.".format(searchCnts))
    
