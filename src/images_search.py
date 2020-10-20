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

from tendo import singleton

me = singleton.SingleInstance()
# Use abslute path here
PROJECT = "[YOUR GCP CLOUD PROJECT]"
LOGINTERVAL = 10
service_account = "[YOUR GCP SERVICE ACCOUNT]"
TMPDIR = "[TMP DIR]"
BUCKET = "[CLOUD BUCKET FOR IMAGES]"
UPLOADED_IMAGES = "[A FILE OF UPLOADEDE IMAGES]" # to check if the uploading was successful, compare this file with the images stored in cloud bucket

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
    query.add_filter('type', '=', 'photo') 
    imagelist = list(query.fetch(limit=50))
    fetched_images = []
    cnts = 0
    for cnts, image in enumerate(imagelist):
      total = 0
      imageId = image.id
      imageUrl = image['media_url']
      cmd = "wget -O {}{}.jpg {}".format(TMPDIR, image.id, image['media_url'])
      rt = os.system(cmd)
      size = os.path.getsize('{}{}.jpg'.format(TMPDIR, image.id))
      if size == 0:
        os.system('rm {}{}.jpg'.format(TMPDIR, image.id))
      fetched_images.append(image)

    rt = os.system("gsutil cp {}* gs://{}/".format(TMPDIR, BUCKET))
    os.system("rm {}*".format(TMPDIR))
    if rt:
      return cnts

    for image in fetched_images:
      image['type'] = 'photo-fetched'
      self.client.put(image)
      with open("{}".format(UPLOADED_IMAGES), "a") as w:
        w.write(json.dumps(image.id) + '\n')

    return cnts
  
if __name__ == '__main__':
  logDate = datetime.datetime.now()
  logging.basicConfig(level=logging.INFO)

  searcher = imageSearch(service_account)
  searchCnts = searcher.search()
  increment = searchCnts

  while increment >= 49: 
    increment = searcher.search()
    searchCnts += increment
    if searchCnts % LOGINTERVAL == 0:
      logging.info("{} images fetched.".format(searchCnts))
    
