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
from dotenv import load_dotenv
load_dotenv()

from tendo import singleton

me = singleton.SingleInstance()
# Use abslute path here
PROJECT = os.getenv("GCP_PROJECT")
LOGINTERVAL = 10
TMPDIR = os.getenv("IMAGES_TMP_DIR")
BUCKET = os.getenv("GCP_BUCKET")
UPLOADED_IMAGES = os.getenv("IMAGES_UPLOADED_FILE") # to check if the uploading was successful, compare this file with the images stored in cloud bucket

class imageSearch:

  def __init__(self): 
    # Connect to database
    self.client = datastore.Client()
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
      cmd = "wget -O {}{}.jpg {}".format(TMPDIR, image.id, image['media_url'])
      rt = os.system(cmd)
      size = os.path.getsize('{}{}.jpg'.format(TMPDIR, image.id))
      if size == 0:
        os.system('rm {}{}.jpg'.format(TMPDIR, image.id))
      fetched_images.append(image)

    if len(imagelist) == 0:
      logging.info("No images fetched from the datastore")
    else:
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

  searcher = imageSearch()
  searchCnts = searcher.search()
  increment = searchCnts

  while increment >= 49: 
    increment = searcher.search()
    searchCnts += increment
    if searchCnts % LOGINTERVAL == 0:
      logging.info("{} images fetched.".format(searchCnts))
    
