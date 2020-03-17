from google.cloud import datastore
import json
import os
import datetime
import pandas as pd
from third_party.pdqhashing.hasher.pdq_hasher_new import PDQHasher

datastoreClient = datastore.Client()


if data['type'] == 'photo':
  cnt += 1
  cmd = "wget -O /hdd/yiqing/twitter_imgs/{}.jpg {}".format(data['media_id'], data['media_url'])
  rt = os.system(cmd)
  if rt != 0:
    with open("../data/twitter_media_not_found", "a") as w:
      w.write(line)
