import tweepy 
import json
import datetime
import os.path
import pandas as pd

class StreamListener(tweepy.StreamListener):
  def on_data(self, raw_data):
    data = json.loads(raw_data)
    with open("../data/output_sample.json", "a") as f: 
      f.write(raw_data)

  def on_status(self, status):
    print(status.text)
  
  def on_error(self, status_code):
    # TODO(yiqing): what are other status codes to add
    if status_code == 420:
      print("Hit Rate Limit")
      return False

CREDENTIALS = "../private/credentials"
DESIGNATED_LINE = 0
with open(CREDENTIALS) as f:
  for ind, line in enumerate(f):
    config = json.loads(line)
    if ind == DESIGNATED_LINE:
      break

# Configure API
auth = tweepy.OAuthHandler(config['consumer_key'], config['consumer_secret'])
auth.set_access_token(config['access_token'], config['access_token_secret'])
api = tweepy.API(auth)


if os.path.isfile('../data/userids'):
  with open("../data/userids", "r") as r:
    userids = json.load(r)
else:
  with open("../data/candidates.csv", "r") as r:
    df = pd.read_csv(r)
  usernames = [u[1:] for u in df['twitter handle'].values]
  
  userids = []
  for username in usernames:
    user = api.get_user(username)._json
    userids.append(user['id_str'])
  with open('../data/userids', "w") as w:
    json.dump(userids, w)

print(datetime.datetime.now())
stream_listener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(follow=userids)


