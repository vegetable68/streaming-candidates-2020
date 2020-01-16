import tweepy
import json
import datetime
import logging
from update import TwitterUpdates

TESTFILE = "../data/test.json"

def update(raw_data, updator):
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
    updator.update_user(deletion_user)
  elif 'retweeted_status' in data:
    # Retweet of other tweets 
    pass 
    #try:
    #  retweeted_id = data['retweeted_status']['id']
    #  retweet_user = data['user']
    #except:
    #  logging.error("Missing Fields in retweet " + str(raw_data)) 
    #updator.update_retweet(retweeted_id, data)
    #updator.update_user(retweet_user['id'], retweet_user)
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
    updator.update_tweet(data)

if __name__ == '__main__':
  updator = TwitterUpdates("yiqing-twitter-harassment")
  logging.basicConfig(level=logging.DEBUG)
  logging.debug("Test Start")
  with open(TESTFILE, "r") as f:
    for line in f:
      update(line, updator)


