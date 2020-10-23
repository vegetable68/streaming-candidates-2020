from google.cloud import datastore
import logging
import json
import datetime
import hashlib
from collections import defaultdict

LOGINTERVAL = 10000

class TwitterUpdates:

  def __init__(self, project_id, service_account=None): 
    # Connect to database
    if service_account:
      self.client = datastore.Client.from_service_account_json(service_account)
    else:
      self.client = datastore.Client(project_id)
    self.TIMEFORMAT = "%a %b %d %H:%M:%S %z %Y" 
    self.cnts = 0
    self.buffer = []
    self.THERESHOLD = 100
    self.inBuffer = defaultdict(dict)
    logging.info("Database {} created.".format(self.client.project))

  def existedField(self, field, record):
    return field in record and record[field] is not None

  def write_record(self, record, kind):
    # Check duplicates before inserting records
    if ('_id' in record):
      if kind not in self.inBuffer:
          self.inBuffer[kind] = {}
      if (record['_id'] not in self.inBuffer[kind]):
        if self.buffer == []:
          logging.info("One Record written.") 
        key = self.client.key(kind, record['_id']) 
        self.inBuffer[kind][record['_id']] = True
        entity = datastore.Entity(key=key)
        for key, val in record.items():
          if not(key == '_id'):
             entity[key] = record[key]
        self.buffer.append(entity)
    else:
       key = self.client.key(kind) 
       entity = datastore.Entity(key=key)
       for key, val in record.items():
         entity[key] = record[key]
       self.buffer.append(entity)
    if len(self.buffer) >= self.THERESHOLD:
       length = len(self.buffer)
       self.client.put_multi(self.buffer)
       self.buffer = []
       for k in self.inBuffer.keys():
         self.inBuffer[k] = {}
       logging.info("{} records written at {}".format(length, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

  def existed(self, record_id, kind):
    key = self.client.key(kind, record_id)
    return (self.client.get(key) is not None) and (record_id not in self.inBuffer[kind])

  def update_value(self, record_id, kind, field, value, operation):
    key = self.client.key(kind, record_id)
    entity = self.client.get(key)
    if entity is None:
      logging.warning("Record {} does not exist in table {}.".format(record_id, kind))
    else:
      if operation == 'set': 
         entity[field] = value
      if operation == 'inc':
         entity[field] += value
      self.client.put(entity)

  def update_place(self, place_id, place_info):
    ret = {}
    ret['_id'] = place_id  # string type id
    ret['url'] = place_info['url']
    ret['place_type'] = place_info['place_type']
    ret['name'] = place_info['full_name']
    ret['country'] = place_info['country_code']
    self.write_record(ret, 'places')

  def update_deletion(self, deleted_id, deletion_user):
    # Note that the tweet being deleted may not necessarily exist
    # in the database yet.
    if not(self.existed(deleted_id, 'tweets')): 
      logging.error("{} deleted by {} not found in database.".format(deleted_id, deletion_user))
    else:
      self.update_value(deleted_id, 'tweets', 'deleted', True, 'set')
  
  def update_retweet(self, retweeted_id, data): 
    # Note that the tweet being retweeted may not necessarily exist
    # in the database yet.
    ret = {}
    # One user can retweet one particular tweet at most once.
    ret['_id'] = str(data['user']['id']) + '@' + str(data['retweeted_status']['id'])
    if self.existed(ret['_id'], 'retweets'):
      return
    ret['timestamp'] = datetime.datetime.strptime(data['created_at'], self.TIMEFORMAT)
    ret['user'] = data['user']['id']
    ret['retweeted'] = data['retweeted_status']['id']
    ret['retweetedFrom_user'] = data['retweeted_status']['user']['id']
    self.write_record(ret, 'retweets')


    # Update the retweeted tweet in our db
    self.update_tweet(data['retweeted_status'], data['created_at'])

  def update_hashtags(self, tweet_id, hashtags):
    for hashtag in hashtags:
      ret = {}
      ret['hashtag'] = hashtag
      ret['tweet_id'] = tweet_id
      self.write_record(ret, 'hashtag')

  def update_media(self, tweet_id, media):
    for m in media:
      ret = {}
      ret['tweet_id'] = tweet_id
      ret['media_id'], ret['type'], ret['media_url'] = m 
      self.write_record(ret, 'media')

  def update_urls(self, tweet_id, urls):
    for url in urls:
      ret = {}
      ret['_id'] = url
      ret['tweet_id'] = tweet_id
      ret['url'] = url
      self.write_record(ret, 'url')

  def update_mentions(self, tweet_id, mentions):
    for mention in mentions:
      ret = {}
      ret['tweet_id'] = tweet_id
      ret['mention'] = mention 
      self.write_record(ret, 'mentions')
    
  def update_user(self, user_id, action, user_info):
    ret = {}
    if self.existed(user_id, 'users'):
      self.update_value(user_id, 'users', action, 1, 'inc')
      return
    if user_info:
      ret['_id'] = user_id
      for attr in ['name', 'protected', 'followers_count', 'friends_count', 'verified']:
        ret[attr] = user_info[attr]
      ret['handle'] = user_info['screen_name']
      for attr in ['location', 'url', 'description']:
        ret[attr] = user_info[attr] if attr in user_info else None
      ret['created_at'] = datetime.datetime.strptime(user_info['created_at'], self.TIMEFORMAT)
    else:
      ret['_id'] = user_id 
    ret['friends'] = None
    ret['followed_cnts'] = 0
    self.write_record(ret, 'users')

  def update_tweet(self, data, last_retweeted_date=None):
    ret = {}
    if self.existed(data['id'], 'tweets'): return
    ret['isDeleted'] = False
    ret['timestamp'] = datetime.datetime.strptime(data['created_at'], self.TIMEFORMAT)
    ret['_id'] = data['id']
    ret['text'] = data['text'] if 'extended_tweet' not in data else data['extended_tweet']['full_text'] 
    ret['sha256'] = hashlib.sha256(str.encode(ret['text'])).hexdigest() 
    ret['source'] = data['source'] # Where the tweet was posted, web / mac etc
    ret['replyTo'] = data['in_reply_to_status_id'] if self.existedField('in_reply_to_status_id', data) else None 
    ret['replyTo_user'] = data['in_reply_to_user_id'] if self.existedField('in_reply_to_user_id', data) else None 
    ret['user'] = data['user']['id']
    ret['processed'] = False
    ret['retweet_count'] = data['retweet_count']
    ret['last_retweeted'] = datetime.datetime.strptime(last_retweeted_date, self.TIMEFORMAT) if last_retweeted_date else None
    ret['quote_count'] = data['quote_count']
    ret['coordinates'] = data['coordinates']['coordinates'] if self.existedField('coordinates', data) else None
    ret['place'] = data['place']['id'] if self.existedField('place', data) else None
    if ret['place'] is not None:
      self.update_place(ret['place'], data['place'])
    ret['quote_tweet'] = data['quoted_status_id'] if self.existedField('quoted_status_id', data) else None
    ret['hashtags'] = ([hashtag['text'] for hashtag in data['entities']['hashtags']]
              + ([hashtag['text'] for hashtag in data['extended_tweets']['entities']['hashtags']]
              if self.existedField('extended_tweets', data) else [])) 
    self.update_hashtags(ret['_id'], ret['hashtags'])
    extended_media = ([(media['id'], media['type'], media['media_url']) for media in data['extended_entities']['media']]
                     if (self.existedField('extended_entities', data) and self.existedField('media', data['extended_entities'])) else [])
    ret['media'] = (([(media['id'], media['type'], media['media_url']) for media in data['entities']['media']]
                    if self.existedField('media', data['entities']) else []) 
           + ([(media['id'], media['type'], media['media_url']) for media in data['extended_tweets']['entities']['media']]
           if (self.existedField('extended_tweets', data) and
              self.existedField('entities', data['extended_tweets']) and
              self.existedField('media', data['extended_tweets']['entities'])) else [])
           + extended_media) 
    ret['hasMedia'] = len(ret['media']) > 0
    self.update_media(ret['_id'], ret['media'])
    ret['media'] = [media[0] for media in ret['media']]
    ret['urls'] = ([url['expanded_url'] for url in data['entities']['urls']] 
          + ([url['expanded_url'] for url in data['extended_tweets']['entities']['urls']]
          if self.existedField('extended_tweets', data) else [])) 
    self.update_urls(ret['_id'], ret['urls'])
    #ret['mentions'] = ([mention['id'] for mention in data['entities']['user_mentions']]
    #          + ([mention['id'] for url in data['extended_tweets']['entities']['user_mentions']]
    #          if self.existedField('extended_tweets', data) else [])) 
    # self.update_mentions(ret['_id'], ret['mentions'])
    # Ignoring Twitter polls and symbols
    self.write_record(ret, "tweets")
    self.cnts += 1
    if self.cnts % LOGINTERVAL == 0:
      logging.info("{} tweets collected.".format(self.cnts))

