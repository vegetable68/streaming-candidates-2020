from google.cloud import datastore
from scoreUtils import *
from nltk import TweetTokenizer
from nltk.util import ngrams
import logging

PROJECT = "yiqing-twitter-candidates"
BATCHSIZE = 500

class tweetScorer:
  def __init__(self, project_id): 
    # Connect to database
    self.client = datastore.Client(project_id)
    self.TIMEFORMAT = "%a %b %d %H:%M:%S %z %Y" 
    logging.info("Database {} created.".format(project_id))

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  scorer = tweetScorer(PROJECT) 
  tknzr = TweetTokenizer()
  cursor = None
  while True:
    query = scorer.client.query(kind="tweets")
    query_iter = query.fetch(start_cursor=cursor, limit=BATCHSIZE)
    tweets = list(next(query_iter.pages))
    cursor = query_iter.next_page_token
    updateList = []
    cnts = 0
    if len(tweets) == 0:
      break
    for entity in tweets:
      tweet = replace_pattern(entity['text'])
      unigrams = tknzr.tokenize(entity['text'])
      entity['bigrams'] =json.dumps(list(ngrams(unigrams, 2)))
      noError = 0
      updateList.append(entity)
      cnts += 1
    scorer.client.put_multi(updateList)
    logging.info("{} out of {} tweets tokenized.".format(cnts, BATCHSIZE))
    if cursor is None: break 
    
