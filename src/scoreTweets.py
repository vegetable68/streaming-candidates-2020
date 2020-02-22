from scoreUtils import *
from google.cloud import datastore
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from nltk import TweetTokenizer
import logging

PROJECT = "yiqing-2020-twitter"
CREDENTIAL = "../private/perspective"
BATCHSIZE = 100
ATTRIBUTES = ['TOXICITY', 'SEVERE_TOXICITY', 'IDENTITY_ATTACK', 'INSULT',
'PROFANITY', 'THREAT', 'SEXUALLY_EXPLICIT', 'FLIRTATION',
'INFLAMMATORY', 'OBSCENE'] # NYT based data

class tweetScorer:
  def __init__(self, project_id): 
    # Connect to database
    self.client = datastore.Client(project_id)
    self.TIMEFORMAT = "%a %b %d %H:%M:%S %z %Y" 
    logging.info("Database {} created.".format(project_id))

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  scorer = tweetScorer(PROJECT) 
  with open(CREDENTIAL, "r") as f:
    for line in f:
      PERSPECTIVE_KEY = line
  analyzer = SentimentIntensityAnalyzer()
  tknzr = TweetTokenizer()
  while True:
    query = scorer.client.query(kind="tweets")
    query.add_filter('processed', '=', False)
    query.order = ['-timestamp']
    tweets = list(query.fetch(limit=BATCHSIZE))
    updateList = []
    cnts = 0
    if len(tweets) == 0:
      break
    for entity in tweets:
      tweet = replace_pattern(entity['text'])
      perspectiveScore = call_perspective_api(tweet, ATTRIBUTES, PERSPECTIVE_KEY)
      polarityScore = analyzer.polarity_scores(tweet)
      noError = 0
      for key in ATTRIBUTES: 
        entity[key] = perspectiveScore[key]
        if entity[key]: noError = 1
      cnts += noError
      for key, val in polarityScore.items():
        entity[key] = val 
      entity['tokens'] = tknzr.tokenize(tweet)
      entity['processed'] = True
      updateList.append(entity)
    scorer.client.put_multi(updateList)
    logging.info("{} out of {} tweets scored.".format(cnts, BATCHSIZE))
    
