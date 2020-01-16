import sys
import time
import logging
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json,requests

def word_tokenize(s):
    regex_str = [
        r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
        r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
        r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
        r"<email_address>", # email addresses 
        r'(?:[\w_]+)', # other words
        r'(?:\S)', # anything else
    ]
    tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
    ret = tokens_re.findall(s)    
    return ret #[r[0] for r in ret]

def call_perspective_api(text, attributes, PERSPECTIVE_KEY):
    backoff_counter = 1
    while True:
      path = ' https://commentanalyzer.googleapis.com/v1alpha1/comments:analyze?key=%s' % PERSPECTIVE_KEY
      request = {
          'comment' : {'text' : text},
          'requestedAttributes' : { c : {} for c in attributes},
          'doNotStore' : True, 
      }
      response = requests.post(path, json=request)
      prob = {}
      if response.status_code == 429:
         time.sleep(10 * backoff_counter)
         backoff_counter += 1
      else:
        break
    if response.status_code == 200:
      data = json.loads(response.text)
      scores_simplified = {}
      attribute_scores = data['attributeScores']
      for attr, data in attribute_scores.items():
          prob[attr] = data['summaryScore']['value']
      return prob
    else:
      logging.error("Status code: {}.".format(response.status_code))
      for attr in attributes:
        prob[attr] = None
      return prob 

def replace_pattern(text):
  regex_dict = {
      r'<[^>]+>' : r'', # HTML tags
      r'RT[ ]?(?:@)([\w_]+):': r'RT : ', # RT
      r'(http[s]?:)?[ ]?/[ ]?/((?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+[ ]?)+' : r'[url_link]', # URLs
      r"\\n": r"\n",
      r"\\t": r"\t",
  }

  for pattern, repl in regex_dict.items():
      text = re.sub(pattern, repl, text) 
  return text

def tweet_tokenize(s):
  regex_str = [
      r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
      r'(?:@)([\w_]+)', # @-mentions
      r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
      r"(?:\#+)([\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
      r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
      r'(?:[\w_]+)', # other words
      r'(?:\S)', # anything else
  ]
  
  tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
  ret = tokens_re.findall(s)    
  return [r[0] for r in ret]
 
def tweet_preprocess(s):
  tokens = tweet_tokenize(replace_pattern(s))
  return tokens

