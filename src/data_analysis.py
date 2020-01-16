import json
from collections import defaultdict

USERIDS = "../data/userids"
DATAFILE = "../data/output_sample.json"

with open(USERIDS, "r") as r:
  userIDs = json.load(r) 

# dedeup
tweetIDs = {}
with open(DATAFILE, "r") as r:
  with open(DATAFILE + 'dedup', "w") as w:
    for line in r:
      data = json.loads(line)
      if not(tweetIDs[data['id']]):
         w.write(line)
      tweetIDs[data['id']] = True

userStats = defaultdict(int)
with open(DATAFILE + 'dedup', "r") as r:
  for line in r:
    data = json.loads(line)
    poster = data['user']['id_str']
    replyTo = data['in_reply_to_user_id_str'] if 'in_reply_to_user_id_str' in data else None
    retweetFrom = data['retweeted_status']['user']['id_str'] if 'retweeted_status' in data else None 
    mentions = [user_mention['id_str'] for user_mention in data['entities']['user_mentions']]
    cnts = 0
    if poster in userIDs:
       userStatus[poster]['posted'] += 1 
       cnts += 1
    if retweetFrom in userIDs:
    # Not excluding self-retweets
       userStatus[retweetFrom]['retweeted'] += 1
       cnts += 1
    for userID in userIDs:
       if userID in mentions: 
         userStatus[userID]['mentioned'] += 1
         cnts += 1
    if replyTo in userIDs:
       userStatus[replyTo]['replied'] += 1
       cnts += 1
print(userStatus) 
