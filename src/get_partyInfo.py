import pandas as pd
import json

with open('../data/candidates-twitter-2020.csv') as r:
  candidatesDf = pd.read_csv(r)
with open('../data/candidates') as r:
  twitter = json.load(r)

party = {}
for _, line in candidatesDf.iterrows():
  if line['party'] in ['Democratic', 'Republican']: 
    party[line['handle']] = line['party']

ret = {}
for user in twitter:
  if user['handle'] in party:
    ret[str(user['id'])] = party[user['handle']]
print(len(ret))

with open("../data/partyInfo", "w") as w:
  json.dump(ret, w)
 
