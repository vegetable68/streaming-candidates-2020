import json

def federal_candidates(states):
  STATE = None
  POSITION = None
  PARTY = None
  candidates = []
  DISTRICT= None
  with open("../data/candidates_html", "r") as f:
    for line in f:
      if line[:-1] in states:
        # From this line, it's the candidates from this state
        STATE = line[:-1] 
        POSITION = None
        PARTY = None
        DISTRICT = None
      if "id=\"U.S._House" in line: 
         POSITION = "House"
         PARTY = None
         DISTRICT = None
      if "id=\"U.S._Senate" in line:
         POSITION = "Senate"
         PARTY = None
         DISTRICT = None
      if "Republican" in line:
         PARTY = "Republican"
         DISTRICT = None
      if "Democratic" in line:
         PARTY = "Democratic"
         DISTRICT = None
      if "Third Party" in line:
         PARTY = "Third Party"
         DISTRICT = None
      if "href" in line:
         CANDIDATE = line[line.find(">")+1:-1]
         if not(line.find("district=") == -1):
            district = line[:line.find(" ")]
            DISTRICT = district[district.find("district=")+9:]
         CANDIDATE_URL = line[line.find("href=")+6:line.find("\" title")]
         print(PARTY, POSITION, STATE, CANDIDATE, CANDIDATE_URL, DISTRICT)
         assert (PARTY and POSITION  and STATE and CANDIDATE and CANDIDATE_URL)
         candidates.append({"candidate_name": CANDIDATE,
                            "ballotpedia_url": CANDIDATE_URL, 
                            "district" : DISTRICT,
                            "party": PARTY,
                            "position": POSITION,
                            "state": STATE})
  with open("../data/candidates", "w") as w:
    for candidate in candidates:
      w.write(json.dumps(candidate) + '\n')

def gubernatorial_candidates(states):
  candidates = []
  for PARTY in ['Republican', 'Democratic']: 
    STATE = None
    URL = None
    CANDIDATE = None
    with open("../data/govornor_{}_html".format(PARTY), "r") as f:
      for ind, line in enumerate(f):
        if ind % 3 == 0:
          STATE = line[:-1].replace(" ", "_")
          assert(STATE in states)
          URL = None
          CANDIDATE = None
        if ind % 3 == 1:
          assert('https' in line[:-1])
          URL = line[:-1]
          CANDIDATE = None
        if ind % 3 == 2:
          CANDIDATE = line[:-1]
          assert(STATE and URL)
          candidates.append({"candidate_name": CANDIDATE,
                             "ballotpedia_url": URL, 
                             "party": PARTY,
                             "position": "Governor",
                             "state": STATE})
  with open("../data/gubernatorial_candidates.json", "w") as w:
    for candidate in candidates:
      w.write(json.dumps(candidate) + '\n')

 
states = []
with open("../data/states", "r") as f:
  for line in f:
    states.append(line[:-1]) 
gubernatorial_candidates(states)
federal_candidates(states)

