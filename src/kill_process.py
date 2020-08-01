import os
import json

try:
  with open("/home/yiqing/candidates-on-twitter/data/pid_dem", "r") as r:
    pid = json.load(r)
  os.system("kill {}".format(pid))
except:
  pass


try:
  with open("/home/yiqing/candidates-on-twitter/data/pid_rep", "r") as r:
    pid = json.load(r)
  os.system("kill {}".format(pid))
except:
  pass
