import os
import json

try:
  with open("/home/yiqing/candidates-on-twitter/data/pid_dem", "r") as r:
    pid = json.load(r)
  os.system("sudo kill {}".format(pid))
  os.system("rm /tmp/-home-yiqing-candidates-on-twitter-src-stream_top_retweeters_dem-.lock")
  os.system("rm /home/yiqing/candidates-on-twitter/data/pid_dem")
except:
  pass


try:
  with open("/home/yiqing/candidates-on-twitter/data/pid_rep", "r") as r:
    pid = json.load(r)
  os.system("sudo kill {}".format(pid))
  os.system("rm /tmp/-home-yiqing-candidates-on-twitter-src-stream_top_retweeters_rep-.lock")
  os.system("rm /home/yiqing/candidates-on-twitter/data/pid_rep")
except:
  pass
