from smtp_excepthook import ExceptHook
import sys
import json

EMAIL = "../private/email"
LOGS_DIR = "../logs/"
with open(EMAIL, "r") as f:
  email = json.load(f)

if __name__ == "__main__":
  sys.excepthook = ExceptHook(email).ExceptHook()
  1 / 0
