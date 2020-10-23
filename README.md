This is the codebase for streaming voter fraud related discussions in 2020.

Setup the index in your datastore porject: https://cloud.google.com/datastore/docs/concepts/indexes#deploying_or_deleting_indexes

Use crontab -e to run cron jobs for stream_tweets and images_search:

Example:
```
*/15 * * * * /absolute/path/anaconda3/bin/python3 /absolute/path/streaming-candidates-2020/src/stream_tweets.py >> /absolute/path/cron_logs/stream_tweets.log 2>&1
*/6 * * * *  /absolute/path/anaconda3/bin/python3 /absolute/path/streaming-candidates-2020/src/images_search.py >> /absolute/path/cron_logs/images_search.log 2>&1
```

To run a cronjob now, you can use [cronitor-cli](https://cronitor.io/docs/using-cronitor-cli):
```
cronitor select
```

To see previously ran cron jobs:
```
grep CRON /var/log/syslog
```