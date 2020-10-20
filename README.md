This is the codebase for streaming voter fraud related discussions in 2020.

Setup the index in your datastore porject: https://cloud.google.com/datastore/docs/concepts/indexes#deploying_or_deleting_indexes

Use crontab -e to run cron jobs for stream_tweets and images_search:

Example:

*/15 * * * * anaconda3/bin/python3 /home/s_tech_cornell/stream_tweets.py
0 */6 * * * anaconda3/bin/python3 /home/s_tech_cornell/images_search.py
