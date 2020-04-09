from const import DIR, logger, date_today
from joblib import delayed, Parallel
from feeds import Feeds
import pandas as pd
import sys, os
import joblib

feeds = pd.read_csv(f"{DIR}/data/rss.csv").iloc[:, :2]
feeds.columns = ['source', 'feed']

feed_threads = {}

with open(f'{DIR}/data/groups.pkl', 'rb') as file:
	groups = joblib.load(file)

def on_close():

	for group in groups:
		print("Closing Group:", group)
		feed_threads[group].on_close()

if __name__ == '__main__':

	for group in groups:
		group, sleep = group, groups[group]
		group_coords = feeds[feeds.source.isin(group)]
		feed_threads[group] = Feeds(
			sources = group_coords.source.values,
			feeds = group_coords.feed.values,
			sleep = sleep,
			logger = logger
		)
		feed_threads[group].start()

