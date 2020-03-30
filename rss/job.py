from const import DIR, logger, date_today
from joblib import delayed, Parallel
from source import Source
from feed import Feed
import pandas as pd
import sys, os

df = pd.read_csv(f'{DIR}/data/rss.csv')
df.columns = ['source', 'feed']

sources = df.groupby('source').apply(lambda x: 
				x.feed.values.tolist()
			).to_dict()
source_objs = {}

def init_folders():

	os.mkdir(f"{DIR}/news_data/{date_today}")

def on_start(source, k):
	print(f"Job #{k}")
	source_objs[source] = Source(source, sources[source])

def on_close():

	for source in sources:
		print("Closing", source)
		source_objs[source].on_close()

if __name__ == '__main__':

	# init_folders()
	Parallel(n_jobs=3)(delayed(on_start)(source, i) for i, source in enumerate(sources))