from const import DIR
import pandas as pd
import pickle

if __name__ == '__main__':

	tickers = {}

	with open(f'{DIR}/data/NYSE.txt', 'r') as file:
		for line in file:
			line = line.split('\t')
			if '-' in line[0] or '.' in line[0]:
				continue
			tickers[line[0]] = line[1][:-1]

	with open(f'{DIR}/data/NASDAQ.txt', 'r') as file:
		for line in file:
			line = line.split('\t')
			if '-' in line[0] or '.' in line[0]:
				continue
			tickers[line[0]] = line[1][:-1]

	df = pd.read_csv(f'{DIR}/data/most_traded_etfs.csv')

	for row in df.values:
		tickers[row[0]] = row[1]

	tickers_to_scrape = {}

	with open(f'{DIR}/data/merged_ticker_list.txt', 'r') as file:
		for line in file:
			ticker = line.replace('\n', '')
			try:
				tickers_to_scrape[ticker] = tickers[ticker]
			except:
				tickers_to_scrape[ticker] = 'No Description.'

	for row in df.values[:100]:
		tickers_to_scrape[row[0]] = row[1]

	with open(f'{DIR}/data/tickers.pickle', 'wb') as file:
		pickle.dump(tickers_to_scrape, file)