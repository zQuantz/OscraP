from const import DIR
import pickle

if __name__ == '__main__':

	tickers = {}

	with open('data/NYSE.txt', 'r') as file:
		for line in file:
			line = line.split('\t')
			if '-' in line[0] or '.' in line[0]:
				continue
			tickers[line[0]] = line[1][:-1]

	with open('data/NASDAQ.txt', 'r') as file:
		for line in file:
			line = line.split('\t')
			if '-' in line[0] or '.' in line[0]:
				continue
			tickers[line[0]] = line[1][:-1]

	with open(f'{DIR}/data/tickers.pickle', 'wb') as file:
		pickle.dump(tickers, file)
