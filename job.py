from const import n_jobs, n_threads, DIR
from joblib import Parallel, delayed
from datetime import datetime
from ticker import Ticker
import pickle

with open(f'{DIR}/data/tickers.pickle', 'rb') as file:
	ticker_dict = pickle.load(file)

def run(i, tickers):

	threads = []

	for ticker in tickers:

		threads.append(Ticker(ticker).start())

		if len(threads) >= n_threads:

			print("Job #{} ... joining.".format(i))
			[thread.join() for thread in threads if thread is not None]
			threads = []

if __name__ == '__main__':

	cs = int(len(ticker_dict) / n_jobs)
	ticker_list = list(ticker_dict.keys())
	chunks = [ticker_list[i - cs : i] for i in range(cs, len(ticker_list) + cs, cs)]

	Parallel(n_jobs = n_jobs)(delayed(run)(i, chunk) for i, chunk in enumerate(chunks))
