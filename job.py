from const import n_jobs, n_threads, DIR, date_today
from alert import send_scraping_report
from joblib import Parallel, delayed
from datetime import datetime
from ticker import Ticker
import sys, os
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
			break

if __name__ == '__main__':

	try:
		os.unlink(f'{DIR}/option_data/{date_today}')
	except Exception as e:
		pass

	## Make storage directory
	os.mkdir(f'{DIR}/option_data/{date_today}')

	cs = int(len(ticker_dict) / n_jobs)
	ticker_list = list(ticker_dict.keys())
	chunks = [ticker_list[i - cs : i] for i in range(cs, len(ticker_list) + cs, cs)]

	Parallel(n_jobs = n_jobs)(delayed(run)(i, chunk) for i, chunk in enumerate(chunks))
	print("Here")

	collected_tickers = [file.split('_')[0] for file in os.listdir(f'{DIR}/option_data/{date_today}')]

	success = []
	failure = []

	for ticker in ticker_dict:
		if ticker in collected_tickers:
			success.append(ticker)
		else:
			failure.append(ticker)

	with open(f'{DIR}/option_data/{date_today}/successful_tickers.txt', 'w') as file:
		file.write(f"Ticker\tCompany Name\tFile Size\n")
		for ticker in success:
			size = os.stat(f'{DIR}/option_data/{date_today}/{ticker}_{date_today}.csv').st_size / 1000
			size = "%.2f kb" % size
			file.write(f"{ticker}\t{ticker_dict[ticker]}\t{size}\n")

	with open(f'{DIR}/option_data/{date_today}/failed_tickers.txt', 'w') as file:
		file.write(f"Ticker\tCompany Name\tFile Size\n")
		for ticker in failure:
			file.write(f"{ticker}\t{ticker_dict[ticker]}\n")

	send_scraping_report(success, failure)
