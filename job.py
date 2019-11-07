from alert import send_scraping_report
from joblib import Parallel, delayed
from const import DIR, date_today
from datetime import datetime


from ticker import Ticker
import shutil
import sys, os
import pickle
import time

with open(f'{DIR}/data/tickers.pickle', 'rb') as file:
	ticker_dict = pickle.load(file)

if __name__ == '__main__':

	## Make storage directory
	os.mkdir(f'{DIR}/options_data/{date_today}')

	###############################
	### Collecting
	###############################
	
	ticker_list = ["SPY", "FDX", "PYPL", "CSCO", "AAPL", "GE", "UVXY"][:1]
	for ticker in ticker_list:
		
		try:
			
			print("Processing:", ticker)
			
			thread = Ticker(ticker)
			thread.start(); thread.join();
			
			time.sleep(30)
			print(ticker, "was collected successfully.")
		
		except Exception as e:
			
			print(ticker, "was not collected successfully.")
			print(e)

		print()


	###############################
	### Logging
	###############################

	collected_tickers = [file.split('_')[0] for file in os.listdir(f'{DIR}/options_data/{date_today}')]

	success = []
	failure = []

	for ticker in ticker_dict:
		if ticker in collected_tickers:
			success.append(ticker)
		else:
			failure.append(ticker)

	with open(f'{DIR}/options_data/{date_today}/successful_tickers.txt', 'w') as file:
		
		file.write(f"Ticker\tCompany Name\tFile Size\n")
		for ticker in success:

			size = os.stat(f'{DIR}/options_data/{date_today}/{ticker}_{date_today}.csv').st_size / 1000
			size = "%.2f kb" % size
			file.write(f"{ticker}\t{ticker_dict[ticker]}\t{size}\n")

	with open(f'{DIR}/options_data/{date_today}/failed_tickers.txt', 'w') as file:
		
		file.write(f"Ticker\tCompany Name\tFile Size\n")
		for ticker in failure:
			file.write(f"{ticker}\t{ticker_dict[ticker]}\n")

	shutil.make_archive(f"{DIR}/options_data/{date_today}", "zip", f"{DIR}/options_data/{date_today}")
	send_scraping_report(success, failure)