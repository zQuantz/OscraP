from const import date_today, named_date_fmt, DIR
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from threading import Thread
from utils import *
import pandas as pd
import numpy as np
import requests
import json, re
import sys, os
import joblib

headers_mobile = { 'User-Agent' : 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B137 Safari/601.1'}
START = "https://finance.yahoo.com/quote/{ticker}/options?p={ticker}"
SUMMARY = "https://finance.yahoo.com/quote/{ticker}/"
OHLC = "https://finance.yahoo.com/quote/{ticker}/history?period1={yesterday}&period2={today}&interval=1d&filter=history&frequency=1d"
PARSER = "lxml"

def fmt(str_number):
	if str_number == '':
		return 0
	return float(str_number.replace(',', '').replace('$', '').replace('%', '').replace('-', '0'))

class Ticker(Thread):

	def __init__(self, ticker):

		Thread.__init__(self)
		self.stats = []
		self.ticker = ticker
		self.START = START.format(ticker = ticker)
		self.pages = [BeautifulSoup(requests.get(self.START, headers = headers_mobile).text, PARSER)]

	def get_dividends(self, bs):

		### STOCK
		table = bs.find("table", attrs={"class" : "W(100%) M(0) Bdcl(c)"})
		rows = table.find_all("td")

		for i in range(len(rows)):

		    if 'Dividend' in rows[i].text:

		    	div = rows[i+1].text

		    	if 'N/A' in div:
		    		div = 0
		    		break

		    	div = fmt(div.split('(')[1][:-1])
		    	break

		## ETF
		for i in range(len(rows)):
		    if rows[i].text == 'Yield':
		    	div = fmt(rows[i+1].text)
		    	break

		print("Div", div)

		if div is None:
			return 0
		else:
			return div / 100

	def initialize(self):

		## Dividends
		response = requests.get(SUMMARY.format(ticker = self.ticker), headers = headers_mobile)
		bs = BeautifulSoup(response.text, PARSER)
		self.div = self.get_dividends(bs)

		## OHLC
		today = datetime.now()
		yesterday = today - timedelta(days=1)

		today = int(today.timestamp())
		yesterday = int(yesterday.timestamp())

		bs = BeautifulSoup(requests.get(OHLC.format(ticker = self.ticker, yesterday = yesterday, today = today),
						   headers = headers_mobile).content, PARSER)
		prices = bs.find("tr", {"class" : "BdT Bdc($seperatorColor) Ta(end) Fz(s) Whs(nw)"}).find_all("td")
		prices = [price.text for price in prices]
		self.current_price = fmt(prices[-2])
		
		ohlc_date = datetime.strptime(prices[0], "%b %d, %Y").strftime("%Y-%m-%d")
		assert ohlc_date == date_today

		self.open = fmt(prices[1])
		self.high = fmt(prices[2])
		self.low = fmt(prices[3])
		self.close = fmt(prices[4])
		self.adj_close = fmt(prices[5])
		self.volume = fmt(prices[6])

		## Expirations
		bs = self.pages[0]
		self.expiry_in_unix, self.expiry_as_string = list(zip(*[(option.get("value"), option.text) for option in bs.find_all("option")]))

	def scrape_options(self):

		for expiry, expiry_date in zip(self.expiry_in_unix, self.expiry_as_string):

			dt = datetime.fromtimestamp(int(expiry))
			expiry_date_fmt = datetime.strptime(expiry_date, named_date_fmt)
			expiration_days = np.busday_count(datetime.now().strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d"))

			page = self.START+f"&date={str(expiry)}"
			response = requests.get(page, headers = headers_mobile)
			bs = BeautifulSoup(response.text, PARSER)

			calls = bs.find("table", {"class" : "calls"})
			puts = bs.find("table", {"class" : "puts"})
			
			if calls:

				call_options = []
				for row in calls.find_all("tr")[1:]:
					es = [e for e in row.find_all("td")[2:]]
					self.stats.append([
							date_today,
							self.open,
							self.high,
							self.low,
							self.close,
							self.adj_close,
							self.volume,
							fmt(es[0].text),
							fmt(es[1].text),
							self.div,
							expiry_date_fmt,
							np.round(max(expiration_days / 252, 0), 6),
							'C',
							fmt(es[-1].text),
							fmt(es[2].text),
							fmt(es[3].text),
							fmt(es[-2].text),
							fmt(es[-3].text),

						])
			if puts:
			
				put_options = []
				for row in puts.find_all("tr")[1:]:
					es = [e for e in row.find_all("td")[2:]]
					self.stats.append([
							date_today,
							self.open,
							self.high,
							self.low,
							self.close,
							self.adj_close,
							self.volume,
							fmt(es[0].text),
							fmt(es[1].text),
							self.div,
							expiry_date_fmt,
							np.round(max(expiration_days / 252, 0), 6),
							'P',
							fmt(es[-1].text),
							fmt(es[2].text),
							fmt(es[3].text),
							fmt(es[-2].text),
							fmt(es[-3].text),

						])

		df = pd.DataFrame(self.stats, columns = ['CurrentDate', 'Open', 'High', 'Low', 'Close', 'AdjClose', 'StockVolume', 
												 'StrikePrice', 'OptionPrice', 'DividendYield', 'ExpirationDate', 
												 'TimeToExpiry', 'OptionType', 'ImpliedVolatility', 'Bid',
											     'Ask', 'Volume', 'OpenInterest'])

		df = format_option_chain(df)
		df.to_csv(f"{DIR}/options_data/{date_today}/{self.ticker}_{date_today}.csv", index=False)

	def run(self):

		self.initialize()
		self.scrape_options()
