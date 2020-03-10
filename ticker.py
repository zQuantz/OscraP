from const import date_today, named_date_fmt, DIR
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import sys, os

headers_mobile = { 'User-Agent' : 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B137 Safari/601.1'}
OHLC = "https://finance.yahoo.com/quote/{ticker}/history?period1={yesterday}&period2={today}&interval=1d&filter=history&frequency=1d"
START = "https://finance.yahoo.com/quote/{ticker}/options?p={ticker}"
SUMMARY = "https://finance.yahoo.com/quote/{ticker}/"
PARSER = "lxml"

class Ticker():

	def __init__(self, ticker, logger):

		self.logger = logger
		self.ticker = ticker
		
		self.stats = []
		self.START = START.format(ticker = ticker)
		self.pages = [BeautifulSoup(requests.get(self.START, headers = headers_mobile).text, PARSER)]

		self.initialize()
		self.scrape_options()

	def fmt(self, str_number, metric=''):
	
		if str_number == '':
			return 0

		if str_number == 'N/A':
			self.logger.info(f'N/A - {self.ticker}, {metric}')
			return 0

		for token in ',$%':
			str_number = str_number.replace(token, '')

		return float(str_number.replace('-', '0'))

	def get_dividends(self, bs):

		table = bs.find_all("table")[1]
		div = table.find("td", {"data-test" : "DIVIDEND_AND_YIELD-value"})

		if not div:
			div = table.find("td", {"data-test" : "TD_YIELD-value"}).text
		else:
			div = div.text.split(' ')[1][1:-1]
			div = div.replace('N/A', '')

		return self.fmt(div, 'div') / 100

	def initialize(self):

		response = requests.get(SUMMARY.format(ticker = self.ticker), headers = headers_mobile)
		bs = BeautifulSoup(response.text, PARSER)
		self.div = self.get_dividends(bs)

		today = datetime.now()
		yesterday = today - timedelta(days=1)

		today = int(today.timestamp())
		yesterday = int(yesterday.timestamp())

		ohlc = OHLC.format(ticker = self.ticker, yesterday = yesterday, today = today)
		bs = BeautifulSoup(requests.get(ohlc, headers = headers_mobile).content, PARSER)

		prices = bs.find("table", {"data-test" : "historical-prices"})
		prices = prices.find_all("tr")[1]
		prices = [price.text for price in prices]

		ohlc_date = datetime.strptime(prices[0], "%b %d, %Y").strftime("%Y-%m-%d")
		assert ohlc_date == date_today

		prices = list(map(self.fmt, prices[1:], ['Open', 'High', 'Low', 'Close', 'AdjClose', 'Volume']))
		self.open = prices[0]
		self.high = prices[1]
		self.low = prices[2]
		self.close = prices[3]
		self.adj_close = prices[4]
		self.volume = prices[5]

		bs = self.pages[0]
		self.expirations = [(option.get("value"), option.text) for option in bs.find_all("option")]

	def append_options(self, table, expiry_date_fmt, expiration_days, symbol):

		for row in table.find_all("tr")[1:]:
			es = [e for e in row.find_all("td")[2:]]
			self.stats.append([
					date_today,
					self.open,
					self.high,
					self.low,
					self.close,
					self.adj_close,
					self.volume,
					self.fmt(es[0].text, 'Strike Price'),
					self.fmt(es[1].text, 'Option Price'),
					self.div,
					expiry_date_fmt,
					np.round(max(expiration_days / 252, 0), 6),
					symbol,
					self.fmt(es[-1].text, 'Implied Volatility') / 100,
					self.fmt(es[2].text, 'Bid'),
					self.fmt(es[3].text, 'Ask'),
					self.fmt(es[-2].text, 'Volume'),
					self.fmt(es[-3].text, 'Open Interest')
				])

	def scrape_options(self):

		for expiry, expiry_date in self.expirations:

			dt = datetime.fromtimestamp(int(expiry))
			expiry_date_fmt = datetime.strptime(expiry_date, named_date_fmt)
			expiration_days = np.busday_count(datetime.now().strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d"))

			page = self.START+f"&date={str(expiry)}"
			response = requests.get(page, headers = headers_mobile)
			bs = BeautifulSoup(response.text, PARSER)

			calls = bs.find("table", {"class" : "calls"})
			puts = bs.find("table", {"class" : "puts"})
			
			if calls:
				self.append_options(calls, expiry_date_fmt, expiration_days, 'C')
			
			if puts:
				self.append_options(puts, expiry_date_fmt, expiration_days, 'P')

		df = pd.DataFrame(self.stats, columns = ['CurrentDate', 'Open', 'High', 'Low', 'Close', 'AdjClose', 'StockVolume', 
												 'StrikePrice', 'OptionPrice', 'DividendYield', 'ExpirationDate', 
												 'TimeToExpiry', 'OptionType', 'ImpliedVolatility', 'Bid',
											     'Ask', 'Volume', 'OpenInterest'])

		df.to_csv(f"{DIR}/options_data/{date_today}/{self.ticker}_{date_today}.csv", index=False)