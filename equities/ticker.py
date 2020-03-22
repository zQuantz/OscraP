from const import date_today, named_date_fmt, DIR, CONVERTER, NUMBERS
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import itertools
import requests
import sys, os

headers_mobile = { 'User-Agent' : 'Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B137 Safari/601.1'}
OHLC = "https://finance.yahoo.com/quote/{ticker}/history?period1={yesterday}&period2={today}&interval=1d&filter=history&frequency=1d"
ANALYSIS = "https://ca.finance.yahoo.com/quote/{ticker}/analysis?p={ticker}"
STATS = "https://finance.yahoo.com/quote/{ticker}/key-statistics?p={ticker}"
START = "https://finance.yahoo.com/quote/{ticker}/options?p={ticker}"
SUMMARY = "https://finance.yahoo.com/quote/{ticker}/"
PARSER = "lxml"

class Ticker():

	def __init__(self, ticker, logger):

		self.logger = logger
		self.ticker = ticker
		
		self.options = []

		try:
			self.div = self.get_dividends()
			self.logger.info(f"{ticker},Dividend,Success,")
		except Exception as e:
			self.logger.warning(f"{ticker},Dividend,Failure,{e}")

		try:
			self.get_ohlc()
			self.logger.info(f"{ticker},OHLC,Success,")
		except Exception as e:
			self.logger.warning(f"{ticker},OHLC,Failure,{e}")
			if e.args[0] == "Fatal":
				raise Exception("Stale ticker. Data not up-to-date.")

		try:
			self.get_key_stats()
			self.logger.info(f"{ticker},Key Stats,Success,")
		except Exception as e:
			self.logger.warning(f"{ticker},Key Stats,Failure,{e}")

		try:
			self.get_analysis()
			self.logger.info(f"{ticker},Analysis,Success,")
		except Exception as e:
			self.logger.warning(f"{ticker},Analysis,Failure,{e}")

		try:
			self.get_options()
			self.logger.info(f"{ticker},Options,Success,")			
		except Exception as e:
			self.logger.warning(f"{ticker},Options,Failure,{e}")	

	def option_fmt(self, str_number, metric=''):
	
		if str_number == '':
			self.logger.info(f"{self.ticker},Value,'',{metric}")
			return 0

		if str_number == 'N/A':
			self.logger.info(f'{self.ticker},Value,N/A,{metric}')
			return 0

		for token in ',$%':
			str_number = str_number.replace(token, '')

		return float(str_number.replace('-', '0'))

	def feature_conversion(self, str_):

		str_ = str_.split()
		if str_[-1] in NUMBERS:
			str_ = str_[:-1]
		str_ = ' '.join(str_)
		
		if '(' in str_:
			modifier = str_[str_.find('(')+1:str_.rfind(')')]
			feature_name = str_.split('(')[0]
			return (feature_name.strip(), modifier)
		else:
			return (str_, '')

	def fmt(self, str_, metric=''):

		try:
			date = datetime.strptime(str_, "%b %d, %Y")
			return date.strftime("%Y-%m-%d")
		except Exception as e:
			pass

		default_value = '' if 'Date' in metric else '' if 'Factor' in metric else 0

		if ':' in str_:
			return str_
		
		if str_ == '' or str_ == 'N/A':
			self.logger.info(f'{self.ticker},Value,{str_},{metric}')
			return None
		
		str_ = str_.replace(',', '').replace('$', '')

		modifier = str_[-1]
		if modifier in NUMBERS:
			return np.round(float(str_), 5)
		
		if modifier == '%':
			return np.round(float(str_[:-1]) / 100, 5)
		
		if modifier in ['M', 'B', 'T']:
			return np.round(float(str_[:-1]) * CONVERTER[modifier], 5)

	def get_dividends(self):

		response = requests.get(SUMMARY.format(ticker = self.ticker), headers = headers_mobile)
		bs = BeautifulSoup(response.text, PARSER)

		table = bs.find_all("table")[1]
		div = table.find("td", {"data-test" : "DIVIDEND_AND_YIELD-value"})

		if not div:
			div = table.find("td", {"data-test" : "TD_YIELD-value"}).text
		else:
			div = div.text.split(' ')[1][1:-1]
			div = div.replace('N/A', '')

		return self.option_fmt(div, 'Dividend') / 100

	def get_ohlc(self):

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
		if ohlc_date != date_today:
			raise Exception("Fatal")

		cols = ['open', 'high', 'low', 'close', 'adj_close', 'stock_volume']
		prices = list(map(self.option_fmt, prices[1:], cols))

		prices += [self.div, date_today]
		cols += ["dividend_yield", 'date_current']

		df = pd.DataFrame([prices], columns = cols)
		df.to_csv(f"{DIR}/financial_data/{date_today}/equities/{self.ticker}_{date_today}.csv", index=False)

	def get_options(self):

		def append_options(table, expiry_date_fmt, expiration_days, symbol):

			for row in table.find_all("tr")[1:]:
				es = [e for e in row.find_all("td")[2:]]
				self.options.append([
						date_today,
						expiry_date_fmt,
						np.round(max(expiration_days / 252, 0), 6),
						symbol,
						self.option_fmt(es[0].text, 'Strike Price'),
						self.option_fmt(es[2].text, 'Bid'),
						self.option_fmt(es[3].text, 'Ask'),
						self.option_fmt(es[-2].text, 'Volume'),
						self.option_fmt(es[1].text, 'Option Price'),
						self.option_fmt(es[-1].text, 'Implied Volatility') / 100,
						self.option_fmt(es[-3].text, 'Open Interest')
					])

		start = START.format(ticker = self.ticker)
		bs = BeautifulSoup(requests.get(start, headers = headers_mobile).text, PARSER)
		self.expirations = [(option.get("value"), option.text) for option in bs.find_all("option")]

		for expiry, expiry_date in self.expirations:

			dt = datetime.fromtimestamp(int(expiry))
			expiry_date_fmt = datetime.strptime(expiry_date, named_date_fmt)
			expiration_days = np.busday_count(datetime.now().strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d"))

			page = start+f"&date={str(expiry)}"
			response = requests.get(page, headers = headers_mobile)
			bs = BeautifulSoup(response.text, PARSER)

			calls = bs.find("table", {"class" : "calls"})
			puts = bs.find("table", {"class" : "puts"})
			
			if calls:
				append_options(calls, expiry_date_fmt, expiration_days, 'C')
			
			if puts:
				append_options(puts, expiry_date_fmt, expiration_days, 'P')

		df = pd.DataFrame(self.options, columns = ['date_current', 'expiration_date', 'time_to_expiry', 'option_type',
												   'strike_price', 'bid', 'ask', 'volume', 'option_price', 'implied_volatility',
												   'open_interest'])
		df.to_csv(f"{DIR}/financial_data/{date_today}/options/{self.ticker}_{date_today}.csv", index=False)

	def get_key_stats(self):

		def get_tds(bs, text):

			span = bs.find("span", text=text)
			main_div = span.parent.parent
			return main_div.find_all("td")

		url = STATS.format(ticker = self.ticker)
		bs = requests.get(url, headers=headers_mobile).content
		bs = BeautifulSoup(bs, PARSER)

		tds = get_tds(bs, "Financial Highlights")
		tds.extend(get_tds(bs, "Trading Information"))
		tds.extend(get_tds(bs, "Valuation Measures"))

		key_stats = []
		for feature_name, feature in zip(tds[0::2], tds[1::2]):
			key = self.feature_conversion(feature_name.text)
			key_stats.append([
				*key,
				self.fmt(feature.text, metric = key[0])
			])

		df = pd.DataFrame(key_stats, columns = ["feature", "modifier", "value"])
		df.to_csv(f"{DIR}/financial_data/{date_today}/key_stats/{self.ticker}_{date_today}.csv", index=False)

	def get_analysis(self):

		def parse_table(table):

			trs = table.find_all("tr")
			header, rows = trs[0], trs[1:]

			header = header.find_all("th")
			table_name = header[0].text

			column_names = [name.text for name in header[1:]]

			data, row_names = [], []
			for row in rows:

				row = row.find_all("td")
				row_names.append(row[0].text)
				data.extend([ele.text for ele in row[1:]])	
				
			df = pd.DataFrame(list(itertools.product(*[row_names, column_names])))
			df.columns = ['feature', 'feature_two']

			df['category'] = table_name
			df['feature_two'], df['modifier'] = list(zip(*df.feature_two.apply(self.feature_conversion)))

			metrics = (df.category + ' ' + df.feature).values.tolist()
			df['value'] = [self.fmt(value, metric=metric) for value, metric in zip(data, metrics)]

			return df[['category', 'feature', 'feature_two', 'modifier', 'value']]

		url = ANALYSIS.format(ticker = self.ticker)
		bs = requests.get(url, headers=headers_mobile).content
		bs = BeautifulSoup(bs, PARSER)

		dfs = []
		tables = bs.find_all("table")
		for table in tables:
			dfs.append(parse_table(table))
		df = pd.concat(dfs)

		df.to_csv(f"{DIR}/financial_data/{date_today}/analysis/{self.ticker}_{date_today}.csv", index=False)
