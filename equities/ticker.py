from const import DIR, DATE, DATA, CONVERTER, NUMBERS, CONFIG

from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import itertools
import sys, os
import time

sys.path.append(f"{DIR}/../utils")
from request import request

###################################################################################################

ANALYSIS = "https://ca.finance.yahoo.com/quote/{ticker}/analysis?p={ticker}"
STATS = "https://finance.yahoo.com/quote/{ticker}/key-statistics?p={ticker}"
OPTIONS = "https://finance.yahoo.com/quote/{ticker}/options?p={ticker}"
OHLC = "https://finance.yahoo.com/quote/{ticker}/history"
SUMMARY = "https://finance.yahoo.com/quote/{ticker}/"
PARSER = "lxml"

NAMED_DATE_FMT = "%B %d, %Y"

OPTION_COLS = [
	"date_current",
	"ticker",
	"expiration_date",
	"days_to_expiry",
	"option_type",
	"strike_price",
	"bid_price",
	"option_price",
	"ask_price",
	"implied_volatility",
	"volume",
	"open_interest"
]

OHLC_COLS = [
	"date_current",
	"ticker",
	"open_price",
	"high_price",
	"low_price",
	"close_price",
	"adjclose_price",
	"volume",
	"dividend_yield",
]

###################################################################################################

class Ticker():

	def __init__(self, ticker, logger, batch_id, retries=None, fault_dict=None):

		self.ticker = ticker
		self.logger = logger
		self.batch_id = batch_id

		self.retries = retries
		self.fault_dict = fault_dict

		if not retries or retries["ohlc"] or retries['options']:
			try:
				self.div = self.get_dividends()
				self.logger.info(f"{ticker},{batch_id},Dividend,Success,")
			except Exception as e:
				self.logger.warning(f"{ticker},{batch_id},Dividend,Failure,{e}")
			self.sleep()

			try:
				self.get_ohlc()
				self.logger.info(f"{ticker},{batch_id},OHLC,Success,")
			except Exception as e:
				self.logger.warning(f"{ticker},{batch_id},OHLC,Failure,{e}")
				if e.args[0] == "Fatal":
					raise Exception("Stale ticker. Data not up-to-date.")
			self.sleep()

		if not retries or retries['keystats']:
			try:
				self.get_keystats()
				self.logger.info(f"{ticker},{batch_id},Key Stats,Success,")
			except Exception as e:
				self.logger.warning(f"{ticker},{batch_id},Key Stats,Failure,{e}")
			self.sleep()

		if not retries or retries['analysis']:
			try:
				self.get_analysis()
				self.logger.info(f"{ticker},{batch_id},Analysis,Success,")
			except Exception as e:
				self.logger.warning(f"{ticker},{batch_id},Analysis,Failure,{e}")
			self.sleep()

		if not retries or retries['options']:
			try:
				self.options = []
				self.get_options()
				self.logger.info(f"{ticker},{batch_id},Options,Success,")	
			except Exception as e:
				self.logger.warning(f"{ticker},{batch_id},Options,Failure,{e}")			
			self.sleep()

	def sleep(self):

		time.sleep(0.5)

	def fmt(self, str_, metric=''):

		try:
			date = datetime.strptime(str_, "%b %d, %Y")
			return date.strftime("%Y-%m-%d")
		except Exception as e:
			pass

		default_value = '' if 'Date' in metric else '' if 'Factor' in metric else 0

		if ':' in str_:
			return str_
		
		if str_ in ['', 'N/A', '∞']:
			return None
		
		str_ = str_.replace(',', '').replace('$', '')

		modifier = str_[-1]
		if modifier in NUMBERS:
			return np.round(float(str_), 5)
		
		if modifier == '%':
			return np.round(float(str_[:-1]) / 100, 5)
		
		if modifier in ['M', 'B', 'T']:
			return np.round(float(str_[:-1]) * CONVERTER[modifier], 5)

	def option_fmt(self, str_number, metric=''):
	
		if str_number == '':
			return 0

		if str_number == 'N/A':
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

	def drop_by_na(self, pkey, df, key='value'):

		def select_nna(x): 
			nn = x.value.notnull()
			if nn.sum() > 0:
				return x.loc[nn, key].values[0]
			else:
				return x[key].values[0]

		df = df.groupby(pkey).apply(lambda x: select_nna(x))
		return df.rename(key).reset_index(drop=False)

	def get_dividends(self):

		url = SUMMARY.format(ticker = self.ticker)
		bs = BeautifulSoup(request(CONFIG, url, self.logger).content, PARSER)

		table = bs.find_all("table")[1]
		div = table.find("td", {"data-test" : "DIVIDEND_AND_YIELD-value"})

		if not div:
			div = table.find("td", {"data-test" : "TD_YIELD-value"}).text
		else:
			div = div.text.split(' ')[1][1:-1]
			div = div.replace('N/A', '')

		return self.option_fmt(div, 'Dividend')

	def get_ohlc(self):

		url = OHLC.format(ticker = self.ticker)
		bs = BeautifulSoup(request(CONFIG, url, self.logger).content, PARSER)

		prices = bs.find("table", {"data-test" : "historical-prices"})
		prices = prices.find_all("tr")[1]
		prices = [price.text for price in prices]

		ohlc_date = datetime.strptime(prices[0], "%b %d, %Y").strftime("%Y-%m-%d")
		if ohlc_date != DATE:
			raise Exception(f'Fatal')

		prices = list(map(self.option_fmt, prices[1:], OHLC_COLS[:-2]))
		prices = [DATE, self.ticker] + prices + [self.div]
		self.adj_close = prices[-3]

		ohlc = pd.DataFrame([prices], columns = OHLC_COLS)
		ohlc.to_csv(f"{DATA}/ohlc/{self.ticker}_{DATE}.csv", index=False)

		if self.retries and self.retries['ohlc']:

			self.fault_dict['ohlc']['new_status'] = 1
			self.logger.info(f"{self.ticker},{self.batch_id},Re-OHLC,Success,1")

	def get_options(self):

		def get_page(url):

			ctr, max_ctr = 0, 3
			while (ctr < max_ctr):	
				
				bs = BeautifulSoup(request(CONFIG, url, self.logger).content, PARSER)
				options = bs.find_all("option")

				if len(options) != 0:
					break

				ctr += 1
				self.logger.info(f"{self.ticker},{self.batch_id},Option Download,{ctr}")
				self.sleep()

			return bs, options

		def append_options(table, expiry_date_fmt, expiration_days, option_type):

			for row in table.find_all("tr")[1:]:
				es = [e for e in row.find_all("td")[2:]]
				self.options.append([
						DATE,
						self.ticker,
						expiry_date_fmt,
						expiration_days,
						option_type,
						self.option_fmt(es[0].text, 'Strike Price'),
						self.option_fmt(es[2].text, 'Bid'),
						self.option_fmt(es[1].text, 'Option Price'),
						self.option_fmt(es[3].text, 'Ask'),
						self.option_fmt(es[-1].text, 'Implied Volatility'),
						self.option_fmt(es[-2].text, 'Volume'),
						self.option_fmt(es[-3].text, 'Open Interest')
					])

		url = OPTIONS.format(ticker = self.ticker)
		bs, options = get_page(url)

		for option in options:

			self.sleep()

			expiry, expiry_date = option.get("value"), option.text
			self.logger.info(f"{self.ticker},{self.batch_id},Option Expiry,{expiry},{expiry_date.replace(',', '.')}")

			expiry_date = datetime.strptime(expiry_date, NAMED_DATE_FMT)
			expiry_date_fmt = expiry_date.strftime("%Y-%m-%d")
			expiration_days = (expiry_date - DTDATE).days + 1

			page = url+f"&date={str(expiry)}"
			bs, _ = get_page(page)

			calls = bs.find("table", {"class" : "calls"})
			puts = bs.find("table", {"class" : "puts"})
			
			if calls:
				append_options(calls, expiry_date_fmt, expiration_days, 'C')
			
			if puts:
				append_options(puts, expiry_date_fmt, expiration_days, 'P')

		df = pd.DataFrame(self.options, columns = OPTION_COLS)
		oid = df.ticker + ' ' + df.expiration_date + ' ' + df.option_type
		sp = df.strike_price.round(2).astype(str)
		sp = sp.str.rstrip("0").str.rstrip(".")
		df['option_id'] = oid + sp

		if not self.retries and len(df) > 0:
			
			df.to_csv(f"{DATA}/options/{self.ticker}_{DATE}.csv", index=False)

		elif len(df) != 0:
			
			try:
				old = pd.read_csv(f"{DATA}/options/{self.ticker}_{DATE}.csv")
			except Exception as e:
				old = pd.DataFrame()

			df = pd.concat([old, df]).reset_index(drop=True)
			df = df.drop_duplicates(subset=['expiration_date', 'strike_price', 'option_type'], keep="last")
			df = df.sort_values(['expiration_date', 'option_type', 'strike_price'])
			df.to_csv(f"{DATA}/options/{self.ticker}_{DATE}.csv", index=False)

			self.fault_dict['options']['new'] = len(df)
			delta = self.fault_dict['options']['new'] - self.fault_dict['options']['old']

			self.fault_dict['options']['delta'] = delta
			self.logger.info(f"{self.ticker},{self.batch_id},Re-Options,Success,{delta}")

		else:

			self.logger.info(f"{self.ticker},{self.batch_id},Options,None Collected,")

	def get_keystats(self):

		def get_items(bs, text):

			span = bs.find("span", text=text)
			main_div = span.parent.parent

			items = []
			for tr in main_div.find_all("tr"):
				
				tds = tr.find_all("td")
				if len(tds) == 0:
					continue

				items.append([tds[0].text, tds[1].text])

			return items

		url = STATS.format(ticker = self.ticker)
		bs = request(CONFIG, url, self.logger).content
		bs = BeautifulSoup(bs, PARSER)

		items = get_items(bs, "Financial Highlights")
		items.extend(get_items(bs, "Trading Information"))
		items.extend(get_items(bs, "Valuation Measures"))

		keystats = []
		for feature_name, feature in items:
			key = self.feature_conversion(feature_name)
			keystats.append([
				*key,
				self.fmt(feature, metric = key[0])
			])

		df = pd.DataFrame(keystats, columns = ["feature", "modifier", "value"])
		df = df.dropna(subset=["value"])

		pkey = ["feature", "modifier"]
		df.loc[:, pkey] = df[pkey].fillna('')

		df['ticker'] = self.ticker
		df['date_current'] = DATE

		if not self.retries and len(df) > 0:
			
			df.to_csv(f"{DATA}/keystats/{self.ticker}_{DATE}.csv", index=False)

		elif len(df) != 0:
			
			try:
				old = pd.read_csv(f"{DATA}/keystats/{self.ticker}_{DATE}.csv")
			except Exception as e:
				old = pd.DataFrame()

			df = pd.concat([old, df]).reset_index(drop=True)
			df = self.drop_by_na(pkey, df)
			df.to_csv(f"{DATA}/keystats/{self.ticker}_{DATE}.csv", index=False)

			self.fault_dict['keystats']['new'] = len(df)
			delta = self.fault_dict['keystats']['new'] - self.fault_dict['keystats']['old']
			
			self.fault_dict['keystats']['delta'] = delta
			self.logger.info(f"{self.ticker},{self.batch_id},Re-Key Stats,Success,{delta}")

		else:

			self.logger.info(f"{self.ticker},{self.batch_id},Key Stats,None Collected,")

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
		bs = request(CONFIG, url, self.logger).content
		bs = BeautifulSoup(bs, PARSER)

		dfs = []
		tables = bs.find_all("table")
		for table in tables:
			dfs.append(parse_table(table))
		
		df = pd.concat(dfs)
		df = df.dropna(subset=["value"])
		
		pkey = ["category", "feature", "feature_two", "modifier"]
		df.loc[:, pkey] = df[pkey].fillna('')

		df['ticker'] = self.ticker
		df['date_current'] = DATE

		if not self.retries and len(df) > 0:
			
			df.to_csv(f"{DATA}/analysis/{self.ticker}_{DATE}.csv", index=False)

		elif len(df) != 0:

			try:
				old = pd.read_csv(f"{DATA}/analysis/{self.ticker}_{DATE}.csv")
			except Exception as e:
				old = pd.DataFrame()

			df = pd.concat([old, df]).reset_index(drop=True)
			df = self.drop_by_na(pkey, df)

			df.to_csv(f"{DATA}/analysis/{self.ticker}_{DATE}.csv", index=False)

			self.fault_dict['analysis']['new'] = len(df)
			delta = self.fault_dict['analysis']['new'] - self.fault_dict['analysis']['old']
			
			self.fault_dict['analysis']['delta'] = delta
			self.logger.info(f"{self.ticker},{self.batch_id},Re-Analysis,Success,{delta}")

		else:

			self.logger.info(f"{self.ticker},{self.batch_id},Analysis,None Collected,")
