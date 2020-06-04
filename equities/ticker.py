from const import DIR, CONVERTER, NUMBERS, CONFIG

from greeks import calculate_greeks
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
DATE = CONFIG['date']

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

		if not retries or retries['key_stats']:
			try:
				self.get_key_stats()
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

		if ('.TO' not in ticker) and (not retries or retries['options']):
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
			self.logger.info(f'{self.ticker},{self.batch_id},Value,{str_},{metric}')
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
			self.logger.info(f"{self.ticker},{self.batch_id},Value,'',{metric}")
			return 0

		if str_number == 'N/A':
			self.logger.info(f'{self.ticker},{self.batch_id},Value,N/A,{metric}')
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

		return self.option_fmt(div, 'Dividend') / 100

	def get_ohlc(self):

		url = OHLC.format(ticker = self.ticker)
		bs = BeautifulSoup(request(CONFIG, url, self.logger).content, PARSER)

		prices = bs.find("table", {"data-test" : "historical-prices"})
		prices = prices.find_all("tr")[1]
		prices = [price.text for price in prices]

		ohlc_date = datetime.strptime(prices[0], "%b %d, %Y").strftime("%Y-%m-%d")
		if ohlc_date != DATE:
			raise Exception(f'Fatal')

		cols = ['open', 'high', 'low', 'close', 'adj_close', 'stock_volume']
		prices = list(map(self.option_fmt, prices[1:], cols))
		self.adj_close = prices[-2]

		prices += [self.div, DATE]
		cols += ["dividend_yield", 'date_current']

		df = pd.DataFrame([prices], columns = cols)
		df.to_csv(f"{DIR}/financial_data/{DATE}/ohlc/{self.ticker}_{DATE}.csv", index=False)

		if self.retries and self.retries['ohlc']:

			self.fault_dict['ohlc']['new_status'] = 1
			self.logger.info(f"{self.ticker},{self.batch_id},Re-OHLC,Success,1")

	def get_options(self):

		def append_options(table, expiry_date_fmt, expiration_days, symbol):

			for row in table.find_all("tr")[1:]:
				es = [e for e in row.find_all("td")[2:]]
				self.options.append([
						DATE,
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

		url = OPTIONS.format(ticker = self.ticker)
		bs, options = get_page(url)

		for option in options:

			self.sleep()

			expiry, expiry_date = option.get("value"), option.text
			self.logger.info(f"{self.ticker},{self.batch_id},Option Expiry,{expiry},{expiry_date.replace(',', '.')}")

			expiry_date_fmt = datetime.strptime(expiry_date, NAMED_DATE_FMT).strftime("%Y-%m-%d")
			
			dt = datetime.fromtimestamp(int(expiry)).strftime("%Y-%m-%d")
			dt_now = datetime.now().strftime("%Y-%m-%d")
			expiration_days = np.busday_count(dt_now, dt)

			page = url+f"&date={str(expiry)}"
			bs, _ = get_page(page)

			calls = bs.find("table", {"class" : "calls"})
			puts = bs.find("table", {"class" : "puts"})
			
			if calls:
				append_options(calls, expiry_date_fmt, expiration_days, 'C')
			
			if puts:
				append_options(puts, expiry_date_fmt, expiration_days, 'P')

		self.options = pd.DataFrame(self.options, columns = ['date_current', 'expiration_date', 'time_to_expiry',
															 'option_type', 'strike_price', 'bid', 'ask', 'volume',
															 'option_price', 'implied_volatility', 'open_interest'])
		self.options = calculate_greeks(self.adj_close, self.div, self.options)

		if not self.retries and len(self.options) > 0:
			
			self.options.to_csv(f"{DIR}/financial_data/{DATE}/options/{self.ticker}_{DATE}.csv", index=False)

		elif len(self.options) != 0:
			
			try:
				old = pd.read_csv(f"{DIR}/financial_data/{DATE}/options/{self.ticker}_{DATE}.csv")
			except Exception as e:
				old = pd.DataFrame()

			df = pd.concat([old, self.options]).reset_index(drop=True)
			df = df.drop_duplicates(subset=['expiration_date', 'strike_price', 'option_type'], keep="last")
			df = df.sort_values(['expiration_date', 'option_type', 'strike_price'])
			df.to_csv(f"{DIR}/financial_data/{DATE}/options/{self.ticker}_{DATE}.csv", index=False)

			self.fault_dict['options']['new_options'] = len(df)
			delta = self.fault_dict['options']['new_options'] - self.fault_dict['options']['options']

			self.logger.info(f"{self.ticker},{self.batch_id},Re-Options,Success,{delta}")

		else:

			self.logger.info(f"{self.ticker},{self.batch_id},Options,None Collected,")

	def get_key_stats(self):

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

		key_stats = []
		for feature_name, feature in items:
			key = self.feature_conversion(feature_name)
			key_stats.append([
				*key,
				self.fmt(feature, metric = key[0])
			])

		df = pd.DataFrame(key_stats, columns = ["feature", "modifier", "value"])
		pkey = ["feature", "modifier"]
		df.loc[:, pkey] = df[pkey].fillna('')

		if not self.retries and len(df) > 0:
			
			df.to_csv(f"{DIR}/financial_data/{DATE}/key_stats/{self.ticker}_{DATE}.csv", index=False)

		elif len(df) != 0:
			
			try:
				old = pd.read_csv(f"{DIR}/financial_data/{DATE}/key_stats/{self.ticker}_{DATE}.csv")
			except Exception as e:
				old = pd.DataFrame()

			df = pd.concat([old, df]).reset_index(drop=True)
			df = self.drop_by_na(pkey, df)
			df.to_csv(f"{DIR}/financial_data/{DATE}/key_stats/{self.ticker}_{DATE}.csv", index=False)

			self.fault_dict['key_stats']['new_null_percentage'] = df.value.isnull().sum() / len(df)
			delta = self.fault_dict['key_stats']['new_null_percentage'] - self.fault_dict['key_stats']['null_percentage']
			
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
		
		pkey = ["category", "feature", "feature_two", "modifier"]
		df.loc[:, pkey] = df[pkey].fillna('')

		if not self.retries and len(df) > 0:
			
			df.to_csv(f"{DIR}/financial_data/{DATE}/analysis/{self.ticker}_{DATE}.csv", index=False)

		elif len(df) != 0:

			try:
				old = pd.read_csv(f"{DIR}/financial_data/{DATE}/analysis/{self.ticker}_{DATE}.csv")
			except Exception as e:
				old = pd.DataFrame()

			df = pd.concat([old, df]).reset_index(drop=True)
			df = self.drop_by_na(pkey, df)

			df.to_csv(f"{DIR}/financial_data/{DATE}/analysis/{self.ticker}_{DATE}.csv", index=False)

			self.fault_dict['analysis']['new_null_percentage'] = df.value.isnull().sum() / len(df)
			delta = self.fault_dict['analysis']['new_null_percentage'] - self.fault_dict['analysis']['null_percentage']
			
			self.logger.info(f"{self.ticker},{self.batch_id},Re-Analysis,Success,{delta}")

		else:

			self.logger.info(f"{self.ticker},{self.batch_id},Analysis,None Collected,")
