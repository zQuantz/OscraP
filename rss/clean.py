from elasticsearch.helpers.errors import BulkIndexError
from elasticsearch import Elasticsearch, helpers
from urllib.parse import urlparse
from const import DIR, CONFIG
from bs4 import BeautifulSoup
from langid import classify
from hashlib import sha256
import pandas as pd
import requests
import sys, os
import json
import time
import re

###################################################################################################

ES_CLIENT = Elasticsearch(CONFIG['ES_IP'], http_comprress=True, timeout=30)
HEADERS = {"Content-Type" : "application/json"}

df = pd.read_csv("data/tickers.csv")
df['FullCode'] = df.ExchangeCode + ":" + df.Ticker

fullcode_set = set(df.FullCode)
ticker_set = set(df.Ticker)
href_set = {"stock", "stocks", "symbol"}

df = pd.read_csv("data/exchanges.csv")
exchange_set = df.Acronym.dropna().tolist()
exchange_set += df['Exchange Name'].dropna().tolist()

extra_exchange_set = ["Oslo", "Paris", "Helsinki", "Copenhagen", "OTC", "OTCQX"]
extra_exchange_set += ["OTCQB", "Stockholm", "CNSX", "OTC Markets", "Brussels"]
extra_exchange_set += ["Frankfurt", "Amsterdam", "Iceland", "Vilnius", "Tallinn"]
extra_exchange_set += ["Luxembourg", "Irish", "Riga", "Symbol"]
extra_exchange_set = [exch.upper() for exch in extra_exchange_set]

exchange_set += extra_exchange_set
exchange_set = [re.sub("-|\.", "", exch) for exch in exchange_set]

TICKER_PAT = "[A-Z\.-]{3,15}[\s]{0,1}:[\s]{0,1}[A-Z\.-]{1,15}"
TICKER_PAT2 = "\((?:Symbol|Nasdaq|Euronext)[\s]{0,1}:[\s]{0,1}[A-Z\.-]+\)"
SUB_PAT = "<pre(.*?)</pre>|<img(.*?)/>|<img(.*?)>(.*?)</img>|</br>"
DEFAULT_TIME = "1970-01-01 00:00:00"

NEWS_DIR = f"{DIR}/news_data"

###################################################################################################

def get_scores(sentences):

	response = requests.post("http://localhost:9602", headers=HEADERS, json={"sentences" : sentences})
	response = json.loads(response.content)
	return response.values()

def validate(match, hit, miss):

	if match.count(":") == 1:
		match = re.sub(" : |: | :", ":", match)
		exch, ticker = match.split(":")
		exch = re.sub("-|\.|Other ", "", exch).upper()
		match = f"{exch}:{ticker}"

	if match in fullcode_set:
		hit.append(match)
	elif ":" in match and match.split(":")[0] in exchange_set:
		hit.append(match)
	elif match in ticker_set:
		hit.append(match)
	else:
		miss.append(match)

	return match

def clean(item):

	ticker_matches = []
	ticker_misses = []
	categories = []
	_authors = []
	contribs = []
	tables = []

	###############################################################################################
	## Author and Categories

	default = {"name" : None}

	_authors.append(item.get("author"))

	for author in item.get("authors", []):
		_authors.append(author.get('name'))

	_authors.append(item.get("author_detail", default).get('name'))
	_authors.append(item.get("publisher"))

	_authors = [author for author in _authors if author]

	article_source = urlparse(item['link']).netloc
	article_source = article_source.split(".")[1]

	for contributor in item.get("contributors", []):
		contribs.append(contributor.get('name'))

	keyword = item.get('dc_keyword')
	if keyword:	
		categories.append(keyword)

	###############################################################################################
	## Tickers & Categories from tags

	for tag in item.get('tags', []):

		if not tag['scheme']:
			continue

		if "ISIN" in tag['scheme']:
			continue

		if "http" in tag['scheme']:

			url = tag['scheme'].split("/")[3:]
			url = set(url)

			if len(url.intersection(href_set)) == 1:
				validate(tag['term'], ticker_matches, ticker_misses)

			elif "taxonomy" in url:

				finds = re.findall("\s([A-Z]+)\s", f" {tag['term']} ")
				if len(finds) == 1:						
					validate(tag['term'], ticker_matches, ticker_misses)

		elif tag['scheme'] == "stock-symbol":

			validate(tag['term'], ticker_matches, ticker_misses)

		else:

			categories.append(tag['term'])

	###############################################################################################
	## NASDAQ Tickers

	try:

		tickers = item['nasdaq_tickers']
		tickers = tickers.split(",")
		
		for ticker in tickers:

			if ":" not in ticker:
				ticker = "NASDAQ:" + ticker

			validate(ticker, ticker_matches, ticker_misses)

	except:

		pass

	###############################################################################################
	## HTML Summary

	summary = item.get('summary', '')
	_summary = BeautifulSoup(summary, "lxml")

	a_tags = _summary.find_all("a")
	for a_tag in a_tags:

		text = f" {a_tag.text} "
		classes = a_tag.get("class", [""])
		href = set(a_tag.get("href", "").split("/")[3:])
		finds = re.findall("\s([A-Z]+)\s", text)

		if len(finds) != 1 or ' ' in a_tag.text:
			continue

		text = text.strip()
		if 'ticker' in classes or len(href.intersection(href_set)) >= 1:
			text = validate(text, ticker_matches, ticker_misses)

		a_tag.replace_with(_summary.new_string(text))

	summary = str(_summary)

	fullcodes = re.findall(TICKER_PAT, summary)
	for fullcode in fullcodes:
		
		text = validate(fullcode, ticker_matches, ticker_misses)
		summary = summary.replace(fullcode, text)

	symbols = re.findall(TICKER_PAT2, summary)
	for symbol in symbols:
		text = validate(symbol[1:-1], ticker_matches, ticker_misses)

	###############################################################################################
	## Summary Part 2

	summary = re.sub(SUB_PAT, "", str(summary))
	_summary = BeautifulSoup(summary, "lxml")

	_tables = _summary.find_all("table")
	for table in _tables:
		tables.append(str(table))
		table.replace_with(_summary.new_string(""))

	xls = _summary.find_all("ul")
	xls += _summary.find_all("ol")
	for xl in xls:
		
		xl_str = ""
		lis = xl.find_all("li")
		
		for li in lis:

			li = li.text.strip()
			
			if len(li) == 0:
				continue

			if li[-1] not in ";.,:?!":
				li += "."

			xl_str += f"{li} "

		xl.replace_with(_summary.new_string(xl_str.strip()))

	summary = ""
	ctr = 0
	for string in _summary.strings:

		summary = summary.strip()
		if string == '\n':
			ctr += 1
		else:
			ctr = 0

		if len(summary) > 0 and ctr > 2 and summary[-1] not in ".:;?!":
			summary = summary + f". {string}"
		else:
			summary = summary + f" {string}"

	###############################################################################################
	## Time Stuff

	timestamp = item.get('published', item.get('updated'))
	try:
		timestamp = pd.to_datetime(timestamp)
	except Exception as e:
		timestamp = pd.to_datetime(int(timestamp))

	timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")

	oscrap_timestamp = item.get('oscrap_acquisition_datetime', DEFAULT_TIME)
	oscrap_timestamp = oscrap_timestamp[:19]

	if oscrap_timestamp == "None":
		oscrap_timestamp = DEFAULT_TIME

	###############################################################################################
	## Language

	language = item.get('language', classify(f"{item['title']} {summary}")[0])

	###############################################################################################
	## Create new object

	new_item = {
		'title' : item['title'],
		'summary' : summary,
		'_summary' : item.get('summary', ''),
		'timestamp' : timestamp,
		'oscrap_timestamp' : oscrap_timestamp,
		'language' : language,
		'link' : item['link'],
		'article_source' : article_source,
		'source' : "rss"
	}

	if ticker_matches:
		new_item['tickers'] = list(set(ticker_matches))

	if ticker_misses:
		new_item['_tickers'] = list(set(ticker_misses))

	if categories:
		new_item['categories'] = list(set(categories))

	if _authors:
		new_item['authors'] = list(set(_authors))

	if contribs:
		new_item['related'] = list(set(contribs))

	if tables:
		new_item['tables'] = tables

	if item.get('credit'):
		new_item['credit'] = item['credit']

	return new_item

def cleaning_loop():

	files = set([".gitignore"])

	while True:

		new_files = os.listdir(NEWS_DIR)
		if len(new_files) < len(files):
			files = set([".gitignore"])
		
		items = []
		for new_file in set(new_files).difference(files):

			with open(f"{NEWS_DIR}/{new_file}", "r") as file:

				try:
					items.extend(json.loads(file.read()))
					files.add(new_file)
				except Exception as e:
					print(e)

		new_items = []
		for item in items:

			title = item.get("title", None)
			if not title:
				continue

			dummy_item = {
				"link" : item['link'],
			}
			dummy_item = json.dumps(dummy_item, sort_keys = True)
			_hash = sha256(dummy_item.encode()).hexdigest()

			new_items.append({
				"_index" : "news",
				"_id" : _hash,
				"_op_type" : "create",
				"_source" : clean(item)
			})

		if len(new_items) != 0:

			titles = [
				item['_source']['title']
				for item in new_items
			]
			scores = get_scores(titles)

			for item, score in zip(new_items, scores):
				item['_source']['sentiment'] = score['prediction']
				item['_source']['sentiment_score'] = score['sentiment_score']

			successes, failures = helpers.bulk(ES_CLIENT,
											   new_items,
											   stats_only=True,
											   raise_on_error=False)
			new_items = []

		time.sleep(5)

if __name__ == '__main__':

	cleaning_loop()

