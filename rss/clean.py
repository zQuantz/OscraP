from unicodedata import normalize
from urllib.parse import urlparse
from socket import gethostname
from bs4 import BeautifulSoup
from langid import classify
from hashlib import sha256
from const import DIR
import pandas as pd
import sys, os
import json
import time
import uuid
import re

###################################################################################################

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

NEWS_DIR = f"{DIR}/news_data"
CLEAN_NEWS_DIR = f"{DIR}/clean_news_data"

###################################################################################################

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

	link_author = urlparse(item['link']).netloc
	link_author = link_author.split(".")[1]
	_authors.append(link_author)

	_authors = [author for author in _authors if author]

	if item.get('author') == "":
		author_display = link_author
	else:
		author_display = "  /  ".join([link_author, item.get("author")])

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
	## Time stamp

	timestamp = item.get('published', item.get('updated'))
	try:
		timestamp = str(pd.to_datetime(timestamp))
	except Exception as e:
		timestamp = str(pd.to_datetime(int(timestamp)))

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
		'oscrap_timestamp' : item.get('oscrap_acquisition_datetime', timestamp),
		'language' : language,
		'link' : item['link'],
		'author_display' : author_display
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
	items = set()

	while True:

		new_files = set(os.listdir(NEWS_DIR)).difference(files)
		
		items = []
		for new_file in new_files:

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

			summary = item.get("summary", None)
			author = item.get("author", "")

			dummy_item = {
				"title" : title,
				"summary" : summary,
				"author" : author
			}
			dummy_item = json.dumps(dummy_item, sort_keys = True)
			hash_ = sha256(dummy_item.encode()).hexdigest()

			if hash_ in items:
				continue

			new_items.append(clean(item))

		if len(new_items) != 0:

			with open(f"{DIR}/clean_news_data/{str(uuid.uuid4())}.txt", "w") as file:
				file.write(json.dumps(new_items))

		time.sleep(5)

if __name__ == '__main__':

	cleaning_loop()

