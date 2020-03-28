from const import DIR, date_today, logger, DELIM
from bs4 import BeautifulSoup
import pandas as pd
import requests
import sys, os

URL = "https://www.cnbc.com/quotes/?symbol={ticker}&qsearchterm={ticker}&tab=news"
TICKERS = ["AAPL", "TSLA", "UBER"]

def get_news(ticker):

	url = URL.format(ticker = ticker)
	page = requests.get(url).content
	page = BeautifulSoup(page)

	page = page.find("h3", text="latest news")
	page = page.parent.parent.parent

	articles = []
	notes = page.find_all("span", {"class" : "note"})
	for note in notes:
	    parent = note.parent
	    
	    a = parent.find("a")
	    
	    href = a.get_attribute_list("href")[0]
	    if 'https' not in href:
	        continue
	    
	    title = a.find("span").text
	    articles.append([title, note.text, href])

	df = pd.DataFrame(articles, columns = ['title', 'date', 'link'])
	df.to_csv(f'{DIR}/news_data/{date_today}/{ticker}_{date_today}.csv', delimiter=DELIM, index=False)

def init_folders():

	os.mkdir(f'{DIR}/news_data/{date_today}')

if __name__ == '__main__':

	init_folders()

	for ticker in TICKERS:
		get_news(ticker)

