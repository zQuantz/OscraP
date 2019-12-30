from datetime import datetime
import logging
import os

## Logging
DIR = os.path.realpath(os.path.dirname(__file__))
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/scraper.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

## Today's Date
date_today = datetime.today().strftime("%Y-%m-%d")
named_date_fmt = "%B %d, %Y"

## Database Columns
option_cols = ['OptionID', 'Ticker', 'CurrentDate', 'ExpirationDate',
			   'TimeToExpiry', 'OptionType', 'StrikePrice', 'Bid', 'Ask',
			   'Volume', 'OptionPrice', 'ImpliedVolatility', 'OpenInterest']
option_new_cols = ['option_id', 'ticker', 'date_current', 'expiration_date',
				   'time_to_expiry', 'option_type', 'strike_price', 'bid', 'ask',
				   'volume', 'option_price', 'implied_volatility', 'open_interest']

equity_cols = ['Ticker', 'CurrentDate', 'Open', 'High', 'Low', 'Close',
			   'AdjClose', 'StockVolume', 'DividendYield']
equity_new_cols = ['ticker', 'date_current', 'open', 'high', 'low', 'close',
				   'adj_close', 'stock_volume', 'dividend_yield']