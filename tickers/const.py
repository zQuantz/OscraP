from datetime import datetime
import logging
import os

###################################################################################################

COLUMNS = [
	'ticker',
	'name',
	'exchange_code',
	'exchange_name',
	'sector',
	'industry',
	'instrument_type',
	'market_cap'
]

SQL_TABLE = """
	CREATE TABLE
		instruments (
			last_updated date,
			ticker varchar(10),
			name varchar(100),
			exchange_code varchar(10),
			exchange_name varchar(50),
			sector varchar(100),
			industry varchar(100),
			instrument_type varchar(10),
			market_cap BIGINT
		)
	"""

###################################################################################################

DIR = os.path.realpath(os.path.dirname(__file__))
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/tickers.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

###################################################################################################

date_today = datetime.today().strftime("%Y-%m-%d")
named_date_fmt = "%B %d, %Y"