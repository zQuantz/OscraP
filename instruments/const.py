from datetime import datetime
import logging
import socket
import json
import sys
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

fh = logging.FileHandler(f'{DIR}/instruments.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

###################################################################################################

with open(f"{DIR}/../config.json", "r") as file:
	CONFIG = json.loads(file.read())

date = datetime.today().strftime("%Y-%m-%d")
with open(f"{DIR}/../config.json", "w") as file:
	
	CONFIG['date'] = date

	if socket.gethostname() == "gpsvm":
		CONFIG['db'] = "compour9_finance"
		CONFIG['gcp_bucket_prefix'] = "instruments"
	else:
		CONFIG['db'] = "compour9_test"
		CONFIG['gcp_bucket_prefix'] = "tmp"

	file.write(json.dumps(CONFIG))

###################################################################################################
