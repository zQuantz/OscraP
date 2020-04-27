from datetime import datetime
from uuid import getnode
import logging
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

DIR = os.path.realpath(os.path.dirname(__file__))

###################################################################################################

with open(f"{DIR}/../config.json", "r") as file:
	CONFIG = json.loads(file.read())

date = datetime.today().strftime("%Y-%m-%d")
with open(f"{DIR}/../config.json", "w") as file:
	
	CONFIG['date'] = date

	if getnode() == 48252843880008:
		CONFIG['db'] = "compour9_finance"
	else:
		CONFIG['db'] = "compour9_test"

	db_address = "mysql://{user}:{password}@{ip}:{port}/{db}"
	db_address = db_address.format(user=CONFIG['db_user'], password=CONFIG['db_password'],
								   ip=CONFIG['db_ip'], port=CONFIG['db_port'], db=CONFIG['db'])
	CONFIG['db_address'] = db_address

	file.write(json.dumps(CONFIG))

###################################################################################################
