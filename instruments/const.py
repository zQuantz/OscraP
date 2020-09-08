from datetime import datetime
from pathlib import Path
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

###################################################################################################

DIR = os.path.realpath(os.path.dirname(__file__))
DIR = Path(DIR)

DATE = datetime.today().strftime("%Y-%m-%d")
DATA = DIR / "instrument_data" / DATE

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

with open(f"{DIR}/../config.json", "w") as file:
	
	if socket.gethostname() == CONFIG['gcp_hostname']:
		CONFIG['db'] = "compour9_finance"
		CONFIG['gcp_bucket_prefix'] = "instruments"
	else:
		CONFIG['db'] = "compour9_test"
		CONFIG['gcp_bucket_prefix'] = "tmp"

	file.write(json.dumps(CONFIG))

###################################################################################################

sys.path.append(f"{DIR}/../db")
from connector import Connector
_connector = Connector(CONFIG, DATE, logger)
