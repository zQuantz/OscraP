from datetime import datetime
from pathlib import Path
import logging
import socket
import json
import sys
import os

###################################################################################################

CONVERTER = {"M" : 1_000_000}
CONVERTER["B"] = CONVERTER["M"] * 1_000
CONVERTER["T"] = CONVERTER["B"] * 1_000
NUMBERS = ''.join([str(i) for i in range(10)])

###################################################################################################

DIR = os.path.realpath(os.path.dirname(__file__))
DIR = Path(DIR)

DATE = datetime.today().strftime("%Y-%m-%d")
DATA = DIR / "financial_data" / DATE

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/scraper.log')
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
		CONFIG['gcp_bucket_prefix'] = "equities"
	else:
		CONFIG['db'] = "compour9_test"
		CONFIG['gcp_bucket_prefix'] = "tmp"

	db_address = "mysql://{user}:{password}@{ip}:{port}/{db}"
	db_address = db_address.format(user=CONFIG['db_user'], password=CONFIG['db_password'],
								   ip=CONFIG['db_ip'], port=CONFIG['db_port'], db=CONFIG['db'])
	CONFIG['db_address'] = db_address

	file.write(json.dumps(CONFIG))

###################################################################################################

sys.path.append(f"{DIR}/../db")
from connector import Connector
_connector = Connector(CONFIG, DATE, logger)
