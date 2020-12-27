from datetime import datetime
from pathlib import Path
import numpy as np
import logging
import socket
import json
import sys
import os

###################################################################################################

DIR = os.path.realpath(os.path.dirname(__file__))
DIR = Path(DIR)

DATE = datetime.today().strftime("%Y-%m-%d")
DATA = DIR / "rate_data" / DATE

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/rates.log')
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
		CONFIG['gcp_bucket_prefix'] = "treasuryrates"
	else:
		CONFIG['db'] = "compour9_test"
		CONFIG['gcp_bucket_prefix'] = "tmp"

	db_address = "mysql://{user}:{password}@{ip}:{port}/{db}"
	db_address = db_address.format(user=CONFIG['db_user'], password=CONFIG['db_password'],
								   ip=CONFIG['db_ip'], port=CONFIG['db_port'], db=CONFIG['db'])
	CONFIG['db_address'] = db_address

	file.write(json.dumps(CONFIG))

###################################################################################################

t_map = [
    0,
    30,
    60,
    90,
    180,
    12 * 30,
    24 * 30,
    36 * 30,
    60 * 30,
    72 * 30,
    120 * 30,
    240 * 30,
    360 * 30
]
t_map = np.array(t_map)

t_names = [
    "date_current",
    "_1_month",
    "_2_months",
    "_3_months",
    "_6_months",
    "_1_year",
    "_2_years",
    "_3_years",
    "_5_years",
    "_7_years",
    "_10_years",
    "_20_years",
    "_30_years",
]

###################################################################################################

sys.path.append(f"{DIR}/../db")
from connector import Connector
_connector = Connector(CONFIG, DATE, logger)
