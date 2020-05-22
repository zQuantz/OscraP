from datetime import datetime
import logging
import socket
import json
import sys
import os

###################################################################################################

DIR = os.path.realpath(os.path.dirname(__file__))

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

date = datetime.today().strftime("%Y-%m-%d")
with open(f"{DIR}/../config.json", "w") as file:
	
	CONFIG['date'] = date

	if socket.gethostname() == "gpsvm":
		CONFIG['db'] = "compour9_finance"
		CONFIG['gcp_bucket_prefix'] = "rates"
	else:
		CONFIG['db'] = "compour9_test"
		CONFIG['gcp_bucket_prefix'] = "tmp"

	db_address = "mysql://{user}:{password}@{ip}:{port}/{db}"
	db_address = db_address.format(user=CONFIG['db_user'], password=CONFIG['db_password'],
								   ip=CONFIG['db_ip'], port=CONFIG['db_port'], db=CONFIG['db'])
	CONFIG['db_address'] = db_address

	file.write(json.dumps(CONFIG))

###################################################################################################
