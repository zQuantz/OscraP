from datetime import datetime
from uuid import getnode
import logging
import json
import os

###################################################################################################

CONVERTER = {"M" : 1_000_000}
CONVERTER["B"] = CONVERTER["M"] * 1_000
CONVERTER["T"] = CONVERTER["B"] * 1_000
NUMBERS = ''.join([str(i) for i in range(10)])

###################################################################################################

DIR = os.path.realpath(os.path.dirname(__file__))

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/scraper.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

###################################################################################################

with open(f"{DIR}/static/config.json", "r") as file:
	CONFIG = json.loads(file.read())

date = datetime.today().strftime("%Y-%m-%d")
with open(f"{DIR}/static/config.json", "w") as file:
	
	CONFIG['date'] = "2020-04-24"#date

	if getnode() == 48252843880008:
		CONFIG['db'] = "compour9_finance"
	else:
		CONFIG['db'] = "compour9_test"

	db_address = "mysql://{user}:{password}@{ip}:{port}/{db}"
	db_address = db_address.format(user=CONFIG['db_user'], password=CONFIG['db_password'],
								   ip=CONFIG['db_ip'], port=CONFIG['db_port'], db=CONFIG['db'])
	CONFIG['db_address'] = db_address

	file.write(json.dumps(CONFIG))