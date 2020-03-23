from datetime import datetime
import logging
import os

## Formatters
NUMBERS = ''.join([str(i) for i in range(10)])

CONVERTER = {"K" : 1_000}
CONVERTER["M"] = CONVERTER["K"] * 1_000
CONVERTER["B"] = CONVERTER["M"] * 1_000
CONVERTER["T"] = CONVERTER["B"] * 1_000
for key in CONVERTER.copy():
	CONVERTER[key.lower()] = CONVERTER[key]

print(CONVERTER)

## Logging
DIR = os.path.realpath(os.path.dirname(__file__))
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/tickers.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

## Today's Date
date_today = datetime.today().strftime("%Y-%m-%d")
named_date_fmt = "%B %d, %Y"