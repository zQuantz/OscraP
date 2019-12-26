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