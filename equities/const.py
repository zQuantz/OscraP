import logging
import os

## Formatters
CONVERTER = {"M" : 1_000_000}
CONVERTER["B"] = CONVERTER["M"] * 1_000
CONVERTER["T"] = CONVERTER["B"] * 1_000
NUMBERS = ''.join([str(i) for i in range(10)])

DIR = os.path.realpath(os.path.dirname(__file__))

## Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(f'{DIR}/scraper.log')
formatter = logging.Formatter('%(asctime)s - %(message)s')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)