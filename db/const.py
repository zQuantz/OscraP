from datetime import datetime
from pathlib import Path
import sqlalchemy as sql
import json
import os

###################################################################################################

DIR = os.path.dirname(os.path.realpath(__file__))
DATE = datetime.today().strftime("%Y-%m-%d")

with open(f"{DIR}/../config.json", "r") as file:
	CONFIG = json.loads(file.read())

CONFIG['db_address'] = CONFIG['db_address'].replace("finance", "test")

from connector import Connector
_connector = Connector(CONFIG, DATE)

###################################################################################################

OLD = {
	"equity" : f"{DIR}/data/old/equities",
	"rates" : f"{DIR}/data/old/rates",
	"instruments" : f"{DIR}/data/old/instruments",
	"rss" : f"{DIR}/data/old/rss"
}

NEW = {
	key : value.replace("/old/", "/new/")
	for key, value in OLD.items()
}
NEW['rates'] = f"{DIR}/data/new/treasuryrates"

TAR = {
	key : value.replace("/new/", "/tar/new/")
	for key, value in NEW.items()
}

OLD = {k : Path(v) for k, v in OLD.items()}
NEW = {k : Path(v) for k, v in NEW.items()}
TAR = {k : Path(v) for k, v in TAR.items()}

###################################################################################################

TABLE_NAMES = [
	"options",
	"ohlc",
	"keystats",
	"analysis",
	"surface",
	"surfacestats",
	"surfaceskew",
	"optionstats",
	"ohlcstats",
	"aggoptionstats",
	"optionscounts",
	"analysiscounts",
	"keystatscounts",
	"treasuryratemap",
	"treasuryrates",
	"instruments",
	"tickerdates",
	"tickeroids"
]