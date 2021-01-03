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

###################################################################################################

OLD = {
	"treasuryrates" : f"{DIR}/data/old/treasuryrates",
	"instruments" : f"{DIR}/data/old/instruments",
	"equity" : f"{DIR}/data/old/equities",
	"splits" : f"{DIR}/data/old/splits",
}

NEW = {
	key : value.replace("/old/", "/new/")
	for key, value in OLD.items()
}

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
	"tickeroids",
	"stocksplits",
	"stocksplitstmp",
	"stocksplitstatus"
]

from connector import Connector
_connector = Connector(CONFIG, DATE)