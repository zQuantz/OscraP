import json
import os

###################################################################################################

DIR = os.path.dirname(os.path.realpath(__file__))
with open("../config.json", "r") as file:
	CONFIG = json.loads(file.read())

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

NEW['tickermaps'] = f"{DIR}/data/new/tickermaps"
NEW['treasuryratemap'] = f"{DIR}/data/new/treasuryratemap"

###################################################################################################