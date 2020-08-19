import json
import os

DIR = os.path.dirname(os.path.realpath(__file__))
with open("../config.json", "r") as file:
	CONFIG = json.loads(file.read())