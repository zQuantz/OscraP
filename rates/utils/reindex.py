import sqlalchemy as sql
import pandas as pd
import numpy as np
import sys, os
import json

###################################################################################################

TABLE_CREATOR = """
	CREATE TABLE rates (
		date_current date PRIMARY KEY,
		_1_month FLOAT,
		_2_months FLOAT,
		_3_months FLOAT,
		_6_months FLOAT,
		_1_year FLOAT,
		_2_years FLOAT,
		_3_years FLOAT,
		_5_years FLOAT,
		_7_years FLOAT,
		_10_years FLOAT,
		_20_years FLOAT,
		_30_years FLOAT
	)
"""

attrs = {"class" : "t-chart"}
BULK_URL = "https://www.treasury.gov/resource-center/data-chart-center/interest-rates/pages/textview.aspx?data=yieldAll"

with open("../config.json", "r") as file:
	CONFIG = json.loads(file.read())

###################################################################################################

def main():

	print("Downloading Table:", BULK_URL)
	df = pd.read_html(BULK_URL, attrs=attrs)
	print("Number of tables found:", len(df))

	if len(df) != 1:
		return

	df = df[0]
	df.columns = [
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

	df['date_current'] = pd.to_datetime(df.date_current)
	df = df.sort_values('date_current', ascending=False)
	df = df.reset_index(drop=True)

	###############################################################################################

	db_address = CONFIG['db_address']
	db_address = db_address[:db_address.rfind("/")]
	db_address += "/compour9_finance"
	engine = sql.create_engine(db_address)

	print("Dropping, Creating and Indexing", db_address)

	try:
		engine.execute("DROP TABLE rates")
		engine.execute(TABLE_CREATOR)
	except Exception as e:
		print(e)

	df.to_sql("rates", engine, if_exists='append', index=False, chunksize=10_000)

	###############################################################################################

	db_address = CONFIG['db_address']
	db_address = db_address[:db_address.rfind("/")]
	db_address += "/compour9_test"
	engine = sql.create_engine(db_address)

	print("Dropping, Creating and Indexing", db_address)

	try:
		engine.execute("DROP TABLE rates")
		engine.execute(TABLE_CREATOR)
	except Exception as e:
		print(e)

	df.to_sql("rates", engine, if_exists='append', index=False, chunksize=10_000)

	###############################################################################################

if __name__ == '__main__':

	main()