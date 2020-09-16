from const import DIR, DATE, logger, _connector
from datetime import datetime
import pandas as pd

###################################################################################################

BASE = "https://eresearch.fidelity.com/eresearch/conferenceCalls.jhtml?tab=splits&begindate={date}"
COLUMNS = [
	"ticker",
	"split_factor",
	"announcement_date",
	"record_date",
	"ex_date"
]

###################################################################################################

def process(dt):

	tries, max_tries = 0, 5
	while tries < max_tries:

		try:

			df = pd.read_html(BASE.format(date=dt), attrs = {"class" : "datatable-component"})	
			
			if len(df) != 1:
				raise Exception("Too Many Tables.")

			df = df[0].iloc[1:, 1:]
			df.columns = COLUMNS

			sf = df.split_factor.str
			sf = sf.split(":", expand=True).astype(float)

			df = df[~df.ticker.str.contains(":CA")]
			df['split_factor'] = sf[1] / sf[0]
			df['processed'] = False

			for col in COLUMNS[-3:]:
				df[col] = pd.to_datetime(df[col]).astype(str)

			return df

		except Exception as e:

			tries += 1

	if tries > max_tries:
		raise Exception("Too Many Tries.")

def once():

	dts = pd.date_range(start=datetime(2019, 10, 1), end=datetime.now(), freq="MS")
	dts = pd.Series(dts).dt.strftime("%m/%d/%Y")

	dfs = []
	for dt in dts:
		
		print("Processing:", dt)

		try:
			dfs.append(process(dt))
		except Exception as e:
			1/0
		
	df = pd.concat(dfs)
	df = df.sort_values("ex_date", ascending=True)
	return df.reset_index(drop=True)

def splits():

	logger.info(f"SCRAPER,SPLITS,INITIATED,")

	now = datetime.now()
	dt = datetime(now.year, now.month, 1).strftime("%m/%d/%Y")

	try:

		df = process(dt)

		_connector.execute("DELETE FROM stocksplitstmpBACK;")
		_connector.write("stocksplitstmpBACK", df.reset_index(drop=True))
		_connector.execute("""
				INSERT IGNORE INTO
					stocksplitsBACK
				SELECT
					*
				FROM
					stocksplitstmpBACK
			""")
		
		logger.info(f"SCRAPER,SPLITS,TERMINATED,{len(df)}")

	except Exception as e:

		logger.warning(f"SCRAPER,SPLITS,FAILURE,{e}")
