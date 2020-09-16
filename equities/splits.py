from const import DIR, DATE, logger
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

def once():

	dts = pd.date_range(start=datetime(2019, 10, 1), end=datetime.now(), freq="MS")
	dts = pd.Series(dts).dt.strftime("%m/%d/%Y")

	dfs = []
	for dt in dts:
		
		print("Processing:", dt)

		tries, max_tries = 0, 5
		while tries < max_tries:
			
			try:
				dfs.append(process(dt))
				break
			except Exception as e:
				print(e)

			tries += 1

			if tries > max_tries:
				print("Too Many Tries.")
		
	df = pd.concat(dfs)
	df = df.sort_values("ex_date", ascending=True)
	return df.reset_index(drop=True)

def splits():

	logger.info(f"SCRAPER,SPLITS,INITIATED,")

	now = datetime.now()
	dt = datetime(now.year, now.month, 1).strftime("%m/%d/%Y")
	
	tries = 0
	max_tries = 5

	while tries < max_tries:

		try:
			df = process(dt)
			break
		except Exception as e:
			logger.warning(f"SCRAPER,SPLITS,FAILURE,{e}")

		tries += 1

		if tries > max_tries:
			raise Exception("Splits. Too Many Tries.")

	df = df[df.ex_date == DATE]
	logger.info(f"SCRAPER,SPLITS,TERMINATED,{len(df)}")

	return df
