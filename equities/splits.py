from const import CONFIG, DIR, DATE, logger, _connector
from datetime import datetime
import tarfile as tar
import pandas as pd
import sys, os

sys.path.append(f"{DIR}/../utils")
from send_email import send_email
from gcp import send_to_bucket
from gcp import send_gcp_metric

###################################################################################################

BASE = "https://eresearch.fidelity.com/eresearch/conferenceCalls.jhtml?tab=splits&begindate={date}"
COLUMNS = [
	"ticker",
	"split_factor",
	"announcement_date",
	"record_date",
	"ex_date"
]

P_COLUMNS = [
	"ticker",
	"ex_date",
	"procedure_order",
	"procedure_name",
	"d1",
	"d2",
	"split_factor",
	"processed_timestamp"
]

BUCKET_NAME = CONFIG['gcp_bucket_name']

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
			df['processed_timestamp'] = None

			for col in COLUMNS[-3:]:
				df[col] = pd.to_datetime(df[col]).astype(str)

			def multiply(group):
				group['split_factor'] = group.split_factor.product()
				return group.iloc[-1, :]

			df = df.groupby(["ticker", "ex_date"]).apply(multiply)
			df = df.reset_index(drop=True)

			return df

		except Exception as e:

			print(e)
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

def store(df):

	filename = DIR / "split_data" / f"{DATE}.csv" 
	df.to_csv(filename, index=False)
	with tar.open(filename.with_suffix(".tar.xz"), "x:xz") as tar_file:
		tar_file.add(filename, filename.name)
	os.unlink(filename)

	send_to_bucket("splits", BUCKET_NAME, f"{DATE}.tar.xz", f"{DIR}/split_data", logger=logger)

def splits():

	logger.info(f"SCRAPER,SPLITS,INITIATED,")

	now = datetime.now()
	report_df = pd.DataFrame()
	dt = datetime(now.year, now.month, 1).strftime("%m/%d/%Y")

	DATE = "2020-08-31"
	_connector.date = DATE

	try:

		df = once()
		# store(df)

		_connector.execute("DELETE FROM stocksplitstmpBACK;")
		_connector.write("stocksplitstmpBACK", df)
		_connector.execute("""
				INSERT IGNORE INTO
					stocksplitsBACK
				SELECT
					*
				FROM
					stocksplitstmpBACK;
			""")

		df = df[df.ex_date == DATE]
		if len(df) != 0:

			logger.info(f"SCRAPER,SPLITS,ADJUSTING,{len(df)}")
			_connector.register_splits(P_COLUMNS, "BACK")
			_connector.adjust_splits("BACK")
		
		metric = 1
		title_modifier = "SUCCESS"
		logger.info(f"SCRAPER,SPLITS,TERMINATED,{len(df)}")

	except Exception as e:

		metric = 0
		title_modifier = "FAILURE"
		logger.warning(f"SCRAPER,SPLITS,FAILURE,{e}")

	###############################################################################################

	report = _connector.read("""
			SELECT
				*
			FROM
				stocksplitstatusBACK
			WHERE
				ex_date = "{date}"
		""".format(date=DATE))

	send_gcp_metric(CONFIG, "splits_success_indicator", "int64_value", metric)
	send_email(CONFIG, f"{title_modifier} - Stock Splits", report.to_html(), [], logger)

if __name__ == '__main__':

	splits()
