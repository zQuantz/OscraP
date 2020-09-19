from datetime import datetime
from threading import Thread
from procedures import *
import sqlalchemy as sql
from const import DIR
import pandas as pd
import sys, os

sys.path.append(f"{DIR}/..")
from utils.dummy_logger import DummyLogger

class Connector:

	def __init__(self, CONFIG, date, logger=None):

		self.db = CONFIG['db']
		self.db_address = CONFIG['db_address']
		self.engine = sql.create_engine(self.db_address,
										pool_size=3,
										max_overflow=0,
										pool_recycle=299,
										pool_pre_ping=True)

		self.max_tries = 10
		self.date = date

		self.logger = logger
		if not logger:
			self.logger = DummyLogger()

	def transact(self, action, *args):

		tries = 0
		while tries < self.max_tries:

			try:

				with self.engine.connect() as conn:
					return action(conn, *args)

			except Exception as e:

				name = action.__name__.capitalize()
				self.logger.warning(f"Connector Error,{name},{e}")

			tries += 1

		if tries >= self.max_tries:
			raise Exception("Too Many SQL Errors.")

	def read(self, query):

		def read(conn, query):
			return pd.read_sql(query, conn)

		return self.transact(read, query)

	def write(self, table, df):

		def write(conn, table, df):
			return df.to_sql(table,
							 conn,
							 if_exists='append',
							 index=False,
							 chunksize=100_000)

		return self.transact(write, table, df)

	def execute(self, statement):

		def execute(conn, statement):
			return conn.execute(statement)

		return self.transact(execute, statement)

	###############################################################################################

	def get_equities_table_count(self):

		return self.read("""
				SELECT
					TABLE_NAME AS tablename,
					TABLE_ROWS AS row_count
				FROM
					information_schema.tables
				WHERE
					TABLE_SCHEMA = "{db}"
				AND
					TABLE_NAME in ("options", "ohlc", "analysis", "keystats")
			""".format(db=self.db))


	def init_batch_tickers(self, batch_id, tickers):

		self.execute(f"""DROP TABLE IF EXISTS batchtickers{batch_id};""")
		self.execute(f"""CREATE TABLE batchtickers{batch_id} (ticker VARCHAR(10) PRIMARY KEY NOT NULL);""")
		batchtickers = pd.DataFrame(tickers, columns = ['ticker'])
		self.write(f"batchtickers{batch_id}", batchtickers)

	def init_date_series(self):

		self.execute("DELETE FROM dateseries;")
		self.execute("""
				INSERT INTO 
					dateseries (
						lag,
						lag_date
					)
				VALUES
					(0, "{date}");
			""".format(date=self.date))
		self.execute("SET @i = 0;")
		self.execute(INIT_DATE_SERIES.format(modifier="", date=self.date))
		self.execute(UPDATE_DATE_SERIES)

	def get_equity_tickers(self, N_USD):

		df = self.read("""
				SELECT
					ticker
				FROM
					instruments
				WHERE
					market_cap >= {}
				AND exchange_code != "TSX"
				ORDER BY
					market_cap DESC
				LIMIT {}
			""".format(1_000_000, N_USD))

		return tuple(df.ticker)

	def get_lower_bounds(self, tablename, batch_id):

		return self.read("""
				SELECT
					ticker,
					AVG(count) * 0.95 as lower_bound
				FROM
					{}
				WHERE
					date_current >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
				AND
					ticker in (SELECT ticker FROM batchtickers{})
				GROUP BY
					ticker
			""".format(tablename, batch_id))

	def get_distinct_ohlc_tickers(self, batch_id):

		return self.read("""
				SELECT
					DISTINCT ticker
				FROM
					ohlc
				WHERE
					date_current >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
				AND ticker in (SELECT ticker FROM batchtickers{})
			""".format(batch_id))

	def launch_derived_engine(self, batch_id):

		def derived_engine():

			for name, procedure in get_derived_procedures(self.date).items():
				self.logger.info(f"Derived Engine,{batch_id},Executing Procedure,{name}")
				procedure = procedure.replace("batchtickers", f"batchtickers{batch_id}")
				procedure = procedure.format(date=self.date)
				self.execute(procedure)

		thread = Thread(target = derived_engine)
		thread.start()

	def adjust_for_splits(self, tickers, factors, modifier=""):

		self.logger.info(f"SPLITS,INITIATED,{' '.join(tickers)},")

		start_date = datetime.strptime("2020-07-01", "%Y-%m-%d")
		end_date = datetime.strptime(self.date, "%Y-%m-%d")

		dates = pd.date_range(start=start_date, end=end_date, freq="D")
		dates = dates.astype(str)

		batches = max(int((end_date - start_date).days / 15), 1)
		batch_size = int(len(dates) / batches)
		batches = [
			dates[i - batch_size : i]
			for i in range(batch_size, len(dates) + batch_size, batch_size)
		]
		batches = [(batch[0], batch[-1]) for batch in batches]
		for i in range(len(batches) - 1):
			batches[i] = (batches[i][0], batches[i+1][0])

		self.logger.info(f"SPLITS,BATCHES,{len(batches)},")

		###########################################################################################

		for i, batch in enumerate(batches):

			for ticker, factor in zip(tickers, factors):

				self.logger.info(f"SPLITS,BATCH,{' '.join(batch)},{ticker} {factor}")

				ohlc = SPLIT_OHLC_UPDATE.format(modifier=modifier,
												factor=factor,
												d1=batch[0],
												d2=batch[1],
												ticker=ticker)
				self.execute(ohlc)
				self.logger.info(f"SPLITS,BATCH,OHLC,COMPLETED")

				options = SPLIT_OPTIONS_UPDATE.format(modifier=modifier,
													  factor=factor,
													  d1=batch[0],
													  d2=batch[1],
													  ticker=ticker)
				self.execute(options)
				self.logger.info(f"SPLITS,BATCH,OPTIONS,COMPLETED")

				option_ids = SPLIT_OPTION_ID_UPDATE.format(modifier=modifier,
														   factor=factor,
														   d1=batch[0],
														   d2=batch[1],
														   ticker=ticker)
				self.execute(option_ids)
				
				self.logger.info(f"SPLITS,BATCH,OPTION ID,COMPLETED")
