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

	def init_date_series(self, modifier=""):

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
		self.execute(INIT_DATE_SERIES.format(modifier=modifier, date=self.date))
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

		return tuple(df.ticker.unique())

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

	def register_splits(self, columns, modifier=""):

		self.logger.info(f"REGISTER SPLITS,INITIATED,{self.date},")

		start_date = datetime.strptime("2019-11-01", "%Y-%m-%d")
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

		df = self.read("""
				SELECT
					*
				FROM
					stocksplits{modifier}
				WHERE
					ex_date = "{date}"
			""".format(modifier=modifier, date=self.date))

		self.logger.info(f"REGISTER SPLITS,TICKERS,{len(df)},")

		###########################################################################################

		processes = []
		for ticker, split_factor, ex_date in zip(df.ticker, df.split_factor, df.ex_date):
			for batch in batches:
				for i, process in enumerate(SPLIT_PROCEDURES):
					processes.append([
							ticker,
							ex_date,
							i,
							process,
							batch[0],
							batch[1],
							split_factor,
							None
						])

		self.logger.info(f"REGISTER SPLITS,PROCESSES,{len(processes)},")
		processes = pd.DataFrame(processes, columns = columns)
		self.write(f"stocksplitstatus{modifier}", processes)

		###########################################################################################

	def adjust_splits(self, modifier="", retake=False):

		self.logger.info(f"ADJUST SPLITS,INITIATED,{self.date},")

		df = self.read("""
				SELECT
					ticker,
					ex_date,
					procedure_name,
					d1,
					d2,
					split_factor
				FROM
					stocksplitstatus{modifier}
				WHERE
					ex_date = "{date}"
				AND processed_timestamp IS NULL
				ORDER BY
					ticker,
					ex_date,
					procedure_order ASC,
					d1 ASC,
					d2 ASC;
			""".format(modifier=modifier, date=self.date))

		if not retake:

			self.execute("""
					DELETE FROM
						tickeroids{modifier}
					WHERE
						ticker IN (
							SELECT
								DISTINCT ticker
							FROM
								stocksplitstatus{modifier}
							WHERE
								ex_date = "{date}"
						)
				""".format(modifier=modifier, date=self.date))

		###########################################################################################

		for row in df.values:

			ticker, ex_date, procedure_name, d1, d2, split_factor = row
			self.logger.info(f"SPLITS,PROCEDURE,{procedure_name} - {d1} {d2},{ticker} {split_factor}")
			procedure = SPLIT_PROCEDURES[procedure_name]
			procedure = procedure.format(modifier=modifier,
										 factor=split_factor,
										 d1=d1,
										 d2=d2,
										 ticker=ticker)
			self.execute(procedure)
			self.logger.info(f"SPLITS,PROCEDURE,{procedure_name},COMPLETED")

			self.logger.info(f"SPLITS,PROCEDURE,Updating Status,")
			procedure = UPDATE_SPLIT_STATUS.format(modifier=modifier,
												   ticker=ticker,
												   d1=d1,
												   d2=d2,
												   ex_date=ex_date,
												   procedure_name=procedure_name)
			self.execute(procedure)
			self.logger.info(f"SPLITS,PROCEDURE,{procedure_name},UPDATED")

		###########################################################################################

		self.execute("""
				UPDATE
					stocksplits{modifier} AS sp
				INNER JOIN
					(
						SELECT
							ticker,
							ex_date
						FROM
							stocksplitstatus{modifier}
						WHERE
							ex_date = "{date}"
						GROUP BY
							ticker,
							ex_date
						HAVING
							COUNT(*) - SUM(IF(processed_timestamp IS NULL, 0, 1)) = 0
					) as t1
					USING(ticker, ex_date)
				SET
					sp.processed_timestamp = CURRENT_TIMESTAMP();
			""".format(modifier=modifier, date=self.date))


