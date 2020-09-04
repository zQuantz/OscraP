from procedures import DERIVED_PROCEDURES
from threading import Thread
import sqlalchemy as sql
import pandas as pd
import sys, os

class Connector:

	def __init__(self, CONFIG, date):

		self.db = CONFIG['db']
		self.db_address = CONFIG['db_address'].replace("finance", "test")
		self.engine = sql.create_engine(self.db_address,
								   		pool_size=10,
								   		max_overflow=0,
								   		pool_recycle=3600)

		self.max_tries = 10
		self.date = date

	def transact(self, action, *args):

		tries = 0
		while tries < self.max_tries:

			try:

				with self.engine.connect() as conn:
					return action(conn, *args)

			except Exception as e:

				print(e)

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
				    TABLE_ROWS AS count
				FROM
				    information_schema.tables
				WHERE
				    TABLE_SCHEMA = "{db}"
				AND
				    TABLE_NAME in ("optionsBACK", "ohlcBACK", "analysisBACK", "keystatsBACK")
			""".format(db=self.db))

	def init_batch_tickers(self, tickers):

		self.execute("""DELETE FROM batchtickers;""")
		batchtickers = pd.DataFrame(tickers, columns = ['ticker'])
		self.write("batchtickers", batchtickers)

	def get_equity_tickers(self, N_USD, N_CAD):

		df = self.read("""
				SELECT
					*
				FROM
					instrumentsBACK
				WHERE
					market_cap >= {}
				ORDER BY
					market_cap DESC
			""".format(1_000_000))

		usd = df[~df.exchange_code.isin(["TSX"])].iloc[:N_USD, :]
		cad = df[df.exchange_code.isin(["TSX"])].iloc[:N_CAD, :]
		tickers = (usd.ticker.values.tolist() + cad.ticker.values.tolist())

		df = df[df.ticker.isin(tickers)]
		df = df.sort_values('market_cap', ascending=False)

		return tuple(df.ticker)

	def get_data_counts(self, tablename):

		return self.read("""
				SELECT
					ticker,
					count
				FROM
					{}
				WHERE
					date_current >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
				AND
					ticker in (SELECT ticker FROM batchtickers)
				GROUP BY
					ticker,
					date_current
				ORDER BY
					date_current DESC
			""".format(tablename))

	def get_distinct_ohlc_tickers(self):

		return self.read("""
				SELECT
					DISTINCT ticker
				FROM
					ohlcBACK
				WHERE
					date_current >= DATE_SUB(CURDATE(), INTERVAL 60 DAY)
				AND ticker in (SELECT ticker FROM batchtickers)
			""")

	def launch_derived_engine(self, tickers):

		def derived_engine():

			self.execute("""DELETE FROM batchtickers;""")
			self.write("batchtickers", pd.DataFrame(tickers, columns = ['ticker']))
			self.execute(f"""SET @date_current = "2020-07-10";""")

			for name, procedure in DERIVED_PROCEDURES.items():
				print("Executing Procedure:", name)
				self.execute(procedure)

		thread = Thread(target = derived_engine)
		thread.start()
