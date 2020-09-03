import sqlalchemy as sql
import pandas as pd
import sys, os

class Connector:

	def __init__(self, CONFIG):

		self.db_address = CONFIG['db_address'].replace("finance", "test")
		self.engine = sql.create_engine(self.db_address,
								   		pool_size=10,
								   		max_overflow=0,
								   		pool_recycle=3600)
		self.max_tries = 10

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

		return self.transact(action, query)

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
