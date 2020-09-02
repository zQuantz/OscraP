import sqlalchemy as sql
import sys, os

class Connector:

	def __init__(self, CONFIG):

		self.db_address = CONFIG['db_address'].replace("finance", "test")
		self.engine = sql.create_engine(self.db_address,
								   		pool_size=10,
								   		max_overflow=0,
								   		pool_recycle=3600)
		self.max_tries = 10

	def read(self, query):

		tries = 0
		while tries < self.max_tries:

			try:

				with self.engine.connect() as conn:
					data = pd.read_sql(query, conn)

				return data

			except Exception as e:

				print(e)

			tries += 1

		if tries >= self.max_tries:
			raise Exception("Too Many SQL Errors.")

	def write(self, table, df):

		tries = 0
		while tries < self.max_tries:

			try:

				with self.engine.connect() as conn:
					df.to_sql(table,
							  conn,
							  if_exists='append',
							  index=False,
							  chunksize=100_000)
				break

			except Exception as e:

				print(e)

			tries += 1

		if tries >= self.max_tries:
			raise Exception("Too Many SQL Errors.")

	def execute(self, statement):

		tries = 0
		while tries < self.max_tries:

			try:

				with self.engine.connect() as conn:
					conn.execute(statement)
				break

			except Exception as e:

				print(e)

			tries += 1

		if tries >= self.max_tries:
			raise Exception("Too Many SQL Errors.")
