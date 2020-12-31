from const import TABLE_NAMES, CONFIG
import sqlalchemy as sql

engine = sql.create_engine(
    sql.engine.url.URL(
        drivername="mysql",
        username=CONFIG['db_user'],
        password=CONFIG['db_password'],
        host=CONFIG['db_ip'],
        port=CONFIG['db_port'],
    ),
    pool_size=3,
	max_overflow=0,
	pool_recycle=299,
	pool_pre_ping=True
)

def initialize():

	insert_tables = [
		"optionscounts",
		"analysiscounts",
		"keystatscounts",
		"treasuryratemap",
		"treasuryrates",
	]



	conn = engine.connect()

	for name in TABLE_NAMES:

		print(name)

		delete_query = """
			DROP TABLE IF EXISTS compour9_test.{name}
		""".format(name = name)
		conn.execute(delete_query)
		print("Delete Query Executed.")
		
		create_query = """
			CREATE TABLE IF NOT EXISTS
				compour9_test.{name}
			LIKE
				compour9_finance.{name}
		""".format(name = name)
		conn.execute(create_query)
		print("Create Query Executed.")

		if name in insert_tables:

			insert_query = """
				INSERT INTO
					compour9_test.{name}
				SELECT
					*
				FROM
					compour9_finance.{name}
				WHERE
					date_current >= "2020-12-15"
				AND date_current < "2020-12-30"
			""".format(name = name)
			conn.execute(insert_query)
			print("Insert Query Executed.")

		elif name == "instruments":

			insert_query = """
				INSERT INTO
					compour9_test.instruments
				SELECT
					*
				FROM
					compour9_finance.instruments
			""".format(name = name)
			conn.execute(insert_query)
			print("Instrument Insert Query Executed.")

		print()

	alter_query = """
		ALTER TABLE
			compour9_test.options
		ADD COLUMN zimplied_volatility FLOAT(4) AFTER implied_volatility
	"""
	conn.execute(alter_query)
	print("Alter Query Executed")

	conn.close()

def delete():

	conn = engine.connect()

	for name in TABLE_NAMES:

		print(name)

		delete_query = """
			DROP TABLE IF EXISTS
				compour9_test.{name}
		"""
		conn.execute(delete_query)

	conn.close()

if __name__ == '__main__':

	initialize()
