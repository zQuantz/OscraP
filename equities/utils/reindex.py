import sys
sys.path.append("..")

from const import DIR, CONFIG

from argparse import ArgumentParser
import sqlalchemy as sql
import pandas as pd
import sys, os

def all():

	pass

def one(folder, database):

	engine = "mysql://{username}:{password}@{host}:{port}/{database}"
	engine = engine.format(username=CONFIG['db_user'], password=CONFIG['db_password'],
						   host=CONFIG['db_ip'], port=CONFIG['db_port'],
						   database=database)
	
	engine = sql.create_engine(engine)

	for file in os.listdir(folder):

		table_name = file.split('.')[0]
		df = pd.read_csv(f"{folder}/{file}")
		file_date = df.date_current.values[0]

		max_tries = 5
		tries = 0

		print(file, table_name, file_date, os.path.basename(folder))

		while tries < max_tries:
		
			try:
				
				delete_query = f"""
					DELETE FROM
						{table_name}
					WHERE
						date_current = "{os.path.basename(folder)}"
				"""
				
				conn = engine.connect()
				result = conn.execute(delete_query)
				print(result)
				conn.close()
				
				delete_query = f"""
					DELETE FROM
						{table_name}
					WHERE
						date_current = "{file_date}"
				"""
				
				conn = engine.connect()
				result = conn.execute(delete_query)
				print(result)
				conn.close()
				
				conn = engine.connect()
				result = df.to_sql(table_name, conn, if_exists='append', index=False, chunksize=10_000)
				conn.close()
				
				print("Success", tries, result)
				
				break
								
			except Exception as e:
				
				print(e)
				
			tries += 1
			
		print()

if __name__ == '__main__':

	argparser = ArgumentParser()
	argparser.add_argument("method")
	argparser.add_argument("folder")
	argparser.add_argument("database")
	args = argparser.parse_args()

	if args.method == "one":
		one(args.folder, args.database)
	elif args.method == "all":
		all()
