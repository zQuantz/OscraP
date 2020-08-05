from itertools import product
import sqlalchemy as sql
from const import t_map
import pandas as pd
import numpy as np
import sys, os
import json

with open("../config.json", "r") as file:
    CONFIG = json.loads(file.read())

engine = sql.create_engine(CONFIG['db_address'].replace("compour9_test", "compour9_finance"))

def get_rate(vals):

    date = vals.date_current
    t = vals.time_to_expiry
    
    r_map = rate_maps[date]
        
    if t >= 30:
        return r_map[-1]

    b1 = t_map <= t
    b2 = t_map > t

    r1 = r_map[b1][-1]
    r2 = r_map[b2][0]

    t1 = t_map[b1][-1]
    t2 = t_map[b2][0]

    interpolated_rate = (t - t1) / (t2 - t1)
    interpolated_rate *= (r2 - r1)

    return interpolated_rate + r1

if __name__ == '__main__':

	rate_query = """

		SELECT
			*
		FROM
			rates
		WHERE
			date_current >= "2019-01-01"
		ORDER BY
			date_current ASC
	"""

	print("Getting Rates")
	conn = engine.connect()
	rates = pd.read_sql(rate_query, conn)
	conn.close()
	print(rates.head())

	rates.date_current = rates.date_current.astype(str)
	rates = rates.set_index("date_current")

	print("Creating Rate Map")
	rate_maps = {}
	for idx, row in zip(rates.index, rates.values):
	    rate_maps[idx] = np.array([0] + row.tolist())
	    rate_maps[idx] /= 100

	print("Mapping Rates")
	tte = np.arange(0, 365 * 10 + 1).astype(int)
	df = pd.DataFrame(product(rates.index, tte), columns = ['date_current', 'time_to_expiry'])
	df['rate'] = df.apply(get_rate, axis=1)
	print(df.head())

	print("Dropping, Creating and Indexing")
	conn = engine.connect()
	
	try:
		conn.execute("DROP TABLE ratemap")
	except Exception as e:
		print("Table not found.")

	conn.execute("CREATE TABLE ratemap (date_current date, time_to_expiry SMALLINT UNSIGNED, rate DOUBLE, PRIMARY KEY (date_current, time_to_expiry))")
	df.to_sql(name='ratemap', con=conn, if_exists='append', index=False, chunksize=100_000)

	conn.close()