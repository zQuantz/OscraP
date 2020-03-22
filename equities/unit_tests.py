from const import DIR, date_today, logger 
from datetime import datetime, timedelta
import sqlalchemy as sql
import pandas as pd
import numpy as np
import sys, os

def check_number_of_options():

	engine = sql.create_engine("mysql://compour9_admin:cg123@74.220.219.153:3306/compour9_finance")

	dt = datetime.now() - timedelta(days=60)
	query = sql.text(f"""
		SELECT
			ticker,
			COUNT(date_current) as count
		FROM
			options
		WHERE
			date_current >= {dt.strftime("%Y-%m-%d")}
		GROUP BY
			ticker, date_current
		ORDER BY
			date_current
		"""
	)
	query = query.bindparams()

	conn = engine.connect()
	df = pd.read_sql(query, conn)
	conn.close()

	quantiles = df.groupby('ticker').apply(lambda x: np.quantile(x['count'].values, 0.25))
	quantiles = quantiles.astype(int).to_dict()

	unhealthy_tickers = {}
	for ticker in quantiles:

		try:
			
			df = pd.read_csv(f"{DIR}/financial_data/{date_today}/options/{ticker}_{date_today}.csv")
			if len(df) <= quantiles[ticker]:
				unhealthy_tickers[ticker] = {
					'quantile' : quantiles[ticker],
					'options' : len(df),
					'new_options' : 0
				}
			logger.warning(f"{ticker},Unit Test - Number of Options,Failure,{e}")

		except Exception as e:
			
			pass
			# logger.warning(f"{ticker},Unit Test - Number of Options,Failure,{e}")


	return unhealthy_tickers
