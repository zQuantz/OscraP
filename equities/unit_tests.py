from const import DIR, date_today, logger 
from datetime import datetime, timedelta
import sqlalchemy as sql
import pandas as pd
import numpy as np
import sys, os

engine = sql.create_engine("mysql://compour9_admin:cg123@74.220.219.153:3306/compour9_finance")

def check_number_of_options():

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

	unhealthy_options = {}
	for ticker in quantiles:

		try:
			
			df = pd.read_csv(f"{DIR}/financial_data/{date_today}/options/{ticker}_{date_today}.csv")
			if len(df) <= quantiles[ticker]:
				unhealthy_options[ticker] = {
					'quantile' : quantiles[ticker],
					'options' : len(df),
					'new_options' : 0
				}

		except FileNotFoundError as file_not_found:

			logger.warning(f"{ticker},Unit Test - Number of Options,Failure,File Not Found")
			unhealthy_options[ticker] = {
					'quantile' : quantiles[ticker],
					'options' : 0,
					'new_options' : 0
				}

		except Exception as e:
			
			logger.warning(f"{ticker},Unit Test - Number of Options,Failure,{e}")

	return unhealthy_options

def check_null_percentage(data):

	label = data.replace('_', ' ').split()
	label = ' '.join(map(str.capitalize, label))
	
	dt = datetime.now() - timedelta(days=60)
	query = sql.text(f"""
		SELECT
			ticker,
			SUM(ISNULL(value)) / COUNT(*) as null_percentage
		FROM
			{data}
		WHERE
			date_current >= {dt.strftime("%Y-%m-%d")}
		GROUP BY
			ticker, date_current
		"""
	)
	query = query.bindparams()

	conn = engine.connect()
	df = pd.read_sql(query, conn)
	conn.close()

	quantiles = df.groupby('ticker').apply(lambda x: x.null_percentage.quantile(0.25).round(4))
	quantiles = quantiles.to_dict()

	unhealthy_tickers = {}
	for ticker in quantiles:

		try:

			df = pd.read_csv(f"{DIR}/financial_data/{date_today}/{data}/{ticker}_{date_today}.csv")
			null_percentage = df.value.isnull().sum() / len(df)
			null_percentage = np.round(null_percentage, 4)

			if null_percentage > quantiles[ticker]:
				unhealthy_tickers[ticker] = {
					'quantile' : quantiles[ticker],
					'null_percentage' : null_percentage,
					'new_null_percentage' : 0
				}

		except FileNotFoundError as file_not_found:

			logger.warning(f"{ticker},Unit Test - {label} Null Percentage,Failure,File Not Found")
			unhealthy_tickers[ticker] = {
					'quantile' : quantiles[ticker],
					'null_percentage' : 0,
					'new_null_percentage' : 0
				}

		except Exception as e:

			logger.warning(f"{ticker},Unit Test - {label} Null Percentage,Failure,{e}")

	return unhealthy_tickers

def check_ohlc():

	dt = datetime.now() - timedelta(days=60)
	query = sql.text(f"""
		SELECT
			DISTINCT(ticker) as tickers
		FROM
			ohlc
		WHERE
			date_current >= {dt.strftime("%Y-%m-%d")}
	""")
	query = query.bindparams()

	conn = engine.connect()
	tickers = pd.read_sql(query, conn).tickers
	conn.close()

	collected_tickers = os.listdir(f"{DIR}/financial_data/{date_today}/ohlc")
	collected_tickers = [ticker.split("_")[0] for ticker in collected_tickers]

	unhealthy_ohlc = {}
	for ticker in tickers:
		if ticker not in collected_tickers:
			unhealthy_ohlc[ticker] = {
				"status" : 0,
				"new_status" : 0
			}

	return unhealthy_ohlc
