from const import DIR, date_today, option_cols, option_new_cols, equity_cols, equity_new_cols, logger

from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from datetime import datetime, timedelta
import sqlalchemy as sql
import pandas as pd
import smtplib, ssl
import numpy as np
import os

def send_to_database():

	def binarize(x):
	    q = np.quantile(x, 0.25)
	    return not (x.values[-1] >= q)[0]

	logger.info("Sending data to SQL.")
	engine = sql.create_engine("mysql://compour9_admin:cg123@74.220.219.153:3306/compour9_finance")

	with engine.connect() as conn:

		options_pre = conn.execute("SELECT COUNT(*) FROM options;").fetchone()[0]
		equities_pre = conn.execute("SELECT COUNT(*) FROM equities;").fetchone()[0]

		options = []
		equities = []

		for file in os.listdir(f'{DIR}/options_data/{date_today}'):

			if '.txt' in file:
				continue

			ticker = file.split('_')[0]
			df = pd.read_csv(f'{DIR}/options_data/{date_today}/{file}')
			df['Ticker'] = ticker
			df['OptionID'] = (df.Ticker + ' ' + df.ExpirationDate + ' ' + df.OptionType
							  + np.round(df.StrikePrice, 2).astype(str))

			opts = df[option_cols]
			opts.columns = option_new_cols
			options.append(opts)

			eqts = df[equity_cols]
			eqts.columns = equity_new_cols
			equities.append(eqts.iloc[:1, :])

			logger.info(f"Processing {ticker} for database ingestion.")

		if len(options) == 0:
			return

		options = pd.concat(options)
		options.to_sql(name='options', con=conn, if_exists='append', index=False, chunksize=10_000)

		equities = pd.concat(equities)
		equities.to_sql(name='equities', con=conn, if_exists='append', index=False, chunksize=10_000)

		options_post = conn.execute("SELECT COUNT(*) FROM options;").fetchone()[0]
		equities_post = conn.execute("SELECT COUNT(*) FROM equities;").fetchone()[0]

		tickers = [
			"AAPL", "TSLA", "MSFT", "F", "XOP", "SPY", "JKS", "V", "DOL.TO", "WEED.TO"
		]
		dt = datetime.now() - timedelta(days=60)
		query = sql.text(f"""
			SELECT
				ticker, date_current
			FROM
				options
			WHERE
				ticker in :tickers
			AND date_current >= {dt.strftime("%Y-%m-%d")}
			"""
		)
		query = query.bindparams(tickers=tickers)
		quantile_flags = pd.read_sql(query, conn)
		quantile_flags = quantile_flags.groupby(["ticker"]).apply(lambda x: 
				binarize(x.groupby('date_current').count())
			)

	return [(options_pre, options_post), (equities_pre, equities_post), quantile_flags]

def send_scraping_report(successful, failures, db_flag, db_stats, indexing_faults):

	sender_email = "zqretrace@gmail.com"
	receiver_email = "zqretrace@gmail.com, zach.barillaro@gmail.com, mp0941745@gmail.com, josephfalvo@outlook.com, lucasmduarte17@gmail.com"
	receiver_email_list = ["zqretrace@gmail.com", "zach.barillaro@gmail.com", "mp0941745@gmail.com", "josephfalvo@outlook.com", "lucasmduarte17@gmail.com"]
	password = "Street1011"

	message = MIMEMultipart("alternative")
	message["Subject"] = "Web Scraping Summary"
	message["From"] = sender_email
	message["To"] = receiver_email

	options_counts, equities_counts, quantile_flags = db_stats

	ls = len(successful)
	lf = len(failures)
	total = ls + lf
	text = f"""
		Summary
		Successful Tickers: {ls} , {np.round((ls / total) * 100, 2)}%
		Failed Tickers: {lf} , {np.round((lf / total) * 100, 2)}%

		Ingestion Summary
		Database Ingestion: {["Failure", "Success"][db_flag]}
		Total Indexing Attempts: {indexing_faults + 1}

		Options Summary
		Unhealthy Tickers: {', '.join(quantile_flags[quantile_flags].index)}
		Starting Row Count: {options_counts[0]}
		Ending Row Count: {options_counts[1]}
		New Rows Added: {options_counts[1] - options_counts[0]}

		Equities Summary
		Starting Row Count: {equities_counts[0]}
		Ending Row Count: {equities_counts[1]}
		New Rows Added: {equities_counts[1] - equities_counts[0]}

		See attached for a breakdown of the tickers and file sizes.
	"""

	message.attach(MIMEText(text, "plain"))

	filename = f'{DIR}/options_data/{date_today}/successful_tickers.txt'
	with open(filename, 'r') as file:
		attachment = MIMEText(file.read())
	attachment.add_header('Content-Disposition', 'attachment', filename=filename)      
	message.attach(attachment)

	filename = f'{DIR}/options_data/{date_today}/failed_tickers.txt'
	with open(filename, 'r') as file:
		attachment = MIMEText(file.read())
	attachment.add_header('Content-Disposition', 'attachment', filename=filename)           
	message.attach(attachment)

	filename = f'{DIR}/options_data/{date_today}.zip'
	with open(filename, 'rb') as file:
		msg = MIMEBase('application', 'zip')
		msg.set_payload(file.read())
	encoders.encode_base64(msg)
	msg.add_header('Content-Disposition', 'attachment', filename=filename)           
	message.attach(msg)

	os.system("rm log.log")
	os.system("echo | tail -2000 scraper.log > log.log")

	filename = f'{DIR}/log.log'
	with open(filename, 'r') as file:
		attachment = MIMEText(file.read())
	attachment.add_header('Content-Disposition', 'attachment', filename=filename)           
	message.attach(attachment)

	context = ssl.create_default_context()
	with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
		server.login(sender_email, password)
		server.sendmail(
			sender_email, receiver_email_list, message.as_string()
	)
