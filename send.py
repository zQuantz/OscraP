from const import DIR, date_today, option_cols, option_new_cols, equity_cols, equity_new_cols, logger
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import sqlalchemy as sql
import pandas as pd
import smtplib, ssl
import numpy as np
import os

def send_to_database():

	logger.info("Sending data to SQL.")

	engine = sql.create_engine("mysql://compour9_admin:cg123@74.220.219.153:3306/compour9_finance")
	conn = engine.connect()

	with engine.connect() as conn:

		pre = conn.execute("SELECT COUNT(*) FROM options;").fetchone()[0]

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

		post = conn.execute("SELECT COUNT(*) FROM options;").fetchone()[0]

	return (pre, post)

def send_scraping_report(successful, failures, db_flag, db_counts, indexing_faults):

	sender_email = "zqretrace@gmail.com"
	receiver_email = "zqretrace@gmail.com, zach.barillaro@gmail.com, mp0941745@gmail.com, josephfalvo@outlook.com, lucasmduarte17@gmail.com"
	receiver_email_list = ["zqretrace@gmail.com", "zach.barillaro@gmail.com", "mp0941745@gmail.com", "josephfalvo@outlook.com", "lucasmduarte17@gmail.com"]
	password = "Street1011"

	message = MIMEMultipart("alternative")
	message["Subject"] = "Web Scraping Summary"
	message["From"] = sender_email
	message["To"] = receiver_email

	ls = len(successful)
	lf = len(failures)
	total = ls + lf
	text = f"""
		Summary
		Successful Tickers: {ls} , {np.round((ls / total) * 100, 2)}%
		Failed Tickers: {lf} , {np.round((lf / total) * 100, 2)}%

		Database Ingestion: {["Failure", "Success"][db_flag]}
		Starting Row Count: {db_counts[0]}
		Ending Row Count: {db_counts[1]}
		New Rows Added: {db_counts[1] - db_counts[0]}
		Total Indexing Attempts: {indexing_faults + 1}

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

	context = ssl.create_default_context()
	with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
	    server.login(sender_email, password)
	    server.sendmail(
	        sender_email, receiver_email_list, message.as_string()
	    )