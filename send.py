from const import DIR, date_today, logger

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

	engine = sql.create_engine("mysql://compour9_admin:cg123@74.220.219.153:3306/compour9_test")

	with engine.connect() as conn:

		options_pre = conn.execute("SELECT COUNT(*) FROM options;").fetchone()[0]
		equities_pre = conn.execute("SELECT COUNT(*) FROM equities;").fetchone()[0]
		analysis_pre = conn.execute("SELECT COUNT(*) FROM analysis;").fetchone()[0]
		key_stats_pre = conn.execute("SELECT COUNT(*) FROM key_stats;").fetchone()[0]

		options = []
		equities = []
		analysis = []
		key_stats = []

		for file in os.listdir(f'{DIR}/financial_data/{date_today}/options'):

			ticker = file.split('_')[0]
			df = pd.read_csv(f'{DIR}/financial_data/{date_today}/options/{file}')
			df['ticker'] = ticker
			df['option_id'] = (df.ticker + ' ' + df.expiration_date + ' ' + df.option_type
							  + np.round(df.strike_price, 2).astype(str))
			options.append(df)

		for file in os.listdir(f'{DIR}/financial_data/{date_today}/equities'):

			ticker = file.split('_')[0]
			df = pd.read_csv(f'{DIR}/financial_data/{date_today}/equities/{file}')
			df['ticker'] = ticker

			equities.append(df.iloc[:1, :])

		for file in os.listdir(f'{DIR}/financial_data/{date_today}/analysis'):

			ticker = file.split('_')[0]
			df = pd.read_csv(f'{DIR}/financial_data/{date_today}/analysis/{file}')
			df['ticker'] = ticker
			df['date_current'] = date_today
			analysis.append(df)

		for file in os.listdir(f'{DIR}/financial_data/{date_today}/key_stats'):

			ticker = file.split('_')[0]
			df = pd.read_csv(f'{DIR}/financial_data/{date_today}/key_stats/{file}')
			df['ticker'] = ticker
			df['date_current'] = date_today
			key_stats.append(df)

		options = pd.concat(options)
		options.to_sql(name='options', con=conn, if_exists='append', index=False, chunksize=10_000)

		equities = pd.concat(equities)
		equities.to_sql(name='equities', con=conn, if_exists='append', index=False, chunksize=10_000)

		analysis = pd.concat(analysis)
		analysis.to_sql(name='analysis', con=conn, if_exists='append', index=False, chunksize=10_000)

		key_stats = pd.concat(key_stats)
		key_stats.to_sql(name='key_stats', con=conn, if_exists='append', index=False, chunksize=10_000)

		options_post = conn.execute("SELECT COUNT(*) FROM options;").fetchone()[0]
		equities_post = conn.execute("SELECT COUNT(*) FROM equities;").fetchone()[0]
		analysis_post = conn.execute("SELECT COUNT(*) FROM analysis;").fetchone()[0]
		key_stats_post = conn.execute("SELECT COUNT(*) FROM key_stats;").fetchone()[0]

	return [(options_pre, options_post), (equities_pre, equities_post), (analysis_pre, analysis_post), (key_stats_pre, key_stats_post)]

def send_scraping_report(successful, failures, unhealthy_tickers, db_flag, db_stats, indexing_faults):

	sender_email = "zqretrace@gmail.com"
	receiver_email = "zqretrace@gmail.com, zach.barillaro@gmail.com, mp0941745@gmail.com, josephfalvo@outlook.com, lucasmduarte17@gmail.com"
	receiver_email_list = ["zqretrace@gmail.com", "zach.barillaro@gmail.com", "mp0941745@gmail.com", "josephfalvo@outlook.com", "lucasmduarte17@gmail.com"]
	password = "Street1011"

	message = MIMEMultipart("alternative")
	message["Subject"] = "Web Scraping Summary"
	message["From"] = sender_email
	message["To"] = receiver_email

	if len(unhealthy_tickers) > 0:

		unhealthy_tickers_str = """
		Quantile (25%), First Run Count, Second Run Count, Delta\n
		"""

		for ticker in unhealthy_tickers:
			
			q = unhealthy_tickers[ticker]['quantile']
			o = unhealthy_tickers[ticker]['options']
			no = unhealthy_tickers[ticker]['new_options']
			d = no - o

			unhealthy_tickers_str += f"""
			{q}, {o}, {no}, {d} \n
		"""

	else:

		unhealthy_tickers_str = ""

	options_counts, equities_counts, analysis_counts, key_stats_counts = db_stats
	total = successful['options'] + failures['options']

	text = f"""
		Ingestion Summary
		Database Ingestion: {["Failure", "Success"][db_flag]}
		Total Indexing Attempts: {indexing_faults + 1}

		Options Summary
		Successful Tickers: {successful['options']}, {np.round(successful['options'] / total * 100, 2)}%
		Failed Tickers: {failures['options']}, {np.round(failures['options'] / total * 100, 2)}%
		Starting Row Count: {options_counts[0]}
		Ending Row Count: {options_counts[1]}
		New Rows Added: {options_counts[1] - options_counts[0]}

		Unhealthy Summary
		{unhealthy_tickers_str}

		Equities Summary
		Successful Tickers: {successful['equities']}, {np.round(successful['equities'] / total * 100, 2)}%
		Failed Tickers: {failures['equities']}, {np.round(failures['equities'] / total * 100, 2)}%
		Starting Row Count: {equities_counts[0]}
		Ending Row Count: {equities_counts[1]}
		New Rows Added: {equities_counts[1] - equities_counts[0]}

		Equities Summary
		Successful Tickers: {successful['analysis']}, {np.round(successful['analysis'] / total * 100, 2)}%
		Failed Tickers: {failures['analysis']}, {np.round(failures['analysis'] / total * 100, 2)}%
		Starting Row Count: {analysis_counts[0]}
		Ending Row Count: {analysis_counts[1]}
		New Rows Added: {analysis_counts[1] - analysis_counts[0]}

		Key Statistics Summary
		Successful Tickers: {successful['key_stats']}, {np.round(successful['key_stats'] / total * 100, 2)}%
		Failed Tickers: {failures['key_stats']}, {np.round(failures['key_stats'] / total * 100, 2)}%
		Starting Row Count: {key_stats_counts[0]}
		Ending Row Count: {key_stats_counts[1]}
		New Rows Added: {key_stats_counts[1] - key_stats_counts[0]}

		See attached for a breakdown of the tickers and file sizes.
	"""
	message.attach(MIMEText(text, "plain"))

	filename = f'{DIR}/financial_data/{date_today}.zip'
	with open(filename, 'rb') as file:
		msg = MIMEBase('application', 'zip')
		msg.set_payload(file.read())
	encoders.encode_base64(msg)
	msg.add_header('Content-Disposition', 'attachment', filename=filename)           
	message.attach(msg)

	os.system(f"bash {DIR}/utils/truncate_log_file.sh")
	filename = f'{DIR}/log.log'
	with open(filename, 'r') as file:
		attachment = MIMEText(file.read())
	attachment.add_header('Content-Disposition', 'attachment', filename=filename)           
	message.attach(attachment)

	context = ssl.create_default_context()
	with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
		server.login(sender_email, password)
		server.sendmail(
			sender_email, receiver_email_list[:2], message.as_string()
	)
