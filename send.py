from const import DIR, date_today, option_cols, option_new_cols, equity_cols, equity_new_cols
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import sqlalchemy as sql
import pandas as pd
import smtplib, ssl
import numpy as np

def send_to_database():

	import os

	engine = sql.create_engine("mysql://compour9_admin:cg123@74.220.219.153:3306/compour9_finance")
	conn = engine.connect()

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

		os = df[option_cols]
		os.columns = option_new_cols
		options.append(os)

		es = df[equity_cols]
		es.columns = equity_new_cols
		equities.append(es.iloc[:1, :])

		print("Processed:", ticker, len(options), len(equities))

	if len(options) == 0:
		return

	options = pd.concat(options)
	options.to_sql(name='options', con=conn, if_exists='append', index=False, chunksize=1000)

	equities = pd.concat(equities)
	equities.to_sql(name='equities', con=conn, if_exists='append', index=False, chunksize=1000)

	conn.close()

def send_scraping_report(successful, failures):

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