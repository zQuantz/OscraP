from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from const import DIR, date_today
from email import encoders
import pandas as pd
import smtplib, ssl
import numpy as np
import os

def send_scraping_report(successful, failures, unhealthy_tickers, db_flag, db_stats, indexing_faults):

	sender_email = "zqretrace@gmail.com"
	receiver_email = "zqretrace@gmail.com, zach.barillaro@gmail.com, mp0941745@gmail.com, josephfalvo@outlook.com, lucasmduarte17@gmail.com"
	receiver_email_list = ["zqretrace@gmail.com", "zach.barillaro@gmail.com", "mp0941745@gmail.com", "josephfalvo@outlook.com", "lucasmduarte17@gmail.com"]
	password = "Street1011"

	message = MIMEMultipart("alternative")
	message["Subject"] = "Web Scraping Summary"
	message["From"] = sender_email
	message["To"] = receiver_email

	unhealthy_tickers_str = ""
	if len(unhealthy_tickers) > 0:

		df = pd.DataFrame(unhealthy_tickers).T
		df['delta'] = df.new_options - df.options
		df.columns = ['Quantile (25%)', 'First Count', 'Second Count', 'Delta']
		unhealthy_tickers_str = df.to_html()

	options_counts, equities_counts, analysis_counts, key_stats_counts = db_stats
	total = successful['options'] + failures['options']

	text = f"""
		Ingestion Summary<br>
		Database Ingestion: {["Failure", "Success"][db_flag]}<br>
		Total Indexing Attempts: {indexing_faults + 1}<br>
		<br>

		Options Summary<br>
		Successful Tickers: {successful['options']}, {np.round(successful['options'] / total * 100, 2)}%<br>
		Failed Tickers: {failures['options']}, {np.round(failures['options'] / total * 100, 2)}%<br>
		Starting Row Count: {options_counts[0]}<br>
		Ending Row Count: {options_counts[1]}<br>
		New Rows Added: {options_counts[1] - options_counts[0]}<br>
		<br>

		Unhealthy Summary<br>
		{unhealthy_tickers_str}<br>
		<br>

		Equities Summary<br>
		Successful Tickers: {successful['equities']}, {np.round(successful['equities'] / total * 100, 2)}%<br>
		Failed Tickers: {failures['equities']}, {np.round(failures['equities'] / total * 100, 2)}%<br>
		Starting Row Count: {equities_counts[0]}<br>
		Ending Row Count: {equities_counts[1]}<br>
		New Rows Added: {equities_counts[1] - equities_counts[0]}<br>
		<br>

		Equities Summary<br>
		Successful Tickers: {successful['analysis']}, {np.round(successful['analysis'] / total * 100, 2)}%<br>
		Failed Tickers: {failures['analysis']}, {np.round(failures['analysis'] / total * 100, 2)}%<br>
		Starting Row Count: {analysis_counts[0]}<br>
		Ending Row Count: {analysis_counts[1]}<br>
		New Rows Added: {analysis_counts[1] - analysis_counts[0]}<br>
		<br>

		Key Statistics Summary<br>
		Successful Tickers: {successful['key_stats']}, {np.round(successful['key_stats'] / total * 100, 2)}%<br>
		Failed Tickers: {failures['key_stats']}, {np.round(failures['key_stats'] / total * 100, 2)}%<br>
		Starting Row Count: {key_stats_counts[0]}<br>
		Ending Row Count: {key_stats_counts[1]}<br>
		New Rows Added: {key_stats_counts[1] - key_stats_counts[0]}<br>
		<br>

		See attached for a breakdown of the tickers and file sizes.<br>
	"""
	message.attach(MIMEText(text, "html"))

	filename = f'{DIR}/financial_data/{date_today}.zip'
	with open(filename, 'rb') as file:
		attachment = MIMEBase('application', 'zip')
		attachment.set_payload(file.read())
	encoders.encode_base64(attachment)
	attachment.add_header('Content-Disposition', 'attachment', filename=filename)           
	message.attach(attachment)

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
			sender_email, receiver_email_list, message.as_string()
	)
