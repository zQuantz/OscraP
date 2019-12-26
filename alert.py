from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from const import date_today, DIR
import numpy as np
import smtplib, ssl

def send_scraping_report(successful, failures):

	####################################
	## EMAIL SETUP
	####################################

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

	## Add text and html options
	part1 = MIMEText(text, "plain")
	message.attach(part1)

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

	# Create secure connection with server and send email
	context = ssl.create_default_context()
	with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
	    server.login(sender_email, password)
	    server.sendmail(
	        sender_email, receiver_email_list, message.as_string()
	    )
