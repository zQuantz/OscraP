from const import CONFIG, DIR

from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import smtplib, ssl
import os

DATE = CONFIG['date']

def email_ticker_table(df):

	sender_email = "zqretrace@gmail.com"
	receiver_email = "zqretrace@gmail.com, zach.barillaro@gmail.com, mp0941745@gmail.com, josephfalvo@outlook.com, lucasmduarte17@gmail.com"
	receiver_email_list = ["zqretrace@gmail.com", "zach.barillaro@gmail.com", "mp0941745@gmail.com", "josephfalvo@outlook.com", "lucasmduarte17@gmail.com"]
	password = "Street1011"

	message = MIMEMultipart("alternative")
	message["Subject"] = "Instrument Table Summary"
	message["From"] = sender_email
	message["To"] = receiver_email

	with open(f"{DIR}/instrument_data/{DATE}/log.log", "w") as log_file:
		for filename in os.listdir(f"{DIR}/instrument_data/{DATE}"):
			if '.csv' in filename:continue
			if filename == 'log.log': continue
			with open(f"{DIR}/instrument_data/{DATE}/"+filename) as file:
				log_file.write(file.read())
			os.unlink(f"{DIR}/instrument_data/{DATE}/"+filename)

	filename = f"{DIR}/instrument_data/{DATE}/log.log"
	with open(filename, 'r') as file:
		attachment = MIMEText(file.read())
	attachment.add_header('Content-Disposition', 'attachment', filename=filename)
	message.attach(attachment)

	message.attach(MIMEText(df.to_html(), "html"))
	context = ssl.create_default_context()
	with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
		server.login(sender_email, password)
		server.sendmail(
			sender_email, receiver_email_list, message.as_string()
	)
