from const import CONFIG, DIR
import sys, os

sys.path.append(f"{DIR}/../utils")
from send_email import send_email

###################################################################################################

DATE = CONFIG['date']

###################################################################################################

def report(df):

	with open(f"{DIR}/instrument_data/{DATE}/log.log", "w") as log_file:
		for filename in os.listdir(f"{DIR}/instrument_data/{DATE}"):
			if '.csv' in filename: continue
			if filename == 'log.log': continue
			with open(f"{DIR}/instrument_data/{DATE}/"+filename) as file:
				log_file.write(file.read())
			os.unlink(f"{DIR}/instrument_data/{DATE}/"+filename)

	attachments = [
		{
			"ContentType" : "plain/text",
			"filename" : "log.log",
			"filepath" : f"{DIR}/instrument_data/{DATE}"
		}
	]

	send_email(config=CONFIG, subject="Instrument Table Summary", body=df.to_html(), attachments=attachments)
