from const import CONFIG, DIR, DATE
import pandas as pd
import sys, os

sys.path.append(f"{DIR}/../utils")
from send_email import send_email

###################################################################################################

def report(df):

	os.system(f"bash {DIR}/utils/truncate_log_file.sh")
	attachments = [
		{
			"ContentType" : "plain/text",
			"filename" : "log.log",
			"filepath" : f"{DIR}"
		}
	]

	send_email(config=CONFIG, subject="Instrument Table Summary", body=df.to_html(), attachments=attachments)
