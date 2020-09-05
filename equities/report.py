from const import DIR, DATE, CONFIG, logger
import pandas as pd
import shutil
import sys
import os

sys.path.append(f"{DIR}/../utils")
from send_email import send_email

###################################################################################################

def report(title_modifier, successful, failures, faults_summary, db_flags, db_stats):

	def write_str(product):

		if len(faults_summary['options']) > 0:

			df = pd.DataFrame(faults_summary[product]).T
			df.columns = ['Lower Bound', 'First Count', 'Second Count', 'Delta']
			return df.to_html()

		return ""

	options_fault_str = write_str("options")
	analysis_faults_str = write_str("analysis")
	keystats_faults_str = write_str("keystats")

	ohlc_faults_str = ""
	if len(faults_summary['ohlc']) > 0:

		df = pd.DataFrame(faults_summary['ohlc']).T
		df.columns = ['Status', 'New Status']
		ohlc_faults_str = df.to_html()

	###############################################################################################

	total = successful['options'] + failures['options']

	starts = db_stats[0][0]
	ends = db_stats[-1][1]

	counts = list(zip(starts, ends))
	analysis_counts, keystats_counts, ohlc_counts, options_counts = counts

	adds = [
		[x2 - x1 for x1, x2 in zip(*batch)]
		for batch in db_stats
	]
	
	df = pd.DataFrame(adds)
	df.columns = ['Analysis Adds', 'Key Stats Adds', 'OHLC Adds', 'Option Adds']
	df = df.set_index([[f"Batch #{i+1}" for i in range(len(df))]])

	db_flag_names = ["Failure", "Successful"]
	db_flags = [db_flag_names[flag] for flag in db_flags]
	df['Indexing Flags'] = db_flags
	
	ingestion_str = df.to_html()

	###############################################################################################

	body = f"""
		Ingestion Summary<br>
		{ingestion_str}<br>
		<br>

		Options Summary<br>
		Successful Tickers: {successful['options']}, {round(successful['options'] / total * 100, 2)}%<br>
		Failed Tickers: {failures['options']}, {round(failures['options'] / total * 100, 2)}%<br>
		Starting Row Count: {options_counts[0]}<br>
		Ending Row Count: {options_counts[1]}<br>
		New Rows Added: {options_counts[1] - options_counts[0]}<br>
		<br>

		OHLC Summary<br>
		Successful Tickers: {successful['ohlc']}, {round(successful['ohlc'] / total * 100, 2)}%<br>
		Failed Tickers: {failures['ohlc']}, {round(failures['ohlc'] / total * 100, 2)}%<br>
		Starting Row Count: {ohlc_counts[0]}<br>
		Ending Row Count: {ohlc_counts[1]}<br>
		New Rows Added: {ohlc_counts[1] - ohlc_counts[0]}<br>
		<br>

		Analysis Summary<br>
		Successful Tickers: {successful['analysis']}, {round(successful['analysis'] / total * 100, 2)}%<br>
		Failed Tickers: {failures['analysis']}, {round(failures['analysis'] / total * 100, 2)}%<br>
		Starting Row Count: {analysis_counts[0]}<br>
		Ending Row Count: {analysis_counts[1]}<br>
		New Rows Added: {analysis_counts[1] - analysis_counts[0]}<br>
		<br>

		Key Statistics Summary<br>
		Successful Tickers: {successful['keystats']}, {round(successful['keystats'] / total * 100, 2)}%<br>
		Failed Tickers: {failures['keystats']}, {round(failures['keystats'] / total * 100, 2)}%<br>
		Starting Row Count: {keystats_counts[0]}<br>
		Ending Row Count: {keystats_counts[1]}<br>
		New Rows Added: {keystats_counts[1] - keystats_counts[0]}<br>
		<br>

		Options Fault Summary<br>
		{options_fault_str}<br>
		<br>

		OHLC Fault Summary<br>
		{ohlc_faults_str}<br>
		<br>

		Analysis Fault Summary<br>
		{analysis_faults_str}<br>
		<br>

		Key Statistics Fault Summary<br>
		{keystats_faults_str}<br>
		<br>

		See attached for the log file and the collected data.<br>
	"""

	###############################################################################################

	attachments = []
	
	os.system(f"bash {DIR}/utils/truncate_log_file.sh")
	filename = f'{DIR}/log.log'
	attachments.append({
		"ContentType" : "plain/text",
		"filename" : f"log.log",
		"filepath" : f"{DIR}"
	})

	###############################################################################################

	send_email(CONFIG, f"{title_modifier} Web Scraping Summary", body, attachments, logger)