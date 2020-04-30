from const import DIR, CONFIG, logger
import pandas as pd
import shutil
import sys
import os

sys.path.append(f"{DIR}/../utils")
from send_email import send_email

###################################################################################################

DATE = CONFIG['date']

###################################################################################################

def report(title_modifier, successful, failures, faults_summary, db_flags, db_stats, indexing_faults):

	###############################################################################################

	option_faults_str = ""
	if len(faults_summary['options']) > 0:
		df = pd.DataFrame(faults_summary['options']).T
		df['delta'] = df.new_options - df.options
		df.columns = ['Quantile (25%)', 'First Count', 'Second Count', 'Delta']
		option_faults_str = df.to_html()

	analysis_faults_str = ""
	if len(faults_summary['analysis']) > 0:

		df = pd.DataFrame(faults_summary['analysis']).T
		df['delta'] = df.new_null_percentage - df.null_percentage
		df.columns = ['Quantile (25%)', 'First Null %', 'Second Null %', 'Delta']
		analysis_faults_str = df.to_html()

	key_stats_faults_str = ""
	if len(faults_summary['key_stats']) > 0:
		
		df = pd.DataFrame(faults_summary['key_stats']).T
		df['delta'] = df.new_null_percentage - df.null_percentage
		df.columns = ['Quantile (25%)', 'First Null %', 'Second Null %', 'Delta']
		key_stats_faults_str = df.to_html()

	ohlc_faults_str = ""
	if len(faults_summary['ohlc']) > 0:

		df = pd.DataFrame(faults_summary['ohlc']).T
		df.columns = ['Status', 'New Status']
		ohlc_faults_str = df.to_html()

	###############################################################################################

	total = successful['options'] + failures['options']

	db_flag_names = ["Failure", "Successful"]
	db_flags = [db_flag_names[flag] for flag in db_flags]

	starts = [start[0] for start in db_stats[0]]
	ends = [end[1] for end in db_stats[-1]]

	counts = list(zip(starts, ends))
	options_counts, ohlc_counts, analysis_counts, key_stats_counts = counts

	adds = [
		[item[1] - item[0] for item in batch]
		for batch in db_stats
	]
	
	df = pd.DataFrame(adds)
	df.columns = ['Option Adds', 'OHLC Adds', 'Analysis Adds', 'Key Stats Adds']
	df = df.set_index([[f"Batch #{i+1}" for i in range(len(df))]])

	df['Indexing Flags'] = db_flags
	df['Indexing Faults'] = indexing_faults
	
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

		Options Fault Summary<br>
		{option_faults_str}<br>
		<br>

		OHLC Summary<br>
		Successful Tickers: {successful['ohlc']}, {round(successful['ohlc'] / total * 100, 2)}%<br>
		Failed Tickers: {failures['ohlc']}, {round(failures['ohlc'] / total * 100, 2)}%<br>
		Starting Row Count: {ohlc_counts[0]}<br>
		Ending Row Count: {ohlc_counts[1]}<br>
		New Rows Added: {ohlc_counts[1] - ohlc_counts[0]}<br>
		<br>

		OHLC Fault Summary<br>
		{ohlc_faults_str}<br>
		<br>

		Analysis Summary<br>
		Successful Tickers: {successful['analysis']}, {round(successful['analysis'] / total * 100, 2)}%<br>
		Failed Tickers: {failures['analysis']}, {round(failures['analysis'] / total * 100, 2)}%<br>
		Starting Row Count: {analysis_counts[0]}<br>
		Ending Row Count: {analysis_counts[1]}<br>
		New Rows Added: {analysis_counts[1] - analysis_counts[0]}<br>
		<br>

		Analysis Fault Summary<br>
		{analysis_faults_str}<br>
		<br>

		Key Statistics Summary<br>
		Successful Tickers: {successful['key_stats']}, {round(successful['key_stats'] / total * 100, 2)}%<br>
		Failed Tickers: {failures['key_stats']}, {round(failures['key_stats'] / total * 100, 2)}%<br>
		Starting Row Count: {key_stats_counts[0]}<br>
		Ending Row Count: {key_stats_counts[1]}<br>
		New Rows Added: {key_stats_counts[1] - key_stats_counts[0]}<br>
		<br>

		Key Statistics Fault Summary<br>
		{key_stats_faults_str}<br>
		<br>

		See attached for the log file and the collected data.<br>
	"""

	###############################################################################################

	attachments = []

	shutil.make_archive(f"{DIR}/financial_data/{DATE}", "zip", f"{DIR}/financial_data/{DATE}")
	filename = f'{DIR}/financial_data/{DATE}.zip'
	attachments.append({
		"ContentType" : "application/zip",
		"filename" : f"{DATE}.zip",
		"filepath" : f"{DIR}/financial_data"
	})

	os.system(f"bash {DIR}/utils/truncate_log_file.sh")
	filename = f'{DIR}/log.log'
	attachments.append({
		"ContentType" : "plain/text",
		"filename" : f"log.log",
		"filepath" : f"{DIR}"
	})

	###############################################################################################

	send_email(CONFIG, f"{title_modifier} Web Scraping Summary", body, attachments, logger)