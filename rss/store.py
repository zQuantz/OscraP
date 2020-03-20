from datetime import datetime, timedelta
from const import DIR
import pandas as pd
import sys, os
import shutil

def move_files():

	folder_name = datetime.now() - timedelta(days = 1)
	folder_name = folder_name.strftime('%Y-%m-%d')
	folder_name = f'{DIR}/news_data_backup/{folder_name}'
	os.mkdir(folder_name)

	for file in os.listdir(f"{DIR}/news_data"):
		os.rename(f'{DIR}/news_data/{file}', f"{folder_name}/{file}")

	shutil.make_archive(folder_name, "zip", f"{folder_name}/..")

if __name__ == '__main__':

	move_files()
