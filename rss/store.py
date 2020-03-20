from datetime import datetime, timedelta
from google.cloud import storage
from const import DIR
import pandas as pd
import sys, os
import shutil

def move_files():

	folder_name = datetime.now() - timedelta(days = 1)
	folder_name = folder_name.strftime('%Y-%m-%d')
	folder_name = f'{DIR}/news_data_backup/{folder_name}'
	os.mkdir(folder_name)

	files = os.listdir(f"{DIR}/news_data")
	files.remove('.gitignore')
	
	for file in files:
		os.rename(f'{DIR}/news_data/{file}', f"{folder_name}/{file}")

	shutil.make_archive(folder_name, "zip", f"{folder_name}/..")
	return folder_name + '.zip'

def send_to_bucket(zip_folder):

    storage_client = storage.Client()
    bucket = storage_client.bucket("oscrap_storage")

    destination_name = zip_folder.split('/')[-1]
    blob = bucket.blob(destination_name)
    blob.upload_from_filename(zip_folder)

if __name__ == '__main__':

	zip_folder = move_files()
	send_to_bucket(zip_folder)
