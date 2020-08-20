from google.cloud import storage
from const import DIR, CONFIG
import tarfile as tar
import sys, os
import json

###################################################################################################

BUCKET = storage.Client().bucket(CONFIG["gcp_bucket_name"])

###################################################################################################

if __name__ == '__main__':

	os.mkdir(f"{DIR}/data/old")
	os.mkdir(f"{DIR}/data/new")
	os.mkdir(f"{DIR}/data/tar")
	os.mkdir(f"{DIR}/data/tar/old")
	os.mkdir(f"{DIR}/data/tar/new")

	FOLDERS = ["equities", "rates", "instruments", "rss"]
	for folder in FOLDERS:
		os.mkdir(f"{DIR}/data/old/{folder}")
		os.mkdir(f"{DIR}/data/tar/old/{folder}")
		if folder == "rates":
			folder = "treasuryrates"
		os.mkdir(f"{DIR}/data/new/{folder}")
		os.mkdir(f"{DIR}/data/tar/new/{folder}")

	for blob in BUCKET.list_blobs():

		if "/" not in blob.name:
			continue

		folder, filename = blob.name.split("/")
		filedate = filename.split(".")[0]

		if folder not in FOLDERS:
			continue

		modifier = ""
		if folder == "equities":
			modifier = f"/{filedate}/"
			os.mkdir(f"{DIR}/data/old/{folder}/{filedate}")
			os.mkdir(f"{DIR}/data/new/{folder}/{filedate}")

		print("Downloading:", folder, filename)
		blob.download_to_filename(f"{DIR}/data/tar/old/{folder}/{filename}")
		with tar.open(f"{DIR}/data/tar/old/{folder}/{filename}", "r:xz") as tar_file:
			tar_file.extractall(path=f"{DIR}/data/old/{folder}{modifier}")