from const import DIR, CONFIG
import tarfile as tar
import sys, os

###################################################################################################

NEW = {
	"equity" : f"{DIR}/data/new/equities",
	"rss" : f"{DIR}/data/new/rss",
	"treasuryrates" : f"{DIR}/data/new/treasuryrates",
	"instruments" : f"{DIR}/data/new/instruments",
}

TAR = {
	key : value.replace("/new/", "/tar/new/")
	for key, value in NEW.items()
}

###################################################################################################

if __name__ == '__main__':

	for key in TAR:

		print("Processing Folder:", key)

		if key == "equity":

			for folder in sorted(os.listdir(NEW[key])):
				
				print("Compressing File:", folder)

				with tar.open(f"{TAR[key]}/{folder}.tar.xz", "x:xz") as tar_file:

					for file in os.listdir(f"{NEW[key]}/{folder}"):
						tar_file.add(f"{NEW[key]}/{folder}/{file}", file)

		else:

			for file in sorted(os.listdir(NEW[key])):

				print("Compressing File:", file)

				basename = file.split(".")[0]

				with tar.open(f"{TAR[key]}/{basename}.tar.xz", "x:xz") as tar_file:
					tar_file.add(f"{NEW[key]}/{file}", file)
