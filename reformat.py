from utils import format_option_chain
from const import DIR
import pandas as pd
import numpy as np
import sys, os
import shutil

if __name__ == '__main__':

	for folder in os.listdir(f"{DIR}/options_data"):
		
		if os.path.isdir(f"{DIR}/options_data/{folder}"):
			
			for file in os.listdir(f"{DIR}/options_data/{folder}"):
				
				ext = file.split('.')[-1]
				if ext == 'txt':
					continue

				df = pd.read_csv(f"{DIR}/options_data/{folder}/{file}")
				cols = df.columns
				cols = [col if col != 'ExpierationDate' else 'ExpirationDate' for col in cols]
				df.columns = cols
				print(df)
				df = format_option_chain(df)
				df.to_csv(f"{DIR}/options_data/{folder}/{file}", index=False)

			try:
				os.remove(f"{DIR}/options_data/{folder}.zip")
			except Exception as e:
				print("Folder", folder, "not fold, Error:", e)
			shutil.make_archive(f"{DIR}/options_data/{folder}", "zip", f"{DIR}/options_data/{folder}")

