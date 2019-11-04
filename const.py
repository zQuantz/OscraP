from datetime import datetime
import os

DIR = os.path.realpath(os.path.dirname(__file__))

## Jobs
n_threads = 25
n_jobs = 1

## Today's Date
date_today = datetime.today().strftime("%Y-%m-%d")
named_date_fmt = "%B %d, %Y"