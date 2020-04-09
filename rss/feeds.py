from apscheduler.schedulers.background import BlockingScheduler
from threading import Thread

from const import DIR, date_today
from collections import deque
from datetime import datetime
from hashlib import md5
import pandas as pd
import numpy as np
import feedparser
import sys, os
import joblib
import json
import uuid

class Feeds(Thread):
    
    def __init__(self, sources, feeds, sleep, logger):

        Thread.__init__(self)

        self.sleep = sleep
        self.logger = logger
        
        self.coords = deque([
            (source.strip(), feed.strip())
            for source, feed in zip(sources, feeds)
        ])

        self.entries = []
        self.last_45 = {
            feed : []
            for _, feed in self.coords
        }

    def run(self):

        job_defaults = {
            'coalesce': True,
            'max_instances': 2
        }
        self.blocker = BlockingScheduler(job_defaults = job_defaults)
        self.blocker.add_job(self.parse_feed, 'cron', second=f'*/{self.sleep}', id='parse_feed')
        
        self.blocker.start()

    def on_close(self):

        self.blocker.shutdown()
        self.join()
        
    def parse_feed(self):

        self.coords.rotate()
        self.source, self.feed = self.coords[0]
        
        print(f"Parsing,{self.source},{self.feed},")
        self.logger.info(f"Parsing,{self.source},{self.feed},")
        response = feedparser.parse(self.feed)
        
        status = response.get('status', None)
        if not status:
            self.logger.warning(f"Status,{self.source},{self.feed},None")
            return

        if status != 200:
            self.logger.warning(f"Status,{self.source},{self.feed},{status}")
            return
        self.logger.info(f"Status,{self.source},{self.feed},200")
        
        entries = response.get('entries', None)
        if not entries:
            self.logger.warning(f"Entries,{self.source},{self.feed},None")
            return
        self.logger.info(f"Status,{self.source},{self.feed},{len(entries)}")

        for entry in entries:
            
            entry_str = json.dumps(entry).encode()
            entry_hash = md5(entry_str).hexdigest()
            
            if entry_hash in self.last_45[self.feed]:
                break

            self.last_45[self.feed].append(entry_hash)
            self.last_45[self.feed] = self.last_45[self.feed][-45:]

            self.entries.append(entry)

        if len(self.entries) > 0:
            with open(f"{DIR}/news_data/{str(uuid.uuid4())}.pkl", "wb") as file:
                joblib.dump(self.entries, file)
            self.entries = []
