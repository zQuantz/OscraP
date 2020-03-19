from apscheduler.schedulers.background import BlockingScheduler
from threading import Thread

from const import DIR, logger, date_today
from datetime import datetime
import pandas as pd
import numpy as np
import feedparser
import sys, os
import uuid

class Feed(Thread):
    
    def __init__(self, source, feed):

        Thread.__init__(self)
        
        self.source = source.strip()
        self.feed = feed.strip()

        self.entries = []
        self.last_45 = []
        
        self.default_date = "Fri, 1 Jan 1960 00:00 GMT"
        self.last_updated = pd.to_datetime(self.default_date)
        
        self.num_tries = 0
        self.num_success = 0
        
        self.num_status_errors = 0
        self.num_no_update_entry = 0
        self.num_not_updated = 0
        self.num_no_entries = 0
        self.num_no_id = 0
        self.num_collected = 0
        self.num_no_status = 0

    def run(self):

        job_defaults = {
            'coalesce': True,
            'max_instances': 1
        }
        self.blocker = BlockingScheduler(job_defaults = job_defaults)
        self.blocker.add_job(self.parse_feed, 'cron', second='*/5', id='parse_feed')
        
        self.blocker.start()

    def on_close(self):

        self.blocker.shutdown()
        self.join()

    def get_last_updated(self, response):

        last_upd = response['feed'].get('updated', None)
        if not last_upd:
            last_upd = response.get('updated', None)

        if not last_upd:
            return None
        return pd.to_datetime(last_upd)

    def get_id(self, entry):

        id_ = entry.get('id', None)
        if not id_:
            id_ = entry.get('link', None)

        if not id_:
            return None
        return id_
        
    def parse_feed(self):

        print("Parsing", self.source, self.feed)

        self.num_tries += 1
        response = feedparser.parse(self.feed)
        
        status = response.get('status', None)
        if not status:
            self.num_no_status += 1
            return

        if status != 200:
            logger.warning(f"{self.source},{self.feed},Status,{response['status']}")
            self.num_status_errors += 1
            return
    
        last_updated = self.get_last_updated(response)
        if not last_updated:
            logger.warning(f"{self.source},{self.feed},Last Updated,{None}")
            self.num_no_update_entry += 1
            return
        
        if last_updated == self.last_updated:
            logger.warning(f"{self.source},{self.feed},Last Updated,No Change")
            self.num_not_updated += 1
            return
        
        self.last_updated = last_updated
        self.num_success += 1
        
        entries = response.get('entries', None)
        if not entries:
            logger.warning(f"{self.source},{self.feed},Entries,{None}")
            self.num_no_entries += 1
            return
        
        logger.warning(f"{self.source},{self.feed},Parsing Success,")
        for entry in entries:
            self.add_entry(entry, response['feed']['title'])

        if len(self.entries) > 0:
            logger.info(f"{self.source},{self.feed},Saving,{len(self.entries)}")
            df = pd.DataFrame(self.entries, columns = ['source', 'feed', 'feed_title', 'id', 'title', 'summary', 'pub_date', 'link'])
            df.to_csv(f"{DIR}/news_data/{str(uuid.uuid4())}.csv", index=False)
            self.entries = []
    
    def add_entry(self, entry, rss_title):

        id_ = self.get_id(entry)
        if not id_:
            self.num_no_id += 1
            return
        
        if id_ in self.last_45:
            return
        
        self.last_45.append(id_)
        self.last_45 = self.last_45[-45:]
        self.num_collected += 1
        
        self.entries.append([
            self.source,
            self.feed,
            rss_title,
            id_,
            entry.get('title', None),
            entry.get('summary', None),
            pd.to_datetime(entry.get('published', self.default_date), utc=True),
            entry.get("link", None)
        ])
