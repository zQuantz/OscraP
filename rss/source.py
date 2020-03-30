import pandas as pd
import numpy as np
import sys, os

from feed import Feed

class Source():

	def __init__(self, source, feeds):

		self.source = source
		self.feeds = {}

		for feed in feeds:
			self.feeds[feed] = Feed(source, feed)
			self.feeds[feed].start()

	def on_close(self):

		for feed in self.feeds:
			self.feeds[feed].on_close()