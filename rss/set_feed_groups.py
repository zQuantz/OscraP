from const import DIR
import pandas as pd
import sys, os
import joblib

df = pd.read_csv(f"{DIR}/data/rss.csv")
counts = df.Source.value_counts().to_dict()

print("Feed Count by Source")
print(df.Source.value_counts())

## Assign groups
groups = [
    ["GlobeNewsWire"],
    ["Benzinga"],
    ["NY Times"],
    ["CNBC", "Investing"],
    ["NASDAQ", "BBC", "Wall Street Journal"],
    ["MarketWatch", "Yahoo Finance", "Bank of Canada", "Bank of England"]
]

## Get group sleep values
group_info = {}
for group in groups:
    num_feeds = 0
    for source in group:
        num_feeds += counts[source]
    sleep = round(max(1, 60 / num_feeds))
    group_info[tuple(group)] = sleep

print()
for group in group_info:
	print("Group:", group, "Sleep Timer:", group_info[group])

with open('data/groups.pkl', 'wb') as file:
	joblib.dump(group_info, file)