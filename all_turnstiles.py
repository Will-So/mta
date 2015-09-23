# Changes to make
# 1. How we read the data
# 2. Parsing the columns away.

import sys
from turnstiles import read_file, makeCols as make_cols, clean_frame
import pandas as pd

def generate_df():
	df = read_file('data/all.csv')
	df = remove_extra_headers(df)
	df = make_cols(df)
	df = df.dropna(subset=['entries', 'exits'])

	return df


def remove_extra_headers(df):
	"""Removes erroneous headers from the file type
	"""
	df = df.loc[df.entries != 'ENTRIES']
	df.entries = df.entries.astype(int)
	df.exits = df.exits.astype(int)

	return df
