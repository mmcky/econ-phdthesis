"""
General DataFrame Utilities
"""

import pandas as pd

def attach_attributes(df, name, dtype, classification, revision, units_value_str, complete_dataset, notes):
	""" Attach attributes to df """
	if type(df) != pd.DataFrame:
		raise TypeError("Incoming Type(%s) != pd.DataFrame"%type(df))
	df.txf_name = name 
	df.txf_data_type=dtype 
	df.txf_classification=classification 
	df.txf_revision = revision
	df.txf_units_value_str = units_value_str
	df.txf_complete_dataset = complete_dataset
	df.txf_notes = notes
	return df