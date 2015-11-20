"""
HDFStore Utilities
"""

import pandas as pd

def convert_hdf_to_stata(source_hdf, target_dir="", verbose=True):
	"""
	Convert All Datasets within the HDFStore to dta stata file
	"""
	store = pd.HDFStore(source_hdf)
	fls = []
	for dataset in store.keys():
		if verbose: print "Converting dataset %s in %s ..." % (dataset, source_hdf)
		data = store[dataset]
		fln, fltype = source_hdf.split("/")[-1].split(".")
		fln = fln + "_" + dataset.replace("/","") + ".dta"
		if target_dir != "":
			fln = target_dir + fln
		fls.append(fln)
		data.to_stata(fln, write_index=False)
		del data
	return fls