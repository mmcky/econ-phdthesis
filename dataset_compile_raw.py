"""
Compile RAW Data into a Single Data File
========================================

Author: Matthew McKay (mamckay@gmail.com)

This script compiles and converts (where necessary) raw data into a single data file

Sources
-------
[1] NBER 
[2] BACI

Notes
-----
1. 	Care must be taken when working with CSV files. 
	a. After 1984 there are some float values in 'value' column
	b. icode, ecode, sitc4 need to be imported explicitly as strings
2. 	Stata and HDF are more similar in type when compared with CSV. Both have the presence of "" which should be replaced with np.nan
3. 	Unit and Quantity information is only available after 1984 in NBER Dataset
4. 	HDF are both compact and fast and therefore should be used as the standard file source for dataset objects

"""

import sys
import pandas as pd
import csv
import numpy as np

#-Dataset Information-#
from dataset_info import SOURCE_DIR, TARGET_RAW_DIR

#------#
#-NBER-#
#------#

#-Convert Each year to CSV File-#
def nber_convert_dta_to_csv(source_dir, target_dir):
	for year in range(1962, 2000+1, 1):
		fn = source_dir + "wtf%s.dta" % str(year)[2:]
		print "Loading Year: %s from file: %s" % (year, fn)
		data = pd.read_stata(fn)
		fn = target_dir + "wtf%s.csv" % str(year)[2:]
		print "Converting Year: %s from file: %s" % (year, fn)
		data.to_csv(fn, index=False, quoting=csv.QUOTE_NONNUMERIC)
		print "Convert DTA to CSV Finished!"

#-Convert All Years to an HDF File-#
def nber_convert_dta_to_hdf(source_dir, target_dir, index='year'):
	if index == 'year':
		fn = target_dir + "nber_year.h5"
		store = pd.HDFStore(fn, complevel=9, complib='zlib')
		for year in range(1962, 2000+1, 1):
			fn = source_dir + "wtf%s.dta" % str(year)[2:]
			print "Loading Year: %s from file: %s" % (year, fn)
			data = pd.read_stata(fn)
			store.put('Y'+str(year), data, format='table')
		print "HDF File Saved ..."
		print store
		store.close()
	else:
		data = pd.DataFrame()
		for year in range(1962, 2000+1, 1):
			fn = source_dir + "wtf%s.dta" % str(year)[2:]
			print "Loading Year: %s from file: %s" % (year, fn)
			data = data.append(pd.read_stata(fn))
		fn = target_dir + "nber.h5"
		store = pd.HDFStore(fn, complevel=9, complib='zlib')
		store.put('nber', data, format='table')
		print "HDF File Saved ..."
		print store
		store.close()
	print "Convert DTA to HDF Finished!"

#-Convert NBER supplementary Data-#
def nber_supp_convert_dta_to_hdf(source_dir, target_dir):
	"""
	Save NBER supplementary data into an HDF file "nber_supp_year.hdf"
	"""
	fn = target_dir + "nber_supp_year.h5"
	store = pd.HDFStore(fn, complevel=9, complib='zlib')
	for year in xrange(1988, 2000+1, 1):
		fn = source_dir + "china_hk%s.dta" % str(year)[2:]
		print "[NBER-SUPP] Loading Year: %s from file: %s" % (year, fn)
		data = pd.read_stata(fn)
		store.put('Y'+str(year), data, format='table')
	print "HDF file Saved ..."
	print store
	store.close()


#------#
#-BACI-#
#------#

#-Convert All CSV Year Files to an HDF File-#
def baci_convert_dta_to_hdf(source_dir, target_dir):
	fn = target_dir + "baci_year.h5"
	store = pd.HDFStore(fn, complevel=9, complib='zlib')
	for year in range(1998, 2012+1, 1):
		fn = source_dir + "baci96_%s.csv" % str(year)
		print "Loading Year: %s from file: %s" % (year, fn)
		data = pd.read_csv(fn, dtype={'hs6' : str})
		store.put('Y'+str(year), data, format='table')
	print "HDF File Saved"
	print store
	store.close()
	print "Convert CSV to HDF Finished!"

#-Raw Data Conversions and Comparisons-#

if __name__ == "__main__":
	
	#-Execution Settings-#
	NBER=True
	dta_to_csv = False 				# Using HDF as Key DataStructure Due to it's size and speed advantage
	dta_to_hdf = True 				# Data Structure of Choice
	
	BACI=True
	csv_to_hdf = True

	#-Convert NBER-#
	if NBER:
		source_dir = SOURCE_DIR['nber']
		target_dir = TARGET_RAW_DIR['nber']
		#-Conversions-#
		if dta_to_csv:
			print "Convert dta to csv files"
			nber_convert_dta_to_csv(source_dir, target_dir)
		if dta_to_hdf:
			print "Convert dta to hdf file"
			nber_convert_dta_to_hdf(source_dir, target_dir, index='year') 
			nber_supp_convert_dta_to_hdf(source_dir, target_dir)

	#-Convert BACI-#
	if BACI:
		source_dir = SOURCE_DIR['baci96'] 	
		target_dir = TARGET_RAW_DIR['baci96']
		#-Conversions-#
		if csv_to_hdf:
			print "Convert csv to hdf file"
			baci_convert_dta_to_hdf(source_dir, target_dir)