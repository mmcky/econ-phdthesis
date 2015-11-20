"""
Generate a RAW Data Archive for NBER

This is a reduced sample of 3 continuous years
Years: 1990,1991, 1992

Notes
-----
1. 	This is a temporary solution for broad testing. 
	In the future this should be replaced with non-binary files or used some other tool like git-annex. 
2. 	This is not a general function and requires setup. 
	EXCLUDE-DISTRIBUTION

"""

import sys
import os
import pandas as pd

if sys.platform.startswith('win'):
	DATA_DIR = r"D:/work-data/datasets/"
elif sys.platform.startswith('darwin') or sys.platform.startswith('linux'):             
    abs_path = os.path.expanduser("~")
    DATA_DIR = abs_path + "/work-data/datasets/"

SOURCE_DIR = DATA_DIR + "36a376e5a01385782112519bddfac85e" + "/"

YEARS = [1990, 1991, 1992]

#-Raw Data-#

if __name__ == "__main__":

	#-Data Archive-#
	fn = "nberwtf_raw_years-1990-1991-1992.h5"
	store = pd.HDFStore(fn, complevel=9, complib='zlib')

	for year in YEARS:
		fn = SOURCE_DIR + "wtf%s.dta" % str(year)[2:]
		print "Loading Year: %s from File: %s" % (year, fn)
		raw_data = pd.read_stata(fn)
		store.put('Y%s'%year, raw_data, format='table')

	print "Closing HDF Store File ... "
	store.close()

	#-Hong Kong - China Adjustment Data-#
	fn = "nberwtf_hkchina_supp_raw_years-1990-1991-1992.h5"
	store = pd.HDFStore(fn, complevel=9, complib='zlib')

	for year in YEARS:
		fn = SOURCE_DIR + "china_hk%s.dta" % str(year)[2:]
		print "Loading Year: %s from File: %s" % (year, fn)
		raw_data = pd.read_stata(fn)
		store.put('Y%s'%year, raw_data, format='table')

	print "Closing HDF Store File ... "
	store.close()