"""
Test Raw Data Generator Script for RAW NBER Data

Testing Package Data File: nberwtf_raw_years-1990-1991-1992.h5 against original source

"""

import sys
import os
import pandas as pd
from pyeconlab.util import package_folder
from pandas.util.testing import assert_frame_equal

if sys.platform.startswith('win'):
	DATA_DIR = r"D:/work-data/datasets/"
elif sys.platform.startswith('darwin') or sys.platform.startswith('linux'):             
    abs_path = os.path.expanduser("~")
    DATA_DIR = abs_path + "/work-data/datasets/"

#-Original Source Data-#
SOURCE_DIR = DATA_DIR + "36a376e5a01385782112519bddfac85e" + "/"  				#-!MANUAL CONFIGURATION!-#

#-Package Data-#
TEST_DATA_DIR = package_folder(__file__, "data") 

#-Compare Data-#
def test_nberwtf_raw_years_package_data(verbose=True):
	"""
	Test RAW Sample WTF Data that is within the REPO
	"""
	TEST_DATA = pd.HDFStore(TEST_DATA_DIR+"nberwtf_raw_years-1990-1991-1992.h5")
	for key in TEST_DATA.keys():
		year = key.replace("/Y", "")
		if verbose: print "Testing Year: %s" % year
		source_dta = pd.read_stata(SOURCE_DIR + "wtf%s.dta"%year[2:])
		assert_frame_equal(TEST_DATA[key], source_dta)
	TEST_DATA.close()

def test_nberwtf_hkchina_raw_years_package_data(verbose=True):
	"""
	Test RAW Sample Data (hk-china adjustments) that is within the REPO
	"""
	TEST_DATA = pd.HDFStore(TEST_DATA_DIR+"nberwtf_hkchina_supp_raw_years-1990-1991-1992.h5")
	for key in TEST_DATA.keys():
		year = key.replace("/Y", "")
		if verbose: print "Testing Year: %s" % year
		source_dta = pd.read_stata(SOURCE_DIR + "china_hk%s.dta" % str(year)[2:])
		assert_frame_equal(TEST_DATA[key], source_dta)
	TEST_DATA.close()