"""
Compilation of Other Useful Datasets
====================================

  1. World Development Indicators Dataset 
  2. Atlas of Complexity Dataset 
  3. Penn World Table Dataset 

Construct h5 datasets and pyeconlab objects

 """

import os
import gc
import shutil
import warnings
import pandas as pd

from pyeconlab import WDI, CIDAtlasDataConstructor, PENN
from dataset_info import SOURCE_DIR, TARGET_DATASET_DIR

#---------#
#-Control-#
#---------#

COMPILE_WDI = False
COMPILE_ATLAS = False 		#-!!-Requires AWS for 'Trade'-!!-# 'Trade Disabled'
COMPILE_PENN = True

#------------------------------#
#-World Development Indicators-#
#------------------------------#

if COMPILE_WDI:
	wdi = WDI(source_dir=SOURCE_DIR['wdi'])
	stata_wide_fln = wdi.to_stata(table_type="wide", target_dir=TARGET_DATASET_DIR['wdi']) 	# wdi_data_wide.dta
	stata_long_fln = wdi.to_stata(table_type="long", target_dir=TARGET_DATASET_DIR['wdi']) 	# wdi_data_long.dta
	hdf_fln = wdi.to_hdf(target_dir = TARGET_DATASET_DIR['wdi']) 										# wdi_data.h5

#-----------------------------#
#-Atlas of Complexity Dataset-#
#-----------------------------#

#-Countries Only Dataset-#

if COMPILE_ATLAS:
	#-Values-#
	print "[INFO] Processing VALUES Data ..."
	for classification in ["SITCR2", "HS92"]:
		print warnings.warn("This will not compile 'trade' data - just export and import data")
		for dtype in ["export", "import"]:  				# -- !! -- Excluding "trade" -- !! -- due to memory constraints -- use stata -- #
			print "Processing %s for %s data ..." % (classification, dtype)
			atlas = CIDAtlasDataConstructor(source_dir=SOURCE_DIR['atlas'], trade_classification=classification, dtype=dtype, reduce_memory=True)
			atlas.construct_standardized_dataset()
			#-Store-#
			startyear = atlas.dataset.year.min()
			endyear = atlas.dataset.year.max()
			fln = TARGET_DATASET_DIR["atlas"] + "cidatlas_%s_%s_%sto%s.h5"%(classification.lower(), dtype, startyear, endyear)
			store = pd.HDFStore(fln, complevel=9, complib='zlib')
			#-Country Value Data-#
			atlas.countries_only()
			#-Value-#
			for level in [4,3,2,1]:
				gc.collect()
				print "[INFO] Saving Level %s ... " % level
				if classification == "SITCR2":
					productid = "sitc%s"%level
					if dtype == "export": 
						idx = ["year", "eiso3c", productid]
					elif dtype == "import": 
						idx = ["year", "iiso3c", productid]
					else:
						idx = ["year", "eiso3c", "iiso3c", productid]
					if level != 4:
						atlas.dataset[productid] = atlas.dataset["sitc4"].apply(lambda x: x[0:level])
				if classification == "HS92":
					productid = "hs%s"%level
					if dtype == "export": 
						idx = ["year", "eiso3c", productid]
					elif dtype == "import": 
						idx = ["year", "iiso3c", productid]
					else:
						idx = ["year", "eiso3c", "iiso3c", productid]
					if level != 4:
						atlas.dataset[productid] = atlas.dataset["hs4"].apply(lambda x: x[0:level])
				#-Collapse Levels-#
				countrydata = atlas.dataset[idx+["value"]].groupby(idx, as_index=False).sum()
				store.put("L%s"%level, countrydata, format="table")
				del countrydata
			store.close()
			del atlas 	
			gc.collect()

	#-RCA-#
	print "[INFO] Processing RCA Data ..."
	for classification in ["SITCR2", "HS92"]:
		for dtype in ["export", "import"]:
			print "Processing %s for %s data ..." % (classification, dtype)
			atlas = CIDAtlasDataConstructor(source_dir=SOURCE_DIR['atlas'], trade_classification=classification, dtype=dtype)
			atlas.construct_standardized_dataset()
			#-Store-#
			startyear = atlas.dataset.year.min()
			endyear = atlas.dataset.year.max()
			fln = TARGET_DATASET_DIR["atlas"] + "cidatlas_%s_%s_rca_%sto%s.h5"%(classification.lower(), dtype, startyear, endyear)
			store = pd.HDFStore(fln, complevel=9, complib='zlib')
			#-Country RCA Data-#
			atlas.countries_only()
			countrydata = atlas.dataset.copy(deep=True)
			if classification == "SITCR2":
				if dtype == "export": 
					idx = ["year", "eiso3c", "sitc4"]
				elif dtype == "import": 
					idx = ["year", "iiso3c", "sitc4"]
			if classification == "HS92":
				if dtype == "export": 
					idx = ["year", "eiso3c", "hs4"]
				elif dtype == "import": 
					idx = ["year", "iiso3c", "hs4"]
			countrydata = countrydata.groupby(idx).sum()["rca"].reset_index()
			store.put("L4", countrydata, format="table")
			store.close()
			del countrydata
			gc.collect()


#--------------------------#
#-Penn World Table Dataset-#
#--------------------------#

if COMPILE_PENN:
	print "[INFO] Processing PENN World Tables ... "
	penn = PENN(source_dir=SOURCE_DIR['penn'])
	penn.to_hdf(fl="penn_%s_%sto%s.h5"%(penn.version, penn.start_year, penn.end_year), target_dir=TARGET_DATASET_DIR["penn"])
	penn.to_stata(fl="penn_%s_%sto%s.dta"%(penn.version, penn.start_year, penn.end_year), target_dir=TARGET_DATASET_DIR["penn"])