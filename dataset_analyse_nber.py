"""

Analyse and Construct Meta Data for NBER Data

Dependancies
------------
pyeconlab.NBERWTFConstructor

Notes
-----
1. Should these be converted over to using the internally constucted raw data? [No Keep using Object Library]

"""

import os
import gc
from pyeconlab import NBERWTFConstructor
import matplotlib.pyplot as plt
import pandas as pd
SOURCE_DATA_DIR = os.path.expanduser("~/work-data/datasets/36a376e5a01385782112519bddfac85e/")

#-------------------#
#-Setup Directories-#
#-------------------#

from dataset_info import RESULTS_DIR
RESULTS = RESULTS_DIR["nber"]

#-Levels-#
SITCR2L4 = True
SITCR2L3 = True
SITCR2L2 = True
SITCR2L1 = True

#-Execution Blocks-#
RAW_PRODUCTCODE_COMPOSITION_TABLES = True 			#Drop World Observations
ADJUSTED_PRODUCTCODE_COMPOSITION_TABLES = True  	#Adjusted for HongKong-China, Drop World Observations
INTERTEMPORAL_PRODUCTCODE_ADJUSTMENTS = True 		#Adjusted for HongKong-China, Drop World Observations This also computes adjustments tables for 6200, 7400, 8400
INTERTEMPORAL_PRODUCTCODE_ADJUSTMENTS_DATALOSS = True
RAW_COUNTRYCODE_COMPOSITION_TABLES = True
RAW_SIMPLESTATS_TABLE = True
RAW_UNOFFICIALCODES_CNTRY_PLOTS = True

DATASET_SIMPLESTATS_TABLE = True
DATASET_PERCENTWORLDTRADE_PLOTS = True
DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES = True

#-1974 to 2000 Datasets-#
DATASET_7400_SIMPLESTATS_TABLE = True
DATASET_7400_PERCENTWORLDTRADE_PLOTS = True
DATASET_7400_PRODUCTCODE_INTERTEMPORAL_TABLES = True

#-1984 to 2000 Datasets-#
DATASET_8400_SIMPLESTATS_TABLE = True
DATASET_8400_PERCENTWORLDTRADE_PLOTS = True
DATASET_8400_PRODUCTCODE_INTERTEMPORAL_TABLES = True

#### ------------------ ####
#### ---> RAW DATA <--- ####
#### ------------------ ####

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##
## ---> Product Composition Tables <--- ##
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##

if RAW_PRODUCTCODE_COMPOSITION_TABLES:
	#
	# Note: All tables include Meta Data in the Index (SITCR2 Official Code Indicator)
	#

	print "Running RAW_PRODUCTCODE_COMPOSITION_TABLES ... (drop_world_observations())"

	if SITCR2L4:
		#-Data: SITC Level 4-#
		nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
		DIR = RESULTS + "intertemporal-productcodes-sitcl4/raw/"
		nber.drop_world_observations() 								#This Keeps NES
		#-Intertemporal ProductCode Indicators and Values Tables at Various Levels-#
		#-P Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='indicator')
		df.to_excel(DIR + 'intertemporal_sitc4_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='value')
		df.to_excel(DIR + 'intertemporal_sitc4_values_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=3)
		df.to_excel(DIR + 'intertemporal_sitc4_valuecompositions_L3_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=2)
		df.to_excel(DIR + 'intertemporal_sitc4_valuecompositions_L2_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=1)
		df.to_excel(DIR + 'intertemporal_sitc4_valuecompositions_L1_wmeta.xlsx')

		#-Intertemporal Composition Tables by Country x Product at Various Higher Levels-#
		#-CP Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=3)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L3_wmeta_cpidx.xlsx')
		#- This Script could be made quicker by adjusting CP and PC Indexes Here -#
		# df.reorder_levels(order=['productcode', 'country'])
		# df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L3_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=3)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L3_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=2)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L2_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=2)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L2_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=1)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L1_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=1)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L1_wmeta_cpidx.xlsx')

		#-Intertemporal Composition Tables by Product x Country at Various Higher Levels-#
		#-PC Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=3)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L3_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=3)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L3_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=2)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L2_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=2)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L2_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L1_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L1_wmeta_pcidx.xlsx')

		del nber, df

	#------#

	if SITCR2L3:
		#-Data: SITC Level 3-#
		#-Intertemporal ProductCode Indicators and Values Tables at Various Levels-#
		DIR = RESULTS + "intertemporal-productcodes-sitcl3/raw/"
		nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
		nber.drop_world_observations() 								#This Keeps NES		nber.collapse_to_productcode_level(level=3, verbose=True)
		nber.collapse_to_productcode_level(level=3, verbose=True)
		df = nber.intertemporal_productcodes_dataset(tabletype='indicator')
		df.to_excel(DIR + 'intertemporal_sitc3_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='value')
		df.to_excel(DIR + 'intertemporal_sitc3_values_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=2)
		df.to_excel(DIR + 'intertemporal_sitc3_valuecompositions_L2_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=1)
		df.to_excel(DIR + 'intertemporal_sitc3_valuecompositions_L1_wmeta.xlsx')

		#-Intertemporal Composition Tables by Country x Product at Various Higher Levels-#
		#-CP Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=2)
		df.to_excel(DIR + 'intertemporal_sitc3_exporter_valuecompositions_L2_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=2)
		df.to_excel(DIR + 'intertemporal_sitc3_importer_valuecompositions_L2_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=1)
		df.to_excel(DIR + 'intertemporal_sitc3_exporter_valuecompositions_L1_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=1)
		df.to_excel(DIR + 'intertemporal_sitc3_importer_valuecompositions_L1_wmeta_cpidx.xlsx')

		#-Intertemporal Composition Tables by Product x Country at Various Higher Levels-#
		#-PC Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=2)
		df.to_excel(DIR + 'intertemporal_sitc3_exporter_valuecompositions_L2_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=2)
		df.to_excel(DIR + 'intertemporal_sitc3_importer_valuecompositions_L2_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc3_exporter_valuecompositions_L1_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc3_importer_valuecompositions_L1_wmeta_pcidx.xlsx')

		del nber, df

	#------#

	if SITCR2L2:
		#-Data: SITC Level 2-#
		#-Intertemporal ProductCode Indicators and Values Tables at Various Levels-#
		DIR = RESULTS + "intertemporal-productcodes-sitcl2/raw/"
		nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
		nber.drop_world_observations() 								#This Keeps NES
		nber.collapse_to_productcode_level(level=2, verbose=True)
		df = nber.intertemporal_productcodes_dataset(tabletype='indicator')
		df.to_excel(DIR + 'intertemporal_sitc2_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='value')
		df.to_excel(DIR + 'intertemporal_sitc2_values_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=1)
		df.to_excel(DIR + 'intertemporal_sitc2_valuecompositions_L1_wmeta.xlsx')

		#-Intertemporal Composition Tables by Country x Product at Various Higher Levels-#
		#-CP Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=1)
		df.to_excel(DIR + 'intertemporal_sitc2_exporter_valuecompositions_L1_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=1)
		df.to_excel(DIR + 'intertemporal_sitc2_importer_valuecompositions_L1_wmeta_cpidx.xlsx')

		#-Intertemporal Composition Tables by Product x Country at Various Higher Levels-#
		#-PC Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc2_exporter_valuecompositions_L1_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc2_importer_valuecompositions_L1_wmeta_pcidx.xlsx')

		del nber, df

	#------#

	#-Intertemporal Exporters-#

	#-Intertemporal Number of Exporters Data at SITC4 Level-#
	DIR = RESULTS + "intertemporal-exporters/raw/"
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	nber.drop_world_observations() 								#This Keeps NES
	df = nber.intertemporal_productcode_exporters(meta=True)
	df.to_excel(DIR + 'intertemporal_sitc4_numcntry_wmeta.xlsx')
	del nber, df

	#-Intertemporal Number of Exporters Data at SITC3 Level-#
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	nber.drop_world_observations() 								#This Keeps NES
	nber.collapse_to_productcode_level(level=3, verbose=True)
	df = nber.intertemporal_productcode_exporters(meta=True)
	df.to_excel(DIR + 'intertemporal_sitc3_numcntry_wmeta.xlsx')
	del nber, df

	#-Intertemporal Number of Exporters Data at SITC2-#
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	nber.drop_world_observations() 								#This Keeps NES
	nber.collapse_to_productcode_level(level=2, verbose=True)
	df = nber.intertemporal_productcode_exporters(meta=True)
	df.to_excel(DIR + 'intertemporal_sitc2_numcntry_wmeta.xlsx')
	del nber, df

	#-Intertemporal Number of Exporters Data at SITC1-#
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	nber.drop_world_observations() 								#This Keeps NES
	nber.collapse_to_productcode_level(level=1, verbose=True)
	df = nber.intertemporal_productcode_exporters(meta=True)
	df.to_excel(DIR + 'intertemporal_sitc1_numcntry_wmeta.xlsx')
	del nber, df


#-----------------------------#
#-ADJUSTED COMPOSITION TABLES-#
#-----------------------------#

if ADJUSTED_PRODUCTCODE_COMPOSITION_TABLES:
	#
	# Note: All tables include Meta Data in the Index (SITCR2 Official Code Indicator)
	#

	print "Running ADJUSTED_PRODUCTCODE_COMPOSITION_TABLES ..."

	if SITCR2L4:
		#-Data: SITC Level 4-#
		nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
		DIR = RESULTS + "intertemporal-productcodes-sitcl4/" 		#These are Primary so Keep at Base Level
		nber.adjust_china_hongkongdata(verbose=True)
		nber.drop_world_observations(verbose=True) 					#This Keeps NES
		#-Intertemporal ProductCode Indicators and Values Tables at Various Levels-#
		#-P Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='indicator')
		df.to_excel(DIR + 'intertemporal_sitc4_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='value')
		df.to_excel(DIR + 'intertemporal_sitc4_values_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=3)
		df.to_excel(DIR + 'intertemporal_sitc4_valuecompositions_L3_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=2)
		df.to_excel(DIR + 'intertemporal_sitc4_valuecompositions_L2_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=1)
		df.to_excel(DIR + 'intertemporal_sitc4_valuecompositions_L1_wmeta.xlsx')

		#-Intertemporal Composition Tables by Country x Product at Various Higher Levels-#
		#-CP Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=3)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L3_wmeta_cpidx.xlsx')
		#- This Script could be made quicker by adjusting CP and PC Indexes Here -#
		# df.reorder_levels(order=['productcode', 'country'])
		# df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L3_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=3)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L3_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=2)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L2_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=2)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L2_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=1)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L1_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=1)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L1_wmeta_cpidx.xlsx')

		#-Intertemporal Composition Tables by Product x Country at Various Higher Levels-#
		#-PC Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=3)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L3_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=3)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L3_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=2)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L2_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=2)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L2_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc4_exporter_valuecompositions_L1_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc4_importer_valuecompositions_L1_wmeta_pcidx.xlsx')

		del nber, df

	#------#

	if SITCR2L3:
		#-Data: SITC Level 3-#
		#-Intertemporal ProductCode Indicators and Values Tables at Various Levels-#
		DIR = RESULTS + "intertemporal-productcodes-sitcl3/"
		nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
		nber.adjust_china_hongkongdata(verbose=True)
		nber.drop_world_observations(verbose=True) 								#This Keeps NES
		nber.collapse_to_productcode_level(level=3, verbose=True)
		df = nber.intertemporal_productcodes_dataset(tabletype='indicator')
		df.to_excel(DIR + 'intertemporal_sitc3_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='value')
		df.to_excel(DIR + 'intertemporal_sitc3_values_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=2)
		df.to_excel(DIR + 'intertemporal_sitc3_valuecompositions_L2_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=1)
		df.to_excel(DIR + 'intertemporal_sitc3_valuecompositions_L1_wmeta.xlsx')

		#-Intertemporal Composition Tables by Country x Product at Various Higher Levels-#
		#-CP Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=2)
		df.to_excel(DIR + 'intertemporal_sitc3_exporter_valuecompositions_L2_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=2)
		df.to_excel(DIR + 'intertemporal_sitc3_importer_valuecompositions_L2_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=1)
		df.to_excel(DIR + 'intertemporal_sitc3_exporter_valuecompositions_L1_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=1)
		df.to_excel(DIR + 'intertemporal_sitc3_importer_valuecompositions_L1_wmeta_cpidx.xlsx')

		#-Intertemporal Composition Tables by Product x Country at Various Higher Levels-#
		#-PC Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=2)
		df.to_excel(DIR + 'intertemporal_sitc3_exporter_valuecompositions_L2_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=2)
		df.to_excel(DIR + 'intertemporal_sitc3_importer_valuecompositions_L2_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc3_exporter_valuecompositions_L1_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc3_importer_valuecompositions_L1_wmeta_pcidx.xlsx')

		del nber, df

	#------#

	if SITCR2L2:
		#-Data: SITC Level 2-#
		#-Intertemporal ProductCode Indicators and Values Tables at Various Levels-#
		DIR = RESULTS + "intertemporal-productcodes-sitcl2/"
		nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
		nber.adjust_china_hongkongdata(verbose=True)
		nber.drop_world_observations(verbose=True) 								#This Keeps NES
		nber.collapse_to_productcode_level(level=2, verbose=True)
		df = nber.intertemporal_productcodes_dataset(tabletype='indicator')
		df.to_excel(DIR + 'intertemporal_sitc2_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='value')
		df.to_excel(DIR + 'intertemporal_sitc2_values_wmeta.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', level=1)
		df.to_excel(DIR + 'intertemporal_sitc2_valuecompositions_L1_wmeta.xlsx')

		#-Intertemporal Composition Tables by Country x Product at Various Higher Levels-#
		#-CP Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', level=1)
		df.to_excel(DIR + 'intertemporal_sitc2_exporter_valuecompositions_L1_wmeta_cpidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', level=1)
		df.to_excel(DIR + 'intertemporal_sitc2_importer_valuecompositions_L1_wmeta_cpidx.xlsx')

		#-Intertemporal Composition Tables by Product x Country at Various Higher Levels-#
		#-PC Index-#
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc2_exporter_valuecompositions_L1_wmeta_pcidx.xlsx')
		df = nber.intertemporal_productcodes_dataset(tabletype='composition', countries='importer', cpidx=False, level=1)
		df.to_excel(DIR + 'intertemporal_sitc2_importer_valuecompositions_L1_wmeta_pcidx.xlsx')

		del nber, df

	#------#

	#-Intertemporal Number of Exporters Data at SITC4 Level-#
	DIR = RESULTS + "intertemporal-exporters/"
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	nber.adjust_china_hongkongdata(verbose=True)
	nber.drop_world_observations(verbose=True) 								#This Keeps NES
	df = nber.intertemporal_productcode_exporters(meta=True)
	df.to_excel(DIR + 'intertemporal_sitc4_numcntry_wmeta.xlsx')
	del nber, df

	#-Intertemporal Number of Exporters Data at SITC3 Level-#
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	nber.adjust_china_hongkongdata(verbose=True)
	nber.drop_world_observations(verbose=True) 								#This Keeps NES
	nber.collapse_to_productcode_level(level=3, verbose=True)
	df = nber.intertemporal_productcode_exporters(meta=True)
	df.to_excel(DIR + 'intertemporal_sitc3_numcntry_wmeta.xlsx')
	del nber, df

	#-Intertemporal Number of Exporters Data at SITC2-#
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	nber.adjust_china_hongkongdata(verbose=True)
	nber.drop_world_observations(verbose=True) 								#This Keeps NES
	nber.collapse_to_productcode_level(level=2, verbose=True)
	df = nber.intertemporal_productcode_exporters(meta=True)
	df.to_excel(DIR + 'intertemporal_sitc2_numcntry_wmeta.xlsx')
	del nber, df

	#-Intertemporal Number of Exporters Data at SITC1-#
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	nber.drop_world_observations() 								#This Keeps NES
	nber.collapse_to_productcode_level(level=1, verbose=True)
	df = nber.intertemporal_productcode_exporters(meta=True)
	df.to_excel(DIR + 'intertemporal_sitc1_numcntry_wmeta.xlsx')
	del nber, df


if INTERTEMPORAL_PRODUCTCODE_ADJUSTMENTS:

	print "Running INTERTEMPORAL_PRODUCTCODE_ADJUSTMENTS ..."		

	if SITCR2L4:
		nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
		DIR = RESULTS + "intertemporal-productcodes-sitcl4/" 		#These are Primary so Keep at Base Level
		nber.adjust_china_hongkongdata(verbose=True)
		nber.drop_world_observations(verbose=True) 					#This Keeps NES
		#-Intertemporally Consistent Codes Adjustments Table-#
		nber.drop_alpha_productcodes(verbose=True) #-Drop These as they remove small amounts of information-#
		drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, "6200"), tabletype="indicator")
		print drop_items
		print collapse_items
		adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl4_adjustments.xlsx")
		pd.Series(drop_items, name="drop").to_csv(DIR + "intertemporal_productcodes_sitcl4_drop.csv")
		pd.Series(collapse_items, name="collapse").to_csv(DIR + "intertemporal_productcodes_sitcl4_collapse.csv")
		#-Values-#
		drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, "6200"), tabletype="value")
		print drop_items
		print collapse_items
		adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl4_value_adjustments.xlsx")
		#-Composition-#
		drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, "6200"), tabletype="composition")
		print drop_items
		print collapse_items
		adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl4_composition_adjustments.xlsx")
		del nber, adjust_table

	if SITCR2L3:
		DIR = RESULTS + "intertemporal-productcodes-sitcl3/"
		nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
		nber.adjust_china_hongkongdata(verbose=True)
		nber.drop_world_observations(verbose=True) 								#This Keeps NES
		nber.collapse_to_productcode_level(level=3, verbose=True)
		#-Intertemporally Consistent Codes Adjustments Table-#
		nber.drop_alpha_productcodes(verbose=True) #-Drop These as they remove small amounts of information-#
		drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, "6200"), tabletype="indicator")
		print drop_items
		print collapse_items
		adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl3_adjustments.xlsx")
		pd.Series(drop_items, name="drop").to_csv(DIR + "intertemporal_productcodes_sitcl3_drop.csv")
		pd.Series(collapse_items, name="collapse").to_csv(DIR + "intertemporal_productcodes_sitcl3_collapse.csv")
		#-Values-#
		drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, "6200"), tabletype="value")
		print drop_items
		print collapse_items
		adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl3_value_adjustments.xlsx")
		#-Composition-#
		drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, "6200"), tabletype="composition")
		print drop_items
		print collapse_items
		adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl3_composition_adjustments.xlsx")
		del nber, adjust_table

	if SITCR2L2:
		DIR = RESULTS + "intertemporal-productcodes-sitcl2/"
		nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
		nber.adjust_china_hongkongdata(verbose=True)
		nber.drop_world_observations(verbose=True) 								#This Keeps NES
		nber.collapse_to_productcode_level(level=2, verbose=True)
		#-Intertemporally Consistent Codes Adjustments Table-#
		nber.drop_alpha_productcodes(verbose=True) #-Drop These as they remove small amounts of information-#
		drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, "6200"), tabletype="indicator")
		print drop_items
		print collapse_items
		adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl2_adjustments.xlsx")
		pd.Series(drop_items, name="drop").to_csv(DIR + "intertemporal_productcodes_sitcl2_drop.csv")
		pd.Series(collapse_items, name="collapse").to_csv(DIR + "intertemporal_productcodes_sitcl2_collapse.csv")
		#-Value-#
		drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, "6200"), tabletype="value")
		print drop_items
		print collapse_items
		adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl2_value_adjustments.xlsx")
		#-Composition-#
		drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, "6200"), tabletype="composition")
		print drop_items
		print collapse_items
		adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl2_composition_adjustments.xlsx")
		del nber, adjust_table

	#--------------------#
	#-Other Time Periods-#
	#--------------------#

	periods = [xrange(1974,2000+1,1), xrange(1984,2000+1,1)]

	for period in periods:

		start_year = period[0]
		end_year = period[-1]

		SpecialCaseDef = "%s%s"%(str(start_year)[2:], str(end_year)[2:])

		print "Running INTERTEMPORAL_PRODUCTCODE_ADJUSTMENTS ... Years => %s to %s ... (Special Case -> %s)" % (start_year,end_year, SpecialCaseDef)	

		if SITCR2L4: 								
			DIR = RESULTS + "intertemporal-productcodes-sitcl4/" 		#These are Primary so Keep at Base Level
			nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR, years=period, verbose=True)
			nber.complete_dataset = True
			nber.adjust_china_hongkongdata(verbose=True)
			nber.drop_world_observations(verbose=True) 					#This Keeps NES
			#-Intertemporally Consistent Codes Adjustments Table-#
			nber.drop_alpha_productcodes(verbose=True) #-Drop These as they remove small amounts of information-#
			drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, SpecialCaseDef), tabletype="indicator")
			print drop_items
			print collapse_items
			adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl4_%sto%s_adjustments.xlsx"%(start_year,end_year))
			pd.Series(drop_items, name="drop").to_csv(DIR + "intertemporal_productcodes_sitcl4_%sto%s_drop.csv"%(start_year,end_year))
			pd.Series(collapse_items, name="collapse").to_csv(DIR + "intertemporal_productcodes_sitcl4_%sto%s_collapse.csv"%(start_year,end_year))
			#-Values-#
			drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, SpecialCaseDef), tabletype="value")
			print drop_items
			print collapse_items
			adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl4_%sto%s_value_adjustments.xlsx"%(start_year,end_year))
			#-Composition-#
			drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, SpecialCaseDef), tabletype="composition")
			print drop_items
			print collapse_items
			adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl4_%sto%s_composition_adjustments.xlsx"%(start_year,end_year))
			del nber, adjust_table

		if SITCR2L3:
			DIR = RESULTS + "intertemporal-productcodes-sitcl3/"
			nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR, years=period, verbose=True)
			nber.complete_dataset = True
			nber.adjust_china_hongkongdata(verbose=True)
			nber.drop_world_observations(verbose=True) 								#This Keeps NES
			nber.collapse_to_productcode_level(level=3, verbose=True)
			#-Intertemporally Consistent Codes Adjustments Table-#
			nber.drop_alpha_productcodes(verbose=True) #-Drop These as they remove small amounts of information-#
			drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, SpecialCaseDef), tabletype="indicator")
			print drop_items
			print collapse_items
			adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl3_%sto%s_adjustments.xlsx"%(start_year,end_year))
			pd.Series(drop_items, name="drop").to_csv(DIR + "intertemporal_productcodes_sitcl3_%sto%s_drop.csv"%(start_year,end_year))
			pd.Series(collapse_items, name="collapse").to_csv(DIR + "intertemporal_productcodes_sitcl3_%sto%s_collapse.csv"%(start_year,end_year))
			#-Values-#
			drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, SpecialCaseDef), tabletype="value")
			print drop_items
			print collapse_items
			adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl3_%sto%s_value_adjustments.xlsx"%(start_year,end_year))
			#-Composition-#
			drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, SpecialCaseDef), tabletype="composition")
			print drop_items
			print collapse_items
			adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl3_%sto%s_composition_adjustments.xlsx"%(start_year,end_year))
			del nber, adjust_table

		if SITCR2L2:
			DIR = RESULTS + "intertemporal-productcodes-sitcl2/"
			nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR, years=period, verbose=True)
			nber.complete_dataset = True
			nber.adjust_china_hongkongdata(verbose=True)
			nber.drop_world_observations(verbose=True) 								#This Keeps NES
			nber.collapse_to_productcode_level(level=2, verbose=True)
			#-Intertemporally Consistent Codes Adjustments Table-#
			nber.drop_alpha_productcodes(verbose=True) #-Drop These as they remove small amounts of information-#
			drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, SpecialCaseDef), tabletype="indicator")
			print drop_items
			print collapse_items
			adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl2_%sto%s_adjustments.xlsx"%(start_year,end_year))
			pd.Series(drop_items, name="drop").to_csv(DIR + "intertemporal_productcodes_sitcl2_%sto%s_drop.csv"%(start_year,end_year))
			pd.Series(collapse_items, name="collapse").to_csv(DIR + "intertemporal_productcodes_sitcl2_%sto%s_collapse.csv"%(start_year,end_year))
			#-Value-#
			drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, SpecialCaseDef), tabletype="value")
			print drop_items
			print collapse_items
			adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl2_%sto%s_value_adjustments.xlsx"%(start_year,end_year))
			#-Composition-#
			drop_items, collapse_items, adjust_table = nber.intertemporal_productcode_lists(return_table=True, include_special=(True, SpecialCaseDef), tabletype="composition")
			print drop_items
			print collapse_items
			adjust_table.to_excel(DIR + "intertemporal_productcodes_sitcl2_%sto%s_composition_adjustments.xlsx"%(start_year,end_year))
			del nber, adjust_table


if INTERTEMPORAL_PRODUCTCODE_ADJUSTMENTS_DATALOSS:

	print "Running INTERTEMPORAL_PRODUCTCODE_ADJUSTMENTS_DATALOSS ..."

	#-World Values-#
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	world_values = nber.dataset.loc[(nber.dataset.importer == "World") & (nber.dataset.exporter == "World")].groupby(["year"]).sum()["value"]
	del nber
	gc.collect()

	DIR = RESULTS + "intertemporal-productcodes-sitcl4/plots/" 		#These are Primary so Keep at Base Level
	
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	nber.adjust_china_hongkongdata(verbose=True)
	nber.drop_world_observations(verbose=True) 					#This Keeps NES
	#-Intertemporally Consistent Codes Adjustments Table-#
	nber.drop_alpha_productcodes(verbose=True) #-Drop These as they remove small amounts of information-#

	#-Obtain Different Datasets based on Value Rules-#
	rowavgnorms = [0.01,0.5,1,2,5]
	rowmaxs = [1,5,10,20,50]
	num_products = []
	for rowavgnorm in rowavgnorms:
		for rowmax in rowmaxs:
			drop_items, collapse_items = nber.intertemporal_productcode_lists(return_table=False, include_special=(True, "6200"), tabletype="composition", value_check=(True,rowavgnorm,rowmax))
			keep_items = set(nber.dataset.sitc4.unique()).difference(set(drop_items))
			num_products.append((rowavgnorm, rowmax, len(keep_items))) 
			data = nber.dataset.loc[nber.dataset["sitc4"].isin(keep_items)]
			yearly_values = data.groupby("year").sum()["value"]
			percent_values = yearly_values.div(world_values)*100
			fig = percent_values.plot(title="Parameters: rowavgnorm(%s); rowmax(%s)"%(rowavgnorm, rowmax))
			plt.savefig(DIR + "rowavgnorm(%s)_rowmax(%s)_percent_wld.pdf"%(rowavgnorm, rowmax))
			plt.close()
			del data
			gc.collect()
	print num_products

	#This Study shows that a good balance is struck with rowavgnorm = 1, and rowmaxs = 5
	#This isn't overly distorting during the early periods and captures most of the dataset value

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##
## ---> Country Composition Tables <--- ##
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##

if RAW_COUNTRYCODE_COMPOSITION_TABLES:

	print "Running RAW_COUNTRYCODE_COMPOSITION_TABLES ..."
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)

	#-Data: ISO3C -#
	#-Intertemporal CountryCode Indicators-#
	DIR = RESULTS + "intertemporal-countrycodes/"
	iiso3n, eiso3n = nber.intertemporal_countrycodes_dataset(verbose=True)
	iiso3n.to_excel(DIR + "intertemporal_iiso3n.xlsx")
	eiso3n.to_excel(DIR + "intertemporal_eiso3n.xlsx")

	nber.reset_dataset()
	nber.complete_dataset = True
	iiso3n, eiso3n = nber.intertemporal_countrycodes_raw_data(verbose=True)
	iiso3n.to_excel(DIR + "raw_intertemporal_iiso3n.xlsx")
	eiso3n.to_excel(DIR + "raw_intertemporal_eiso3n.xlsx")


#### ----> END COMPOSITION TABLES <---- ####

#### ----> SIMPLE STATS TABLES <---- ####

if RAW_SIMPLESTATS_TABLE:
	from pyeconlab.trade.util import describe

	print "Running RAW_SIMPLESTATS_TABLE ..."
	DIR = RESULTS + "tables/"
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	table = describe(nber.dataset, productcode="sitc4", importer="importer", exporter="exporter")
	#-Excel Table-#
	table.to_excel(DIR + "raw_wtf_stats.xlsx")
	#-Latex Snippet-#
	with open(DIR + "raw_wtf_stats_table.latex", "w") as latex_file:
		latex_file.write(table.to_latex())
	del nber

	#-Change this to a Single Table-#
	# #-By Year-#
	# for year in xrange(1962,2000+1,1):
	# 	nber_year = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR, years=[year])
	# 	table = describe(nber_year.dataset, productcode="sitc4", importer="importer", exporter="exporter")
	# 	#-Excel Table-#
	# 	table.to_excel(DIR + "raw_wtf_stats_%s.xlsx" % year)
	# 	#-Latex Snippet-#
	# 	with open(DIR + "raw_wtf_stats_%s.latex" % year, "w") as latex_file:
	# 		latex_file.write(table.to_latex())

#### ----> END SIMPLE STATS TABLES <---- ####


#### -------------------- #####
#### ----> PLOTTING <---- #####
#### -------------------- #####

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##
## ---> Unofficial Product Code Plotes <--- ##
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##

if RAW_UNOFFICIALCODES_CNTRY_PLOTS:

	print "Running RAW_UNOFFICIALCODES_CNTRY_PLOTS ..."
	DIR = RESULTS + "plots/percent_unofficial_codes/"

	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	nber.add_sitcr2_official_marker()
	
	#-World Export Values-#
	world_export = nber.dataset.loc[(nber.dataset.exporter == "World")]
	world_export = world_export.groupby(['year', 'SITCR2']).sum()['value']
	world_export = world_export.unstack(level=['SITCR2'])
	world_export['%'] = world_export[0].div(world_export[0] + world_export[1])*100
	s = world_export['%']
	s.plot(title="WLD (Percent of Export in Unofficial Codes)", yticks=[0,25,50,75,100])
	plt.savefig(DIR + 'WLD_percent_unofficial_export.png')
	plt.close()

	#-Country Export Values-#
	nber.countries_only()
	cntry_export = nber.dataset.loc[(nber.dataset.eiso3c != '.')]
	cntry_export = cntry_export.groupby(['eiso3c', 'year', 'SITCR2']).sum()['value']
	cntry_export = cntry_export.unstack(level=['SITCR2'])
	cntry_export['%'] = cntry_export[0].div(cntry_export[0]+cntry_export[1])*100
	s = cntry_export['%']
	for cntry in s.index.levels[0]:
		s.ix[cntry].plot(title="%s (Percent of Export in Unofficial Codes)" % cntry, yticks=[0,25,50,75,100])
		plt.savefig(DIR + '%s_percent_unofficial_export.png' % cntry)
		plt.close()


#### ------------------------------------------------------------------------------------------- ####

#### ------------------ ####
#### ---> DATASETS <--- ####
#### ------------------ ####

import gc
import glob
import pandas as pd
from dataset_info import TARGET_DATASET_DIR
SOURCE_DIR = TARGET_DATASET_DIR['nber']

STORES = glob.glob(SOURCE_DIR + "*.h5")
STORES = [x for x in STORES if x.split("/")[-1][0:3] != "raw"] 	#Filter Out RAW Files
TARGET_DIR = RESULTS + "tables/"

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##
## ----> SIMPLE STATS TABLES <---- ##
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##

if DATASET_SIMPLESTATS_TABLE:

	print
	print "Running DATASET_SIMPLESTATS_TABLE ..."
	print

	from pyeconlab.trade.util import describe

	for dataset_file in STORES:
		print "Running STATS on File %s" % dataset_file
		store = pd.HDFStore(dataset_file)
		datasets = store.keys()
		for dataset in sorted(datasets):
			dataset = dataset.strip("/") 								#Remove Directory Structure
			print "Computing SIMPLE STATS for dataset: %s" % dataset
			data = pd.read_hdf(dataset_file, key=dataset)
			productcode = "".join(dataset_file.split("/")[-1].split("-")[2].split("r2l"))
			dataset_table = describe(data, table_name=dataset, productcode=productcode)
			if dataset == "A":
				table = dataset_table
			else:
				table = table.merge(dataset_table, left_index=True, right_index=True)
		store.close()
		#-Excel Table-#
		fl = dataset_file.split("/")[-1].split(".")[0] + "_stats" + ".xlsx"
		table.to_excel(TARGET_DIR + fl)
		#-Latex Snippet-#
		fl = dataset_file.split("/")[-1].split(".")[0] + "_stats" + ".tex"
		with open(TARGET_DIR + fl, "w") as latex_file:
			latex_file.write(table.to_latex())

	# #-By Year-#
	# Is this Required? #

if DATASET_PERCENTWORLDTRADE_PLOTS:
	
	print
	print "Running DATASET_PERCENTWORLDTRADE_PLOTS..."
	print

	TARGET_DIR = RESULTS + "plots/percent_world_values/"

	#-World Values-#
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	world_values = nber.dataset.loc[(nber.dataset.importer == "World") & (nber.dataset.exporter == "World")].groupby(["year"]).sum()["value"]
	del nber
	gc.collect()

	for dataset_file in STORES:
		print "Producing GRAPH on File %s" % dataset_file
		store = pd.HDFStore(dataset_file)
		datasets = store.keys()
		for dataset in sorted(datasets):
			print "Computing GRAPH for dataset: %s" % dataset
			data = pd.read_hdf(dataset_file, key=dataset)
			yearly_values = data.groupby(["year"]).sum()["value"]
			percent_values = yearly_values.div(world_values)*100
			fig = percent_values.plot(title="Dataset: %s (%s)"%(dataset, dataset_file))
			plt.savefig(TARGET_DIR + "%s_%s_percent_wld.pdf"%(dataset, dataset_file.split('/')[-1].split('.')[0]))
			plt.close()
		store.close()

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##
## ----> INTERTEMPORAL PRODUCTCODE TABLES <---- ##
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##

if DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES:

	print "[6200] Running DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES ..."

	def split_filenames(fl):
		dataset, data_type, classification, years = fl.split("-")
		classification, product_level = classification[:-2], classification[-1:]
		return dataset, data_type, classification, product_level

	TARGET_DIR = RESULTS + "intertemporal-productcodes/"

	for store in STORES:
		print "Computing Composition Tables for: %s" % store
		dataset, data_type, classification, product_level = split_filenames(store.split("/")[-1])
		store = pd.HDFStore(store)
		for dataset in store.keys():
			print "Computing table for dataset: %s ..." % dataset
			dataset = dataset.strip("/")
			intertemp_product = store[dataset].groupby(["year", "sitc%s"%product_level]).sum().unstack("year")
			intertemp_product.columns = intertemp_product.columns.droplevel()
			intertemp_product.to_excel(TARGET_DIR + "intertemporal_product_%s_%sl%s_%s.xlsx"%(data_type, classification, product_level, dataset))
		store.close()


## --------------------------------------------------- OTHER YEARS ----------------------------------------------------------------- ##

#------------------------#
#-NBER DATA 1974 to 2000-#
#------------------------#

LOCAL_DIR = "Y7400/"
STORES = glob.glob(SOURCE_DIR + LOCAL_DIR + "*.h5")
STORES = [x for x in STORES if x.split("/")[-1][0:3] != "raw"] 	#Filter Out RAW Files

if DATASET_7400_SIMPLESTATS_TABLE:
	print
	print "Running DATASET_7400_SIMPLESTATS_TABLE ..."
	print

	TARGET_DIR = RESULTS + "tables/" + LOCAL_DIR

	from pyeconlab.trade.util import describe
		
	for dataset_file in STORES:
		print "Running STATS on File %s" % dataset_file
		store = pd.HDFStore(dataset_file)
		datasets = store.keys()
		for dataset in sorted(datasets):
			dataset = dataset.strip("/") 								#Remove Directory Structure
			print "Computing SIMPLE STATS for dataset: %s" % dataset
			data = pd.read_hdf(dataset_file, key=dataset)
			productcode = "".join(dataset_file.split("/")[-1].split("-")[2].split("r2l"))
			dataset_table = describe(data, table_name=dataset, productcode=productcode)
			if dataset == "A":
				table = dataset_table
			else:
				table = table.merge(dataset_table, left_index=True, right_index=True)
			del data
			gc.collect()
		store.close()
		#-Excel Table-#
		fl = dataset_file.split("/")[-1].split(".")[0] + "_stats" + ".xlsx"
		table.to_excel(TARGET_DIR + fl)
		#-Latex Snippet-#
		fl = dataset_file.split("/")[-1].split(".")[0] + "_stats" + ".tex"
		with open(TARGET_DIR + fl, "w") as latex_file:
			latex_file.write(table.to_latex())


if DATASET_7400_PERCENTWORLDTRADE_PLOTS:
	
	print
	print "[7400] Running DATASET_7400_PERCENTWORLDTRADE_PLOTS..."
	print

	TARGET_DIR = RESULTS + "plots/percent_world_values/" + LOCAL_DIR

	#-World Values-#
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	world_values = nber.dataset.loc[(nber.dataset.importer == "World") & (nber.dataset.exporter == "World")].groupby(["year"]).sum()["value"]
	del nber
	gc.collect()

	for dataset_file in STORES:
		print "Producing GRAPH on File %s" % dataset_file
		store = pd.HDFStore(dataset_file)
		datasets = store.keys()
		for dataset in sorted(datasets):
			print "Computing GRAPH for dataset: %s" % dataset
			data = pd.read_hdf(dataset_file, key=dataset)
			yearly_values = data.groupby(["year"]).sum()["value"]
			percent_values = yearly_values.div(world_values)*100
			fig = percent_values.plot(title="Dataset: %s (%s)"%(dataset, dataset_file))
			plt.savefig(TARGET_DIR + "%s_%s_percent_wld.pdf"%(dataset, dataset_file.split('/')[-1].split('.')[0]))
			plt.close()
			del data
			gc.collect()
		store.close()

if DATASET_7400_PRODUCTCODE_INTERTEMPORAL_TABLES:

	print "[7400] Running DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES ..."

	def split_filenames(fl):
		dataset, data_type, classification, years = fl.split("-")
		classification, product_level = classification[:-2], classification[-1:]
		return dataset, data_type, classification, product_level

	TARGET_DIR = RESULTS + "intertemporal-productcodes/Y7400/"

	for store in STORES:
		print "Computing Composition Tables for: %s" % store
		dataset, data_type, classification, product_level = split_filenames(store.split("/")[-1])
		store = pd.HDFStore(store)
		for dataset in store.keys():
			print "Computing table for dataset: %s ..." % dataset
			dataset = dataset.strip("/")
			intertemp_product = store[dataset].groupby(["year", "sitc%s"%product_level]).sum().unstack("year")
			intertemp_product.columns = intertemp_product.columns.droplevel()
			intertemp_product.to_excel(TARGET_DIR + "intertemporal_product_%s_%sl%s_%s.xlsx"%(data_type, classification, product_level, dataset))
			del intertemp_product
			gc.collect()
		store.close()


#------------------------#
#-NBER DATA 1984 to 2000-#
#------------------------#

LOCAL_DIR = "Y8400/"
STORES = glob.glob(SOURCE_DIR + LOCAL_DIR + "*.h5")
STORES = [x for x in STORES if x.split("/")[-1][0:3] != "raw"] 	#Filter Out RAW Files

if DATASET_8400_SIMPLESTATS_TABLE:
	print
	print "[8400] Running DATASET_8400_SIMPLESTATS_TABLE ..."
	print

	TARGET_DIR = RESULTS + "tables/" + LOCAL_DIR

	from pyeconlab.trade.util import describe
		
	for dataset_file in STORES:
		print "Running STATS on File %s" % dataset_file
		store = pd.HDFStore(dataset_file)
		datasets = store.keys()
		for dataset in sorted(datasets):
			dataset = dataset.strip("/") 								#Remove Directory Structure
			print "Computing SIMPLE STATS for dataset: %s" % dataset
			data = pd.read_hdf(dataset_file, key=dataset)
			productcode = "".join(dataset_file.split("/")[-1].split("-")[2].split("r2l"))
			dataset_table = describe(data, table_name=dataset, productcode=productcode)
			if dataset == "A":
				table = dataset_table
			else:
				table = table.merge(dataset_table, left_index=True, right_index=True)
			del data
			gc.collect()
		store.close()
		#-Excel Table-#
		fl = dataset_file.split("/")[-1].split(".")[0] + "_stats" + ".xlsx"
		table.to_excel(TARGET_DIR + fl)
		#-Latex Snippet-#
		fl = dataset_file.split("/")[-1].split(".")[0] + "_stats" + ".tex"
		with open(TARGET_DIR + fl, "w") as latex_file:
			latex_file.write(table.to_latex())


if DATASET_8400_PERCENTWORLDTRADE_PLOTS:
	
	print
	print "[8400] Running DATASET_8400_PERCENTWORLDTRADE_PLOTS..."
	print

	TARGET_DIR = RESULTS + "plots/percent_world_values/" + LOCAL_DIR

	#-World Values-#
	nber = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR)
	world_values = nber.dataset.loc[(nber.dataset.importer == "World") & (nber.dataset.exporter == "World")].groupby(["year"]).sum()["value"]
	del nber
	gc.collect()

	for dataset_file in STORES:
		print "Producing GRAPH on File %s" % dataset_file
		store = pd.HDFStore(dataset_file)
		datasets = store.keys()
		for dataset in sorted(datasets):
			print "Computing GRAPH for dataset: %s" % dataset
			data = pd.read_hdf(dataset_file, key=dataset)
			yearly_values = data.groupby(["year"]).sum()["value"]
			percent_values = yearly_values.div(world_values)*100
			fig = percent_values.plot(title="Dataset: %s (%s)"%(dataset, dataset_file))
			plt.savefig(TARGET_DIR + "%s_%s_percent_wld.pdf"%(dataset, dataset_file.split('/')[-1].split('.')[0]))
			plt.close()
			del data
			gc.collect()
		store.close()

if DATASET_8400_PRODUCTCODE_INTERTEMPORAL_TABLES:

	print "[8400] Running DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES ..."

	def split_filenames(fl):
		dataset, data_type, classification, years = fl.split("-")
		classification, product_level = classification[:-2], classification[-1:]
		return dataset, data_type, classification, product_level

	TARGET_DIR = RESULTS + "intertemporal-productcodes/Y8400/"

	for store in STORES:
		print "Computing Composition Tables for: %s" % store
		dataset, data_type, classification, product_level = split_filenames(store.split("/")[-1])
		store = pd.HDFStore(store)
		for dataset in store.keys():
			print "Computing table for dataset: %s ..." % dataset
			dataset = dataset.strip("/")
			intertemp_product = store[dataset].groupby(["year", "sitc%s"%product_level]).sum().unstack("year")
			intertemp_product.columns = intertemp_product.columns.droplevel()
			intertemp_product.to_excel(TARGET_DIR + "intertemporal_product_%s_%sl%s_%s.xlsx"%(data_type, classification, product_level, dataset))
			del intertemp_product
			gc.collect()
		store.close()


	