"""
Chapter 4: Probable and Improbable Product Emergence
====================================================

This captures analysis conducted in support of Chapter 3 - Probable and Improbable Product Emergence 
The statistical work is done in STATA (chapter_3_probable_improbable.do) 

"""

import pandas as pd
import numpy as np
from pyeconlab import CPExportData, WDI
from pyeconlab.trade.util import compute_persistence, from_dict_to_dataframe, \
									reindex_dynamic_dataframe, from_dict_of_series_to, \
									reindex_dynamic_dict, compute_diffusion_properties_nx, \
									attach_attributes

#-Local Imports-#
from dataset_info import TARGET_DATASET_DIR, SOURCE_DIR, CHAPTER_RESULTS

#Note: Easiest Way to run in Parrallel is through 3 x IPython Notebooks
# #-MultiCore-#
# try:
# 	from IPython.parallel import Client
# 	c = Client()
# 	MULTICORE = True
# except:
# 	MULTICORE = False

#-Setup-#
DATASET_DIR = TARGET_DATASET_DIR['nberbaci96']
WDI_SOURCE_DIR = SOURCE_DIR['wdi']
WAZIARG_DIR = SOURCE_DIR['waziarg']				#Trade Liberalisation Indicators
RESULTS_DIR = CHAPTER_RESULTS[3]

#-Helper Functions-#

def tradelib_stats(df):
	""" 
	Compute some tradelib stats for Statistical Work
	"""
	
	#-Cases with 2 Starting Values-#
	special_cases = {
	    "KEN" : 1993,
	    "JAM" : 1989,
	    "LKA" : 1992,
	    "VEN" : 1996,
	}

	def apply_special_cases(x):
	    try:
	        return special_cases[x]
	    except:
	        return np.nan

	#-Start Year Value-#
	df["tradelibyear"] = df.apply(lambda row: row['year'] if row['tradelib']==1 else np.nan,axis=1)
	df["tradelibyear"] = df[["iso3c","tradelibyear"]].groupby("iso3c").transform(min)
	#-Second Start Year for Special Cases-#
	df["tradelibyear2"] = df["iso3c"].apply(lambda x: apply_special_cases(x))
	#-Compute Number of Transitions-#
	df["tradelibperiods"] = df[["iso3c", "tradelib"]].groupby("iso3c").diff()
	df["tradelibperiods"] = df["tradelibperiods"].apply(lambda x: np.nan if x == -1 else x)
	df["tradelibperiods"] = df[["iso3c","tradelibperiods"]].groupby("iso3c").transform(sum)
	return df

#---------------------------------#
#-Regression Dataset Construction-#
#---------------------------------#

def compute_regression_dataset(export_dataset):
	""" 
	Compute a Regression Dataset Helper Function
	"""
	#-Export System-#
	dynples = export_dataset.to_dynamic_productlevelexportsystem()
	dynples = dynples.dynamic_global_panel()
	dynples.rca_matrices(complete_data=True)
	dynples.mcp_matrices()
	dynples.proximity_matrices()

	#-Compute Product Changes-#
	Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts = dynples.compute_product_changes()
	df_BothYears = reindex_dynamic_dataframe(from_dict_to_dataframe(Mcp_BothYears))
	Mc_BothYears = df_BothYears.groupby(level=['country', 'to_year']).sum()
	df_NewProducts = reindex_dynamic_dataframe(from_dict_to_dataframe(Mcp_NewProducts))
	Mc_NewProducts = df_NewProducts.groupby(level=['country', 'to_year']).sum()
	df_DieProducts = reindex_dynamic_dataframe(from_dict_to_dataframe(Mcp_DieProducts))
	Mc_DieProducts = df_DieProducts.groupby(level=['country', 'to_year']).sum()

	#-Compute Probable and Improbable Emergence-#
	Mc_ProbableProducts, Mc_ImProbableProducts = dynples.compute_probable_improbable_emergence(prox_cutoff='median', style='average')
	Mc_ProbableProducts = from_dict_of_series_to(Mc_ProbableProducts, series_name='ProbableProducts')
	Mc_ImProbableProducts = from_dict_of_series_to(Mc_ImProbableProducts, series_name = 'ImprobableProducts')

	#-Compute Probable\Improbable + Persistence-# 
	Mcp_ProbableProducts, Mcp_ImProbableProducts = dynples.compute_probable_improbable_emergence(prox_cutoff='median', style='average', output='reduced')
	Mc_ProbablePersistent = from_dict_of_series_to(compute_persistence(dynples.mcp, Mcp_ProbableProducts, output="summary"), series_name="ProbablePersistent")
	Mc_ImProbablePersistent = from_dict_of_series_to(compute_persistence(dynples.mcp, Mcp_ImProbableProducts, output="summary"), series_name="ImProbablePersistent")

	#-Compute Persistence Total Products as a Check-#
	Mc_PersistentProducts = from_dict_of_series_to(compute_persistence(dynples.mcp, reindex_dynamic_dict(Mcp_NewProducts, base='finish'), output="summary"), series_name="NewPersistent")

	#Compute Centrality
	AvgCentrality = dynples.compute_average_centrality(sum_not_mean=True)
	AvgCentrality = from_dict_of_series_to(AvgCentrality, series_name='AvgCentrality')

	#-Compute Diffusion Properties-#
	Mc_ProxAvgDiff, Mc_ProxVarDiff, Mc_ProxWidthDiff = compute_diffusion_properties_nx(dynples.mcp, dynples.proximity)
	Mc_ProxAvgDiff = from_dict_of_series_to(Mc_ProxAvgDiff, series_name='AvgProx')
	Mc_ProxVarDiff = from_dict_of_series_to(Mc_ProxVarDiff, series_name='VarProx')
	Mc_ProxWidthDiff = from_dict_of_series_to(Mc_ProxWidthDiff, series_name='WidthProx')

	#GDP and GDPPC Series
	wdi = WDI(WDI_SOURCE_DIR)
	GDP = wdi.series_long('NY.GDP.MKTP.CD')
	GDPPC = wdi.series_long('NY.GDP.PCAP.CD')
	GDPPCConst = wdi.series_long('NY.GDP.PCAP.KD')
	GDPPCPPP = wdi.series_long('NY.GDP.PCAP.PP.CD') 	  #Only Available 1990 (USE PENN)
	GDPPCPPPConst = wdi.series_long('NY.GDP.PCAP.PP.KD')  #Only Available 1990 (Use PENN)
	GDPGrowth = wdi.series_long('NY.GDP.MKTP.KD.ZG')
	GDPPCGrowth = wdi.series_long('NY.GDP.PCAP.KD.ZG')
	GNIAtlas = wdi.series_long('NY.GNP.ATLS.CD')
	GNIPPP = wdi.series_long('NY.GNP.MKTP.PP.CD')
	GNIPCAtlas = wdi.series_long('NY.GNP.PCAP.CD')
	NetBarterToT = wdi.series_long('TT.PRI.MRCH.XD.WD')
	#-Infrastructure-#
	AirDepartures = wdi.series_long('IS.AIR.DPRT')
	RailLinesKm = wdi.series_long('IS.RRS.TOTL.KM')
	ElectricityUsePC = wdi.series_long('EG.USE.ELEC.KH.PC')
	#-Others-#
	Population = wdi.series_long('SP.POP.TOTL')
	LandArea = wdi.series_long('AG.LND.TOTL.K2')

	#Trade Liberalisation Data
	trade_lib = pd.read_csv(WAZIARG_DIR+'trade_lib_wacziarg.csv')
	trade_lib = tradelib_stats(trade_lib) 							#-Add in TradeLib Stats-#
	trade_lib = trade_lib.set_index(keys=['iso3c', 'year'])

	#Merge and Export File (Country-Year Data)
	df = AvgCentrality
	for item in [Mc_ProbableProducts, Mc_ImProbableProducts, Mc_ProbablePersistent, Mc_ImProbablePersistent, Mc_ProxAvgDiff, Mc_ProxVarDiff, Mc_ProxWidthDiff]:
		df = df.join(item, how='outer')
	#-Single Index for Merging-#
	df.index = pd.Index(df.index)
	for item in [GDP, GDPPC, GDPPCConst, GDPPCPPP, GDPPCPPPConst, GDPGrowth, GDPPCGrowth, GNIAtlas, GNIPPP, GNIPCAtlas, NetBarterToT, AirDepartures, RailLinesKm, ElectricityUsePC, Population, LandArea]:
		item.index = pd.Index(item.index)
		df = df.merge(item, how='left', left_index=True, right_index=True)
	for item in [Mc_BothYears, Mc_NewProducts, Mc_DieProducts, Mc_PersistentProducts]:
		item.index = pd.Index(item.index)
		df = df.merge(item, how='left', left_index=True, right_index=True)
	for item in [trade_lib]:
		item.index = pd.Index(item.index)
		df = df.merge(item, how='left', left_index=True, right_index=True)
	#-Restore MultiIndex-#
	df.index = pd.MultiIndex.from_tuples(df.index, names=['country', 'year'])
	return df


#----------------------------------#
#-Compute Stata Regression Dataset-#
#----------------------------------#

#--------------#
#-SITC Level 4-#
#--------------#

for dataset in ["C", "D", "E"]: 	#Note: 'F' Can be Derived from E and preserve Country Matching Capacity

	print
	print "Running Chapter 3 Dataset Construction for Dataset (Level 4) Dataset(%s) ..."%(dataset)

	#-NBER 1962 to 2000 Dataset-#
	#~~~~~~~~~~~~~~~~~~~~~~~~~~~#
	print "Processing NBER ..."
	DATASET_DIR = TARGET_DATASET_DIR['nber']
	DATA6200 = pd.read_hdf(DATASET_DIR+"nber-export-sitcr2l4-1962to2000.h5", key=dataset)
	DATA6200 = DATA6200.rename(columns={'sitc4':'productcode'})
	DATA6200 = attach_attributes(DATA6200, name="nberbaci", dtype="export", classification="SITC", \
							 revision=2, units_value_str="1000$", complete_dataset=True, notes="Dataset %s"%dataset)
	DATA6200 = CPExportData(DATA6200, allow_mixed_productcode=True)
	#-Compute Regression Dataset-#
	df = compute_regression_dataset(DATA6200)
	#-Store Stata-#
	df.to_stata(RESULTS_DIR+"nber-export-sitcr2l4-1962to2000-analysis-dataset%s.dta"%dataset)
	#-Store CSV-#
	df.to_csv(RESULTS_DIR+"nber-export-sitcr2l4-1962to2000-analysis-dataset%s.csv"%dataset)
	del DATA6200


	#-NBER BACI 1962 to 2012 Dataset-#
	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
	print "Processing NBER BACI ..."
	DATASET_DIR = TARGET_DATASET_DIR['nberbaci96']
	DATA6212 = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l4-1962to2012-harmonised.h5", key=dataset)
	DATA6212 = DATA6212.rename(columns={'sitc4':'productcode'})
	DATA6212 = attach_attributes(DATA6212, name="nberbaci", dtype="export", classification="SITC", \
							 revision=2, units_value_str="1000$", complete_dataset=True, notes="Dataset %s"%dataset)
	DATA6212 = CPExportData(DATA6212, allow_mixed_productcode=True)
	#-Compute Regression Dataset-#
	df = compute_regression_dataset(DATA6212)
	#-Store Stata-#
	df.to_stata(RESULTS_DIR+"nberbaci-export-sitcr2l4-1962to2012-analysis-dataset%s.dta"%dataset)
	#-Store CSV-#
	df.to_csv(RESULTS_DIR+"nberbaci-export-sitcr2l4-1962to2012-analysis-dataset%s.csv"%dataset)
	del DATA6212

#~~~~~~~~~~~~~~~~~~~~#
#-Auxiliary Datasets-#
#~~~~~~~~~~~~~~~~~~~~#

#-NBER BACI 1984 to 2000 Dataset-#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
print "Running Chapter 3 Dataset Construction for Dataset (Level 4) Auxiliary Datasets (1984 to 2000) ..."
DATASET_DIR = TARGET_DATASET_DIR['nber']
DATA8400 = pd.read_hdf(DATASET_DIR+"Y8400/nber-export-sitcr2l4-1984to2000.h5", key='D')
DATA8400 = DATA8400.rename(columns={'sitc4':'productcode'})
DATA8400 = attach_attributes(DATA8400, name="nberbaci", dtype="export", classification="SITC", \
						 revision=2, units_value_str="1000$", complete_dataset=True, notes="Dataset D")
DATA8400 = CPExportData(DATA8400, allow_mixed_productcode=True)
#-Compute Regression Dataset-#
df = compute_regression_dataset(DATA8400)
#-Store Stata-#
df.to_stata(RESULTS_DIR+"nber-export-sitcr2l4-1984to2000-analysis-datasetD.dta")
#-Store CSV-#
df.to_csv(RESULTS_DIR+"nber-export-sitcr2l4-1984to2000-analysis-datasetD.csv")
del DATA8400


#----------------------#
#-SITC Level 3 Dataset-#
#----------------------#

for dataset in ["C", "D", "E"]:

	print
	print "Running Chapter 3 Dataset Construction for Dataset (Level 3) Dataset(%s) ..."%(dataset)

	#-NBER 1962 to 2000 Dataset-#
	#~~~~~~~~~~~~~~~~~~~~~~~~~~~#
	print "Processing NBER ..."
	DATASET_DIR = TARGET_DATASET_DIR['nber']
	DATA6200 = pd.read_hdf(DATASET_DIR+"nber-export-sitcr2l3-1962to2000.h5", key=dataset)
	DATA6200 = DATA6200.rename(columns={'sitc3':'productcode'})
	DATA6200 = attach_attributes(DATA6200, name="nberbaci", dtype="export", classification="SITC", \
							 revision=2, units_value_str="1000$", complete_dataset=True, notes="Dataset %s"%dataset)
	DATA6200 = CPExportData(DATA6200, allow_mixed_productcode=True)
	#-Compute Regression Dataset-#
	df = compute_regression_dataset(DATA6200)
	#-Store Stata-#
	df.to_stata(RESULTS_DIR+"nber-export-sitcr2l3-1962to2000-analysis-dataset%s.dta"%dataset)
	#-Store CSV-#
	df.to_csv(RESULTS_DIR+"nber-export-sitcr2l3-1962to2000-analysis-dataset%s.csv"%dataset)
	del DATA6200

	#-NBER BACI 1962 to 2012 Dataset-#
	#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
	print "Processing NBERBACI ..."
	DATASET_DIR = TARGET_DATASET_DIR['nberbaci96']
	DATA6212 = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l3-1962to2012-harmonised.h5", key=dataset)
	DATA6212 = DATA6212.rename(columns={'sitc3':'productcode'})
	DATA6212 = attach_attributes(DATA6212, name="nberbaci", dtype="export", classification="SITC", \
							 revision=2, units_value_str="1000$", complete_dataset=True, notes="Dataset %s"%dataset)
	DATA6212 = CPExportData(DATA6212, allow_mixed_productcode=True)
	#-Compute Regression Dataset-#
	df = compute_regression_dataset(DATA6212)
	#-Store Stata-#
	df.to_stata(RESULTS_DIR+"nberbaci-export-sitcr2l3-1962to2012-analysis-dataset%s.dta"%dataset)
	#-Store CSV-#
	df.to_csv(RESULTS_DIR+"nberbaci-export-sitcr2l3-1962to2012-analysis-dataset%s.csv"%dataset)
	del DATA6212


#------------------------------------------------------------#
#-Dataset Construction of Reference Atlas of Complexity Data-#
#------------------------------------------------------------#

#-SITC Level 4-#

print
print "Running Chapter 3 Dataset Construction for CIDATLAS 1962 to 2012 Dataset ..."

DATASET_DIR = TARGET_DATASET_DIR['atlas']

#-Dataset-#
CIDATLAS6212 = pd.read_hdf(DATASET_DIR+"cidatlas_sitcr2_export_1962to2012.h5", key='L4')
CIDATLAS6212 = CIDATLAS6212.rename(columns={'sitc4':'productcode'})
CIDATLAS6212 = attach_attributes(CIDATLAS6212, name="cidatlas", dtype="export", classification="SITC", \
						 revision=2, units_value_str="$", complete_dataset=True, notes="Dataset CIDATLAS(L4)")
CIDATLAS6212 = CPExportData(CIDATLAS6212)
#-Compute Regression Dataset-#
df = compute_regression_dataset(CIDATLAS6212)
#-Store Stata-#
df.to_stata(RESULTS_DIR+"cidatlas_sitcr2_export_1962to2012-analysis-L4.dta")
#-Store CSV-#
df.to_csv(RESULTS_DIR+"cidatlas_sitcr2_export_1962to2012-analysis-L4.csv")

#-SITC Level 3-#

#-Dataset-#
CIDATLAS6212 = pd.read_hdf(DATASET_DIR+"cidatlas_sitcr2_export_1962to2012.h5", key='L3')
CIDATLAS6212 = CIDATLAS6212.rename(columns={'sitc3':'productcode'})
CIDATLAS6212 = attach_attributes(CIDATLAS6212, name="cidatlas", dtype="export", classification="SITC", \
						 revision=2, units_value_str="$", complete_dataset=True, notes="Dataset CIDATLAS(L3)")
CIDATLAS6212 = CPExportData(CIDATLAS6212)
#-Compute Regression Dataset-#
df = compute_regression_dataset(CIDATLAS6212)
#-Store Stata-#
df.to_stata(RESULTS_DIR+"cidatlas_sitcr2_export_1962to2012-analysis-L3.dta")
#-Store CSV-#
df.to_csv(RESULTS_DIR+"cidatlas_sitcr2_export_1962to2012-analysis-L3.csv")


##---------------------##
##-Sensativity Studies-##
##---------------------##

#------------------------------------------------#
#-Regression Dataset Construction with Variables-#
#------------------------------------------------#

def compute_regression_dataset_with_choice(export_dataset, prox_cutoff, style):
	""" 
	Compute a Regression Dataset Helper Function
	"""
	#-Export System-#
	dynples = export_dataset.to_dynamic_productlevelexportsystem()
	dynples = dynples.dynamic_global_panel()
	dynples.rca_matrices(complete_data=True)
	dynples.mcp_matrices()
	dynples.proximity_matrices()

	#-Compute Product Changes-#
	Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts = dynples.compute_product_changes()
	df_BothYears = reindex_dynamic_dataframe(from_dict_to_dataframe(Mcp_BothYears))
	Mc_BothYears = df_BothYears.groupby(level=['country', 'to_year']).sum()
	df_NewProducts = reindex_dynamic_dataframe(from_dict_to_dataframe(Mcp_NewProducts))
	Mc_NewProducts = df_NewProducts.groupby(level=['country', 'to_year']).sum()
	df_DieProducts = reindex_dynamic_dataframe(from_dict_to_dataframe(Mcp_DieProducts))
	Mc_DieProducts = df_DieProducts.groupby(level=['country', 'to_year']).sum()

	#-Compute Probable and Improbable Emergence-#
	Mc_ProbableProducts, Mc_ImProbableProducts = dynples.compute_probable_improbable_emergence(prox_cutoff=prox_cutoff, style=style)
	Mc_ProbableProducts = from_dict_of_series_to(Mc_ProbableProducts, series_name='ProbableProducts')
	Mc_ImProbableProducts = from_dict_of_series_to(Mc_ImProbableProducts, series_name = 'ImprobableProducts')

	#-Compute Probable\Improbable + Persistence-# 
	Mcp_ProbableProducts, Mcp_ImProbableProducts = dynples.compute_probable_improbable_emergence(prox_cutoff=prox_cutoff, style=style, output='reduced')
	Mc_ProbablePersistent = from_dict_of_series_to(compute_persistence(dynples.mcp, Mcp_ProbableProducts, output="summary"), series_name="ProbablePersistent")
	Mc_ImProbablePersistent = from_dict_of_series_to(compute_persistence(dynples.mcp, Mcp_ImProbableProducts, output="summary"), series_name="ImProbablePersistent")

	#-Compute Persistence Total Products as a Check-#
	Mc_PersistentProducts = from_dict_of_series_to(compute_persistence(dynples.mcp, reindex_dynamic_dict(Mcp_NewProducts, base='finish'), output="summary"), series_name="NewPersistent")

	#Compute Centrality
	AvgCentrality = dynples.compute_average_centrality(sum_not_mean=True)
	AvgCentrality = from_dict_of_series_to(AvgCentrality, series_name='AvgCentrality')

	#-Compute Diffusion Properties-#
	Mc_ProxAvgDiff, Mc_ProxVarDiff, Mc_ProxWidthDiff = compute_diffusion_properties_nx(dynples.mcp, dynples.proximity)
	Mc_ProxAvgDiff = from_dict_of_series_to(Mc_ProxAvgDiff, series_name='AvgProx')
	Mc_ProxVarDiff = from_dict_of_series_to(Mc_ProxVarDiff, series_name='VarProx')
	Mc_ProxWidthDiff = from_dict_of_series_to(Mc_ProxWidthDiff, series_name='WidthProx')

	#GDP and GDPPC Series
	wdi = WDI(WDI_SOURCE_DIR)
	GDP = wdi.series_long('NY.GDP.MKTP.CD')
	GDPPC = wdi.series_long('NY.GDP.PCAP.CD')
	GDPPCConst = wdi.series_long('NY.GDP.PCAP.KD')
	GDPPCPPP = wdi.series_long('NY.GDP.PCAP.PP.CD') 	  #Only Available 1990 (USE PENN)
	GDPPCPPPConst = wdi.series_long('NY.GDP.PCAP.PP.KD')  #Only Available 1990 (Use PENN)
	GDPGrowth = wdi.series_long('NY.GDP.MKTP.KD.ZG')
	GDPPCGrowth = wdi.series_long('NY.GDP.PCAP.KD.ZG')
	GNIAtlas = wdi.series_long('NY.GNP.ATLS.CD')
	GNIPPP = wdi.series_long('NY.GNP.MKTP.PP.CD')
	GNIPCAtlas = wdi.series_long('NY.GNP.PCAP.CD')
	NetBarterToT = wdi.series_long('TT.PRI.MRCH.XD.WD')
	#-Infrastructure-#
	AirDepartures = wdi.series_long('IS.AIR.DPRT')
	RailLinesKm = wdi.series_long('IS.RRS.TOTL.KM')
	ElectricityUsePC = wdi.series_long('EG.USE.ELEC.KH.PC')
	#-Others-#
	Population = wdi.series_long('SP.POP.TOTL')
	LandArea = wdi.series_long('AG.LND.TOTL.K2')

	#Trade Liberalisation Data
	trade_lib = pd.read_csv(WAZIARG_DIR+'trade_lib_wacziarg.csv')
	trade_lib = tradelib_stats(trade_lib) 							#-Add in TradeLib Stats-#
	trade_lib = trade_lib.set_index(keys=['iso3c', 'year'])

	#Merge and Export File (Country-Year Data)
	df = AvgCentrality
	for item in [Mc_ProbableProducts, Mc_ImProbableProducts, Mc_ProbablePersistent, Mc_ImProbablePersistent, Mc_ProxAvgDiff, Mc_ProxVarDiff, Mc_ProxWidthDiff]:
		df = df.join(item, how='outer')
	#-Single Index for Merging-#
	df.index = pd.Index(df.index)
	for item in [GDP, GDPPC, GDPPCConst, GDPPCPPP, GDPPCPPPConst, GDPGrowth, GDPPCGrowth, GNIAtlas, GNIPPP, GNIPCAtlas, NetBarterToT, AirDepartures, RailLinesKm, ElectricityUsePC, Population, LandArea]:
		item.index = pd.Index(item.index)
		df = df.merge(item, how='left', left_index=True, right_index=True)
	for item in [Mc_BothYears, Mc_NewProducts, Mc_DieProducts, Mc_PersistentProducts]:
		item.index = pd.Index(item.index)
		df = df.merge(item, how='left', left_index=True, right_index=True)
	for item in [trade_lib]:
		item.index = pd.Index(item.index)
		df = df.merge(item, how='left', left_index=True, right_index=True)
	#-Restore MultiIndex-#
	df.index = pd.MultiIndex.from_tuples(df.index, names=['country', 'year'])
	return df


#-NBER 1962 to 2000 Dataset-#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~#
dataset = 'D'
print "Processing NBER 1962 to 2000 for Sensativity Analysis Dataset %s..."%dataset
DATASET_DIR = TARGET_DATASET_DIR['nber']
DATA6200 = pd.read_hdf(DATASET_DIR+"nber-export-sitcr2l4-1962to2000.h5", key='D')
DATA6200 = DATA6200.rename(columns={'sitc4':'productcode'})
DATA6200 = attach_attributes(DATA6200, name="nberbaci", dtype="export", classification="SITC", \
						 revision=2, units_value_str="1000$", complete_dataset=True, notes="Dataset %s"%dataset)
DATA6200 = CPExportData(DATA6200, allow_mixed_productcode=True)

#-Settings-#
PROX_CUTOFFS = [0.1,0.2,0.3,"mean","median"]
STYLES = ["average", "base", "next"]
LOCAL_DIR = "sensativity-analysis/"

for prox_cutoff in PROX_CUTOFFS:
	for style in STYLES: 
		print "Computing Dataset D with prox_cutoff(%s) and style(%s) ..."%(prox_cutoff, style)
		#-Compute Regression Dataset-#
		df = compute_regression_dataset_with_choice(DATA6200, prox_cutoff, style)
		#-Store Stata-#
		df.to_stata(RESULTS_DIR+LOCAL_DIR+"nber-export-sitcr2l4-1962to2000-sensativity-dataset%s-proxcut(%s)style(%s).dta"%(dataset,prox_cutoff,style))
del DATA6200




##------------------##
##-Plots and Graphs-##
##------------------##

#-Graph for Prox_cutoff and Probable Emergence-#

def compute_probable_emergence(export_dataset, prox_cutoff):
	#-Export System-#
	dynples = export_dataset.to_dynamic_productlevelexportsystem()
	dynples = dynples.dynamic_global_panel()
	dynples.rca_matrices(complete_data=True)
	dynples.mcp_matrices()
	dynples.proximity_matrices()

	#-Compute Product Changes-#
	Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts = dynples.compute_product_changes()
	df_BothYears = reindex_dynamic_dataframe(from_dict_to_dataframe(Mcp_BothYears))
	Mc_BothYears = df_BothYears.groupby(level=['country', 'to_year']).sum()
	df_NewProducts = reindex_dynamic_dataframe(from_dict_to_dataframe(Mcp_NewProducts))
	Mc_NewProducts = df_NewProducts.groupby(level=['country', 'to_year']).sum()
	df_DieProducts = reindex_dynamic_dataframe(from_dict_to_dataframe(Mcp_DieProducts))
	Mc_DieProducts = df_DieProducts.groupby(level=['country', 'to_year']).sum()

	#-Compute Probable Emergence-#
	Mc_ProbableProducts, Mc_ImProbableProducts = dynples.compute_probable_improbable_emergence(prox_cutoff=prox_cutoff, style=style)
	Mc_ProbableProducts = from_dict_of_series_to(Mc_ProbableProducts, series_name='ProbableProducts')

	#-Compute Probable + Persistence-# 
	Mcp_ProbableProducts, Mcp_ImProbableProducts = dynples.compute_probable_improbable_emergence(prox_cutoff=prox_cutoff, style=style, output='reduced')
	Mc_ProbablePersistent = from_dict_of_series_to(compute_persistence(dynples.mcp, Mcp_ProbableProducts, output="summary"), series_name="ProbablePersistent")

	return Mc_ProbableProducts, Mc_ProbablePersistent

#-Graph for Dataset 'D'-#	
dataset = 'D'
print "Processing NBER 1962 to 2012 for Sensativity Analysis Dataset %s..."%dataset
DATASET_DIR = TARGET_DATASET_DIR['nberbaci96']
DATA6200 = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l4-1962to2012-harmonised.h5", key='D')
DATA6200 = DATA6200.rename(columns={'sitc4':'productcode'})
DATA6200 = attach_attributes(DATA6200, name="nberbaci", dtype="export", classification="SITC", \
						 revision=2, units_value_str="1000$", complete_dataset=True, notes="Dataset %s"%dataset)
DATA6200 = CPExportData(DATA6200, allow_mixed_productcode=True)

PROX_CUTOFFS = [0.1,0.2,0.3,"mean", "median"]
STYLE = "average"
LOCAL_DIR = "plots"

probable = {}
probablepersistent = {}
for prox_cutoff in PROX_CUTOFFS:
	print "Computing Dataset D with prox_cutoff(%s) and style(average) ..."%(prox_cutoff)
	#-Compute Regression Dataset-#
	ProbableProducts, ProbablePersistent = compute_probable_emergence(DATA6200, prox_cutoff)
	ProbableProducts.columns = [prox_cutoff]
	ProbablePersistent.columns = [prox_cutoff]
	probable[prox_cutoff] = ProbableProducts
	probablepersistent[prox_cutoff] = ProbablePersistent
#-Probable Emergence-#
for item in sorted(probable.keys()):
	if item == 0.1:
		data = probable[item]
	else:
		data = data.merge(probable[item], left_index=True, right_index=True)
data = data.groupby(level="year").sum()
ax = data.plot()

#-Plot-#


#-Graph for Dataset 'E'-#	
dataset = 'E'
print "Processing NBER 1962 to 2012 for Sensativity Analysis Dataset %s..."%dataset
DATASET_DIR = TARGET_DATASET_DIR['nberbaci96']
DATA6200 = pd.read_hdf(DATASET_DIR+"nber-export-sitcr2l4-1962to2012-harmonised.h5", key='D')
DATA6200 = DATA6200.rename(columns={'sitc4':'productcode'})
DATA6200 = attach_attributes(DATA6200, name="nberbaci", dtype="export", classification="SITC", \
						 revision=2, units_value_str="1000$", complete_dataset=True, notes="Dataset %s"%dataset)
DATA6200 = CPExportData(DATA6200, allow_mixed_productcode=True)

PROX_CUTOFFS = [0.1,0.2,0.3,"mean", "median"]
STYLE = "average"
LOCAL_DIR = "plots"

probable = {}
probablepersistent = {}
for prox_cutoff in PROX_CUTOFFS:
	print "Computing Dataset D with prox_cutoff(%s) and style(average) ..."%(prox_cutoff)
	#-Compute Regression Dataset-#
	ProbableProducts, ProbablePersistent = compute_probable_emergence(DATA6200, prox_cutoff, style)
	probable[prox_cutoff] = ProbableProducts
	probablepersistent[prox_cutoff] = ProbablePersistent

#-Plot-#