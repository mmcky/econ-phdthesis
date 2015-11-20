"""
Chapter 2+3: Theory of Comparative Advantage and Network Structure of Trade
===========================================================================

Graphs and Plots
----------------
1. Graphs and Plots used in Section 3
	a. Mcp Matrices (Sorted in Different Ways)
	b. Proximity Matrices (Sorted in Different Ways)
	c. DEU RCA Vector Sorted by PCI

Trade Inefficiency Plots 
------------------------
1. Plots that support Trade Inefficiency Section

Seriation - R
-------------
1. Prepare Simple Cross Section Dataset for R [Yr 2012]
	a. Trade Flows
	b. RCA Values

"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyeconlab import CPExportData, ProductLevelExportSystem
from pyeconlab.trade.util import attach_attributes
from pyeconlab.wdi import WDI

from dataset_info import TARGET_DATASET_DIR, CHAPTER_RESULTS, SOURCE_DIR

#-Setup Common Data-#
WDI_DIR = SOURCE_DIR["wdi"]
wdi = WDI(source_dir=WDI_DIR)

#-------------------#
#-Execution Control-#
#-------------------#
PLOTS = True
INEFFICIENT_STUDY = True
SERIATION = True


#------------------#
#-Plots and Charts-#
#------------------#

if PLOTS:

	#-Year:2000-#
	DATASET_DIR = TARGET_DATASET_DIR['nber']
	RESULTS_DIR = CHAPTER_RESULTS[2]
	DATASET = 'D'
	YEAR = 2000

	print "[INFO] Computing Plots for the Year %s from NBER datasets %s ..." % (YEAR, DATASET)

	data = pd.read_hdf(DATASET_DIR+"nber-export-sitcr2l4-1962to2000.h5", DATASET)
	# DATASET_DIR = TARGET_DATASET_DIR['nberbaci96']
	# data = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l4-1962to2012-harmonised.h5", DATASET) #Year 2012 Graphs
	data = data.loc[data.year == YEAR]
	data = data.rename(columns={'eiso3c':'country','sitc4':'productcode', 'value':'export'})
	data = data.reset_index()
	del data["index"]
	del data["year"]
	data = data.set_index(["country", "productcode"])

	s = ProductLevelExportSystem()
	s.from_df(data, country_classification="ISO3c", product_classification="SITCR2", compile_dtypes=["DataFrame"], year=YEAR)
	s.rca_matrix(complete_data=True)
	s.mcp_matrix()

	#-Mcp Plots-#
	#-Basic Mcp-#
	fig = s.plot_mcp(row_sortby_label="Alphabetical", column_sortby_label="SITC Code")
	plt.savefig(RESULTS_DIR + 'nber_mcp_alpha_numeric_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Sorted by Diversity and Ubiquity-#
	ubiquity = s.compute_ubiquity()
	diversity = s.compute_diversity()
	s.mcp = s.sorted_matrix(s.mcp, row_sortby=diversity, column_sortby=ubiquity, column_ascending=False)
	fig = s.plot_mcp(row_sortby_label="Diversification", column_sortby_label="Ubiquity")
	plt.savefig(RESULTS_DIR + 'nber_mcp_ubiquity_diversity_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Sorted by GDPPC and Ubiquity-#
	gdppc = wdi.series_long(series_code="NY.GDP.PCAP.CD").reorder_levels(["year","iso3c"]).ix[YEAR]["GDPPC"].copy()
	s.mcp = s.sorted_matrix(s.mcp, row_sortby=gdppc, column_sortby=ubiquity, column_ascending=False).dropna()
	fig = s.plot_mcp(row_sortby_label="GDPPC", column_sortby_label="Ubiquity")
	plt.savefig(RESULTS_DIR + 'nber_mcp_gdppc_diversity_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Sorted by ECI and PCI-#
	s = ProductLevelExportSystem()
	s.from_df(data, country_classification="ISO3c", product_classification="SITCR2", compile_dtypes=["DataFrame"], year=YEAR)
	s.rca_matrix(complete_data=True)
	s.mcp_matrix()
	eci = s.compute_eci(auto_adjust_sign=True)
	pci = s.compute_pci(auto_adjust_sign=True)
	s.mcp = s.sorted_matrix(s.mcp, row_sortby=eci.copy(), column_sortby=pci.copy())
	fig = s.plot_mcp(row_sortby_label="ECI", column_sortby_label="PCI")
	plt.savefig(RESULTS_DIR + 'nber_mcp_eci_pci_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Trade Shares-#
	cntryshare = s.country_shares()
	prodshare = s.product_shares()
	fig = s.plot_scaled_mcp_heatmap(s.mcp, cpdata_name="{0,1}", row_scaleby=cntryshare, column_scaleby=prodshare)
	plt.savefig(RESULTS_DIR + 'nber_mcp_eci(cntryshare)_pci(prodshare)_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-RCA Version-#
	s.rca = s.sorted_matrix(s.rca, row_sortby=eci.copy(), column_sortby=pci.copy())
	fig = s.plot_scaled_mcp_heatmap(s.rca, cpdata_name="RCA", row_scaleby=cntryshare, column_scaleby=prodshare, low_value_cutoff=1, high_value_cutoff=4)
	plt.savefig(RESULTS_DIR + 'nber_rca_eci(cntryshare)_pci(prodshare)_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Vector of DEU RCA from RCA sorted by PCI-#
	from pyeconlab.trade.dataset.NBERWTF.meta import iso3c_to_countryname
	for CNTRY in ["JPN", "DEU", "AUS","SAU","CAN","SWE","DNK","CZE","CHN","KEN","IDN","IND"]:
		srs = s.rca.ix[CNTRY]
		srs = pd.DataFrame([srs, pci]).T
		srs = srs.sort(columns=["PCI"])
		fig = srs[CNTRY].apply(lambda x: 4 if x >= 4 else x).plot(title="%s RCA Vector sorted by PCI [Yr %s]"%(iso3c_to_countryname[CNTRY],YEAR))
		fig.set_ylabel("RCA (Capped at 4)")
		fig.set_xlabel("SITC Revision 2 Level 4")
		plt.savefig(RESULTS_DIR+"nber_rcavector(%s)_sort(pci)_yr%s_dataset(%s).png"%(CNTRY,YEAR,DATASET))
		plt.clf()
	#-Proximity Plots-#
	#-Ricardian Plot-#
	s = ProductLevelExportSystem()
	s.from_df(data, country_classification="ISO3c", product_classification="SITCR2", compile_dtypes=["DataFrame"], year=YEAR)
	s.rca_matrix(complete_data=True)
	s.mcp_matrix()
	pci = s.compute_pci(auto_adjust_sign=True)
	s.proximity_matrix()
	fig = s.plot_proximity(prox_cutoff=0.6, sortby=pci.copy(), sortby_text="PCI", step=10)
	plt.savefig(RESULTS_DIR + 'nber_proximity_sort(pci)_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-K-Means HO Plot-#
		#-TBD-#
	#-Lall and Ubiquity Plots-#
		#-TBD-#
	
	#-Tables-#
	from pyeconlab.trade.classification import SITCR2
	sitc = SITCR2()
	sitc = sitc.code_description_dict()

	ec = eci.copy()
	ec.sort(ascending=False)
	ec = ec.to_frame().reset_index()
	ec["rank"] = ec["ECI"].rank(ascending=False)
	ec.to_excel(RESULTS_DIR + 'nber_eci_yr%s_dataset(%s).xlsx'%(YEAR,DATASET))
	pc = pci.copy()
	pc.sort(ascending=False)
	pc = pc.to_frame().reset_index()
	pc["rank"] = pc["PCI"].rank(ascending=False)
	pc["description"] = pc["index"].apply(lambda x: sitc[x])
	pc.to_excel(RESULTS_DIR + 'nber_pci_yr%s_dataset(%s).xlsx'%(YEAR,DATASET))

	pr = s.proximity.stack().reset_index()
	pr = pr.rename(columns={0:'proximity','productcode1':'p1','productcode2':'p2'})
	pr["p1-description"] = pr["p1"].apply(lambda x: sitc[x])
	pr = pr.merge(pc[["index","rank"]],left_on="p1", right_on="index")
	del pr["index"]
	pr = pr.rename(columns={'rank':'p1-rank'})
	pr["p2-description"] = pr["p2"].apply(lambda x: sitc[x])
	pr = pr.merge(pc[["index","rank"]],left_on="p2", right_on="index")
	del pr["index"]
	pr = pr.rename(columns={'rank':'p2-rank'})
	pr.to_excel(RESULTS_DIR+'nber_proximity_yr%s_dataset(%s).xlsx'%(YEAR,DATASET))
	pr = pr.loc[pr.proximity != 1]
	pr.sort(columns=["proximity"], ascending=False)
	pr.to_excel(RESULTS_DIR+'nber_proximity_yr%s_dataset(%s)_sorted.xlsx'%(YEAR,DATASET))

	#-2012-#
	DATASET_DIR = TARGET_DATASET_DIR['nberbaci96']
	DATASET = 'D'
	RESULTS_DIR = CHAPTER_RESULTS[2]
	YEAR = 2012

	print "[INFO] Computing Plots for the Year %s from NBER datasets %s ..." % (YEAR, DATASET)

	data = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l4-1962to2012-harmonised.h5", DATASET)
	data = data.loc[data.year == YEAR]
	data = data.rename(columns={'eiso3c':'country','sitc4':'productcode', 'value':'export'})
	data = data.reset_index()
	del data["index"]
	del data["year"]
	data = data.set_index(["country", "productcode"])

	s = ProductLevelExportSystem()
	s.from_df(data, country_classification="ISO3c", product_classification="SITCR2", compile_dtypes=["DataFrame"], year=YEAR)
	s.rca_matrix(complete_data=True)
	s.mcp_matrix()

	#-Mcp Plots-#
	#-Basic Mcp-#
	fig = s.plot_mcp(row_sortby_label="Alphabetical", column_sortby_label="SITC Code")
	plt.savefig(RESULTS_DIR + 'nberbaci96_mcp_alpha_numeric_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Sorted by Diversity and Ubiquity-#
	ubiquity = s.compute_ubiquity()
	diversity = s.compute_diversity()
	s.mcp = s.sorted_matrix(s.mcp, row_sortby=diversity, column_sortby=ubiquity, column_ascending=False)
	fig = s.plot_mcp(row_sortby_label="Diversification", column_sortby_label="Ubiquity")
	plt.savefig(RESULTS_DIR + 'nberbaci96_mcp_ubiquity_diversity_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Sorted by GDPPC and Ubiquity-#
	gdppc = wdi.series_long(series_code="NY.GDP.PCAP.CD").reorder_levels(["year","iso3c"]).ix[YEAR]["GDPPC"].copy()
	s.mcp = s.sorted_matrix(s.mcp, row_sortby=gdppc, column_sortby=ubiquity, column_ascending=False).dropna()
	fig = s.plot_mcp(row_sortby_label="GDPPC", column_sortby_label="Ubiquity")
	plt.savefig(RESULTS_DIR + 'nberbaci96_mcp_gdppc_diversity_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Sorted by ECI and PCI-#
	s = ProductLevelExportSystem()
	s.from_df(data, country_classification="ISO3c", product_classification="SITCR2", compile_dtypes=["DataFrame"], year=YEAR)
	s.rca_matrix(complete_data=True)
	s.mcp_matrix()
	eci = s.compute_eci(auto_adjust_sign=True)
	pci = s.compute_pci(auto_adjust_sign=True)
	s.mcp = s.sorted_matrix(s.mcp, row_sortby=eci.copy(), column_sortby=pci.copy())
	fig = s.plot_mcp(row_sortby_label="ECI", column_sortby_label="PCI")
	plt.savefig(RESULTS_DIR + 'nberbaci96_mcp_eci_pci_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Trade Shares-#
	cntryshare = s.country_shares()
	prodshare = s.product_shares()
	fig = s.plot_scaled_mcp_heatmap(s.mcp, cpdata_name="{0,1}", row_scaleby=cntryshare, column_scaleby=prodshare)
	plt.savefig(RESULTS_DIR + 'nberbaci96_mcp_eci(cntryshare)_pci(prodshare)_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-RCA Version-#
	s.rca = s.sorted_matrix(s.rca, row_sortby=eci.copy(), column_sortby=pci.copy())
	fig = s.plot_scaled_mcp_heatmap(s.rca, cpdata_name="RCA", row_scaleby=cntryshare, column_scaleby=prodshare, low_value_cutoff=1, high_value_cutoff=4)
	plt.savefig(RESULTS_DIR + 'nberbaci96_rca_eci(cntryshare)_pci(prodshare)_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Vector of DEU RCA from RCA sorted by PCI-#
	from pyeconlab.trade.dataset.NBERWTF.meta import iso3c_to_countryname
	for CNTRY in ["JPN", "DEU", "AUS","SAU","CAN","SWE","DNK","CZE","CHN","KEN","IDN","IND"]:
		srs = s.rca.ix[CNTRY]
		srs = pd.DataFrame([srs, pci]).T
		srs = srs.sort(columns=["PCI"])
		fig = srs[CNTRY].apply(lambda x: 4 if x >= 4 else x).plot(title="%s RCA Vector sorted by PCI [Yr %s]"%(iso3c_to_countryname[CNTRY],YEAR))
		fig.set_ylabel("RCA (Capped at 4)")
		fig.set_xlabel("SITC Revision 2 Level 4")
		plt.savefig(RESULTS_DIR+"nberbaci96_rcavector(%s)_sort(pci)_yr%s_dataset(%s).png"%(CNTRY,YEAR,DATASET))
		plt.clf()
	#-Proximity Plots-#
	#-Ricardian Plot-#
	s = ProductLevelExportSystem()
	s.from_df(data, country_classification="ISO3c", product_classification="SITCR2", compile_dtypes=["DataFrame"], year=YEAR)
	s.rca_matrix(complete_data=True)
	s.mcp_matrix()
	pci = s.compute_pci(auto_adjust_sign=True)
	s.proximity_matrix()
	fig = s.plot_proximity(prox_cutoff=0.6, sortby=pci.copy(), sortby_text="PCI", step=10)
	plt.savefig(RESULTS_DIR + 'nberbaci96_proximity_sort(pci)_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-K-Means HO Plot-#
		#-TBD-#
	#-Lall and Ubiquity Plots-#
		#-TBD-#

	#-Tables-#
	from pyeconlab.trade.classification import SITCR2
	sitc = SITCR2()
	sitc = sitc.code_description_dict()

	ec = eci.copy()
	ec.sort(ascending=False)
	ec = ec.to_frame().reset_index()
	ec["rank"] = ec["ECI"].rank(ascending=False)
	ec.to_excel(RESULTS_DIR + 'nberbaci96_eci_yr%s_dataset(%s).xlsx'%(YEAR,DATASET))
	pc = pci.copy()
	pc.sort(ascending=False)
	pc = pc.to_frame().reset_index()
	pc["rank"] = pc["PCI"].rank(ascending=False)
	pc["description"] = pc["index"].apply(lambda x: sitc[x])
	pc.to_excel(RESULTS_DIR + 'nberbaci96_pci_yr%s_dataset(%s).xlsx'%(YEAR,DATASET))

	pr = s.proximity.stack().reset_index()
	pr = pr.rename(columns={0:'proximity','productcode1':'p1','productcode2':'p2'})
	pr["p1-description"] = pr["p1"].apply(lambda x: sitc[x])
	pr = pr.merge(pc[["index","rank"]],left_on="p1", right_on="index")
	del pr["index"]
	pr = pr.rename(columns={'rank':'p1-rank'})
	pr["p2-description"] = pr["p2"].apply(lambda x: sitc[x])
	pr = pr.merge(pc[["index","rank"]],left_on="p2", right_on="index")
	del pr["index"]
	pr = pr.rename(columns={'rank':'p2-rank'})
	pr.to_excel(RESULTS_DIR+'nberbaci96_proximity_yr%s_dataset(%s).xlsx'%(YEAR,DATASET))
	pr = pr.loc[pr.proximity != 1]
	pr.sort(columns=["proximity"], ascending=False)
	pr.to_excel(RESULTS_DIR+'nberbaci96_proximity_yr%s_dataset(%s)_sorted.xlsx'%(YEAR,DATASET))

#--------------------------#
#-Trade Inefficiency Study-#
#--------------------------#


if INEFFICIENT_STUDY:

	from pyeconlab import DynamicProductLevelExportSystem

	DATASET_DIR = TARGET_DATASET_DIR['nberbaci96']
	RESULTS_DIR = CHAPTER_RESULTS[2]
	
	#------------------------#
	#-Plot Using Dataset 'D'-#
	#------------------------#

	DATASET = 'D'
	data = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l4-1962to2012-harmonised.h5", DATASET) 	#-Intertemporal Consistent Data-#
	data = data.rename(columns={'eiso3c' : 'country', 'sitc4' : 'productcode', 'value' : 'export'})
	data = data.set_index(["year"])
	s = DynamicProductLevelExportSystem()
	s.from_df(data)
	s.rca_matrices(complete_data=True)
	s.mcp_matrices()
	s.compute_eci()
	s.auto_adjust_eci_sign()
	s.compute_pci()
	s.auto_adjust_pci_sign()

	#-Plot for Year 2000-#
	YEAR = 2000
	xs = s[YEAR]
	itdata = xs.identify_inefficient_trade(row_ascending=True, column_ascending=True).copy(deep=True)
	#-Use Legacy Functions (For Plotting)-#
	from pyeconlab.trade.util.plotting import prepare_scaling_sortby_vectors, plot_scaled_mcp_heatmap_v3 	
	from matplotlib import cm
	graph_data, row_scaleby, column_scaleby = prepare_scaling_sortby_vectors(itdata, row_scaleby=xs.total_country_export, column_scaleby=xs.total_product_export)
	fig = plot_scaled_mcp_heatmap_v3(graph_data, value_type="Inefficiency Metric", row_scaleby=row_scaleby, row_label=('ECI', 'Country Export Share'), column_scaleby=column_scaleby, cmap=cm.RdBu) 
	plt.savefig(RESULTS_DIR + 'nberbaci_mcp_eci(cntryshare)_pci(prodshare)_inefficientmetric_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Save Associated Graph Data-#
	graph_data.to_excel(RESULTS_DIR + "nberbaci_mcp_eci(cntryshare)_pci(prodshare)_inefficientmetric_yr%s_dataset(%s)_data.xlsx"%(YEAR,DATASET))

	#-Time Series Plot of Values-#
	ts_data = []
	for year in range(1962,2012+1,1):
		eci_year = s[year].eci
		pci_year = s[year].pci
		data = s[year].identify_inefficient_trade() 
		masktable = data.applymap(lambda x: 1 if x < -0.5 else np.nan)
		values = s[year].as_cp_matrix(droplevel=True)
		values = values.align(masktable, join='right')
		inefficient_value = (values[0] * values[1]).sum().sum()
		ts_data.append(inefficient_value)
	ts_df = pd.DataFrame(ts_data, index=range(1962,2012+1,1)) 
	fig = ts_df.plot(legend=False, title="Inefficient Trade [Deviation >= -0.5]")
	fig.set_xlabel("Year")
	fig.set_ylabel("US$ (1000's)")
	fig.set_ylim(bottom=0)
	fig.xaxis.axes.set_xticks(xrange(1965,2012+1,5))
	plt.savefig(RESULTS_DIR + "nberbaci_inefficient_trade_by_value_0.5deviation_ts_dataset(%s).png"%DATASET, dpi=400)
	plt.clf()

	#As Percentage of World Trade #
	ts_data = []
	for year in range(1962,2012+1,1):
		eci_year = s[year].eci
		pci_year = s[year].pci
		data = s[year].identify_inefficient_trade() 
		masktable = data.applymap(lambda x: 1 if x < -0.5 else np.nan)
		values = s[year].as_cp_matrix(droplevel=True)
		world_value = values.sum().sum()
		values = values.align(masktable, join='right')
		inefficient_value = (values[0] * values[1]).sum().sum()
		ts_data.append(inefficient_value / world_value)
	ts_df = pd.DataFrame(ts_data, index=range(1962,2012+1,1)) 
	fig = (ts_df*100).plot(legend=False, title="Inefficient Trade % of World Trade [Deviation >= -0.5]")
	fig.set_xlabel("Year")
	fig.set_ylabel("Percent")
	fig.set_ylim(bottom=0)
	fig.xaxis.axes.set_xticks(xrange(1965,2012+1,5))
	plt.savefig(RESULTS_DIR + "nberbaci_inefficient_trade_by_prcwrldtrade_0.5deviation_ts_dataset(%s).png"%DATASET, dpi=400)
	plt.clf()


	#-Cutoff Comparison Plot-#
	tdf_data = {}
	for cutoff in [-0.75, -0.5, -0.25]:
		print "Processing cutoff (%s)"%cutoff
		#-Compute Each Graph-#
		ts_data = []
		for year in range(1962,2012+1,1):
			eci_year = s[year].eci
			pci_year = s[year].pci
			data = s[year].identify_inefficient_trade() 
			masktable = data.applymap(lambda x: 1 if x < cutoff else np.nan)
			values = s[year].as_cp_matrix(droplevel=True)
			world_value = values.sum().sum()
			values = values.align(masktable, join='right')
			inefficient_value = (values[0] * values[1]).sum().sum()
			ts_data.append(inefficient_value / world_value)
		tdf_data[str(cutoff)] = pd.Series(ts_data, index=range(1962,2012+1,1)) 
	tdf_df = pd.DataFrame(tdf_data, index=range(1962,2012+1,1))
	fig = (tdf_df*100).plot(title="Inefficient Trade % of World Trade")
	fig.set_xlabel("Year")
	fig.set_ylabel("Percent")
	fig.set_ylim(bottom=0)
	fig.xaxis.axes.set_xticks(xrange(1965,2012+1,5))
	plt.savefig(RESULTS_DIR + "nberbaci_inefficient_trade_by_prcwrldtrade_various_deviations_ts_dataset(%s).png"%DATASET, dpi=600)
	plt.clf()

	#------------------------#
	#-Plot Using Dataset 'E'-#
	#------------------------#

	DATASET = 'E'
	data = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l4-1962to2012-harmonised.h5", DATASET) 	#-Intertemporal Consistent Data-#
	data = data.rename(columns={'eiso3c' : 'country', 'sitc4' : 'productcode', 'value' : 'export'})
	data = data.set_index(["year"])
	s = DynamicProductLevelExportSystem()
	s.from_df(data)
	s.rca_matrices(complete_data=True)
	s.mcp_matrices()
	s.compute_eci()
	s.auto_adjust_eci_sign()
	s.compute_pci()
	s.auto_adjust_pci_sign()

	#-Plot for Year 2000-#
	YEAR = 2000
	xs = s[YEAR]
	itdata = xs.identify_inefficient_trade(row_ascending=True, column_ascending=True)
	#-Use Legacy Functions (For Plotting)-#
	from pyeconlab.trade.util.plotting import prepare_scaling_sortby_vectors, plot_scaled_mcp_heatmap_v3 	
	from matplotlib import cm
	graph_data, row_scaleby, column_scaleby = prepare_scaling_sortby_vectors(itdata, row_scaleby=xs.total_country_export, column_scaleby=xs.total_product_export)
	fig = plot_scaled_mcp_heatmap_v3(graph_data, value_type="Inefficiency Metric", row_scaleby=row_scaleby, row_label=('ECI', 'Country Export Share'), column_scaleby=column_scaleby, cmap=cm.RdBu) 
	plt.savefig(RESULTS_DIR + 'nberbaci_mcp_eci(cntryshare)_pci(prodshare)_inefficientmetric_yr%s_dataset(%s).png'%(YEAR,DATASET), dpi=600)
	plt.clf()
	#-Save Associated Graph Data-#
	graph_data.to_excel(RESULTS_DIR + "nberbaci_mcp_eci(cntryshare)_pci(prodshare)_inefficientmetric_yr%s_dataset(%s)_data.xlsx"%(YEAR,DATASET))

	#-Time Series Plot of Values-#
	ts_data = []
	for year in range(1962,2012+1,1):
		eci_year = s[year].eci
		pci_year = s[year].pci
		data = s[year].identify_inefficient_trade() 
		masktable = data.applymap(lambda x: 1 if x < -0.5 else np.nan)
		values = s[year].as_cp_matrix(droplevel=True)
		values = values.align(masktable, join='right')
		inefficient_value = (values[0] * values[1]).sum().sum()
		ts_data.append(inefficient_value)
	ts_df = pd.DataFrame(ts_data, index=range(1962,2012+1,1)) 
	fig = ts_df.plot(legend=False, title="Inefficient Trade [Deviation >= -0.5]")
	fig.set_xlabel("Year")
	fig.set_ylabel("$ (1000's)")
	plt.savefig(RESULTS_DIR + "nberbaci_inefficient_trade_by_value_0.5deviation_ts_dataset(%s).png"%DATASET)
	plt.clf()

	#As Percentage of World Trade #
	ts_data = []
	for year in range(1962,2012+1,1):
		eci_year = s[year].eci
		pci_year = s[year].pci
		data = s[year].identify_inefficient_trade() 
		masktable = data.applymap(lambda x: 1 if x < -0.5 else np.nan)
		values = s[year].as_cp_matrix(droplevel=True)
		world_value = values.sum().sum()
		values = values.align(masktable, join='right')
		inefficient_value = (values[0] * values[1]).sum().sum()
		ts_data.append(inefficient_value / world_value)
	ts_df = pd.DataFrame(ts_data, index=range(1962,2012+1,1)) 
	fig = (ts_df*100).plot(legend=False, title="Inefficient Trade % of World Trade [Deviation >= -0.5]")
	fig.set_xlabel("Year")
	fig.set_ylabel("Percent")
	plt.savefig(RESULTS_DIR + "nberbaci_inefficient_trade_by_prcwrldtrade_0.5deviation_ts_dataset(%s).png"%DATASET)
	plt.clf()


	#-Cutoff Comparison Plot-#
	tdf_data = {}
	for cutoff in [-0.75, -0.5, -0.25]:
		print "Processing cutoff (%s)"%cutoff
		#-Compute Each Graph-#
		ts_data = []
		for year in range(1962,2012+1,1):
			eci_year = s[year].eci
			pci_year = s[year].pci
			data = s[year].identify_inefficient_trade() 
			masktable = data.applymap(lambda x: 1 if x < cutoff else np.nan)
			values = s[year].as_cp_matrix(droplevel=True)
			world_value = values.sum().sum()
			values = values.align(masktable, join='right')
			inefficient_value = (values[0] * values[1]).sum().sum()
			ts_data.append(inefficient_value / world_value)
		tdf_data[str(cutoff)] = pd.Series(ts_data, index=range(1962,2012+1,1)) 
	tdf_df = pd.DataFrame(tdf_data, index=range(1962,2012+1,1))
	fig = (tdf_df*100).plot(title="Inefficient Trade % of World Trade")
	fig.set_xlabel("Year")
	fig.set_ylabel("Percent")
	plt.savefig(RESULTS_DIR + "nberbaci_inefficient_trade_by_prcwrldtrade_various_deviations_ts_dataset(%s).png"%DATASET, dpi=600)
	plt.clf()

#----------------------------#
#-Country Similarity Network-#
#----------------------------#

#-Year:2000-#
DATASET_DIR = TARGET_DATASET_DIR['nber']
RESULTS_DIR = CHAPTER_RESULTS[2]
DATASET = 'D'
YEAR = 2000

data = pd.read_hdf(DATASET_DIR+"nber-export-sitcr2l4-1962to2000.h5", DATASET)
# DATASET_DIR = TARGET_DATASET_DIR['nberbaci96']
# data = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l4-1962to2012-harmonised.h5", DATASET) #Year 2012 Graphs
data = data.loc[data.year == YEAR]
data = data.rename(columns={'eiso3c':'country','sitc4':'productcode', 'value':'export'})
data = data.reset_index()
del data["index"]
del data["year"]
data = data.set_index(["country", "productcode"])

s = ProductLevelExportSystem()
s.from_df(data, country_classification="ISO3c", product_classification="SITCR2", compile_dtypes=["DataFrame"], year=YEAR)
s.rca_matrix(complete_data=True)
s.mcp_matrix()
s.compute_country_proximity(fillna=True)
#s.compute_country_proximity(matrix_type='pearsons', fillna=True)

#-Network-#
#-Minimum Spanning Tree for GEPHI-#
prox = s.country_proximity
import networkx as nx
g = nx.from_numpy_matrix(prox.values)
g = nx.relabel_nodes(g, dict(enumerate(prox.columns)))
mst = nx.minimum_spanning_tree(g)
print len(mst.nodes())
print len(mst.edges())
#-MST and Edges Above 0.4-#
for n,nb,d in g.edges_iter(data=True):
    d = d['weight']
    if d == 1:
        continue
    if d >= 0.40:
        mst.add_edge(n,nb,attr_dict={'weight':d})
print len(mst.nodes())
print len(mst.edges())
#-Export to GEXF File-#
nx.write_gexf(mst, RESULTS_DIR+'cntry-prox-mst-with-prox(0-40).gexf')

#-----------#
#-Seriation-#
#-----------#

if SERIATION:

	#-Prepare Datasets for Year 2012 For Seriation Study Using R-#

	DATASET_DIR = TARGET_DATASET_DIR['nberbaci96']
	RESULTS_DIR = CHAPTER_RESULTS['D'] 					

	N = 10 	#-Top Number of Products-#

	#-Export Values-#
	for level in [4,3,2]:
		for dataset in ["C","E"]:
			print "Converting VALUE Dataset for Seriation ... Level:%s; Dataset:%s"%(level, dataset)
			DATA6212 = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l%s-1962to2012-harmonised.h5"%level, key=dataset)
			DATA6212 = DATA6212.rename(columns={'sitc%s'%level:'productcode'})
			#-2012-#
			DATA2012 = DATA6212.loc[DATA6212.year == 2012]
			DATA2012.reset_index(inplace=True)
			del DATA2012["index"] 
			DATA2012.to_csv(RESULTS_DIR + "seriation-nberbaci-export-sitcr2l%s-xs2012-dataset%s.csv"%(level,dataset), index=False)
			#-Top 10 Data (by Value) for Each Country-#
			TOPN = DATA2012.sort(columns="value", ascending=False)
			TOPN = TOPN.groupby("eiso3c").head(N)
			TOPN = TOPN.sort()
			TOPN.to_csv(RESULTS_DIR + "seriation-nberbaci-export-sitcr2l%s-xs2012-dataset%s_top(%s).csv"%(level,dataset,N), index=False)
			TOPN = TOPN.set_index(["year", "eiso3c", "productcode"])
			TOPN = TOPN.unstack(level="productcode").fillna(0.0)
			TOPN.columns = TOPN.columns.droplevel()
			TOPN = TOPN.reset_index()
			del TOPN["year"]
			TOPN.to_csv(RESULTS_DIR + "seriation-nberbaci-export-sitcr2l%s-xs2012-dataset%s_top(%s)_allproductcodes.csv"%(level,dataset,N), index=False)

	#-RCA Values-#
	for level in [4,3,2]:
		for dataset in ["C","E"]:
			print "Converting RCA/MCP Dataset for Seriation ... Level:%s; Dataset:%s"%(level, dataset)
			DATA6212 = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l%s-1962to2012-harmonised.h5"%level, key=dataset)
			DATA6212 = DATA6212.rename(columns={'sitc%s'%level:'productcode'})
			DATA6212 = attach_attributes(DATA6212, name="nberbaci", dtype="export", classification="SITC", \
								 revision=2, units_value_str="1000$", complete_dataset=True, notes="Dataset %s"%dataset)
			DATA6212 = CPExportData(DATA6212, allow_mixed_productcode=True)
			DATA6212 = DATA6212.to_dynamic_productlevelexportsystem()
			#-2012-#
			DATA2012 = DATA6212[2012]
			DATA2012.rca_matrix(fillna=True, complete_data=True)
			#-Remove Values Less than 1-#
			DATA2012.rca = DATA2012.rca.applymap(lambda x: 0 if x < 1 else x) 	
			DATA2012.rca.to_csv(RESULTS_DIR + "seriation-nberbaci-export(rca)-sitcr2l%s-xs2012-dataset%s.csv"%(level,dataset))
			DATA2012.mcp_matrix(cutoff=1)
			DATA2012.mcp.to_csv(RESULTS_DIR + "seriation-nberbaci-export(mcp)-sitcr2l%s-xs2012-dataset%s.csv"%(level,dataset))
			#-Top 10 Data (by RCA) for Each Country-#
			TOPN = DATA2012.rca.unstack()
			TOPN.name = "rca"
			TOPN = TOPN.reset_index().sort(columns="rca", ascending=False)
			TOPN = TOPN.groupby("country").head(N)
			TOPN = TOPN.set_index(["country", "productcode"])
			TOPN = TOPN.sort()
			TOPN = TOPN.reset_index()
			TOPN.to_csv(RESULTS_DIR + "seriation-nberbaci-export(rca)-sitcr2l%s-xs2012-dataset%s_top(%s).csv"%(level,dataset,N), index=False)
			TOPN = TOPN.set_index(["country", "productcode"])
			TOPN = TOPN.unstack(level="productcode").fillna(0.0)
			TOPN.columns = TOPN.columns.droplevel()
			TOPN = TOPN.reset_index()
			TOPN.to_csv(RESULTS_DIR + "seriation-nberbaci-export(rca)-sitcr2l%s-xs2012-dataset%s_top(%s)_allproductcodes.csv"%(level,dataset,N), index=False)

	#-Yu RCA Values-#
	for level in [4]:
		for dataset in ["C","E"]:
			print "Converting Yu RCA/MCP Dataset for Seriation ... Level:%s; Dataset:%s"%(level, dataset)
			DATA6212 = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l%s-1962to2012-harmonised.h5"%level, key=dataset)
			DATA6212 = DATA6212.rename(columns={'sitc%s'%level:'productcode'})
			DATA6212 = attach_attributes(DATA6212, name="nberbaci", dtype="export", classification="SITC", \
								 revision=2, units_value_str="1000$", complete_dataset=True, notes="Dataset %s"%dataset)
			DATA6212 = CPExportData(DATA6212, allow_mixed_productcode=True)
			DATA6212 = DATA6212.to_dynamic_productlevelexportsystem()
			#-2012-#
			DATA2012 = DATA6212[2012]
			DATA2012.yu_rca_matrix(fillna=True, set_property=True)
			#-Remove Values Less than 0-#
			DATA2012.rca = DATA2012.rca.applymap(lambda x: 0 if x < 0 else x) 	
			DATA2012.rca.to_csv(RESULTS_DIR + "seriation-nberbaci-export-yurca-sitcr2l%s-xs2012-dataset%s.csv"%(level,dataset))
			DATA2012.mcp_matrix(cutoff=0)
			DATA2012.mcp.to_csv(RESULTS_DIR + "seriation-nberbaci-export-yumcp-sitcr2l%s-xs2012-dataset%s.csv"%(level,dataset))
			#-Top 10 Data (by RCA) for Each Country-#
			TOPN = DATA2012.rca.unstack()
			TOPN.name = "rca"
			TOPN = TOPN.reset_index().sort(columns="rca", ascending=False)
			TOPN = TOPN.groupby("country").head(N)
			TOPN = TOPN.set_index(["country", "productcode"])
			TOPN = TOPN.sort()
			TOPN = TOPN.reset_index()
			TOPN.to_csv(RESULTS_DIR + "seriation-nberbaci-export-yurca-sitcr2l%s-xs2012-dataset%s_top%s.csv"%(level,dataset,N), index=False)
			TOPN = TOPN.set_index(["country", "productcode"])
			TOPN = TOPN.unstack(level="productcode").fillna(0.0)
			TOPN.columns = TOPN.columns.droplevel()
			TOPN = TOPN.reset_index()
			TOPN.to_csv(RESULTS_DIR + "seriation-nberbaci-export-yurca-sitcr2l%s-xs2012-dataset%s_top%s_allproductcodes.csv"%(level,dataset,N), index=False)



#----------------#
#-Results from R-#
#----------------#

#-BEA_TSP-RCA-100000-#
# Method: BEA_TSP
# Data Used: RCA Data (None < 1)
# Iterations: 100,000

#-Run #1-#
TSP_R1CO = ["MAR","JOR","SOM","ETH","DJI","BFA","MLI","UGA","MOZ","MWI","ZWE","NCL","CUB","DOM","NIC","GMB","CAF","BEN","GNB","MMR","RWA","BOL","COG","LAO","PRY","PAK","NPL","AFG","TCD","SDN","NER","ZAF","GAB","GHA","CIV","CMR","LBR","SLE","KNA","KIR","WSM","FJI","KHM","LKA","KEN","MDG","SYR","TGO","SEN","ARG","URY","GUY","SUR","GIN","JAM","AUS","FLK","SHN","GRL","SPM","ISL","SYC","MUS","BLZ","GTM","ECU","CRI","HND","SLV","HTI","BGD","BDI","YEM","NZL","EST","LVA","LTU","BIH","ALB","LBN","MKD","ZMB","GRC","UZB","TKM","IND","PNG","EGY","PRK","VNM","THA","IDN","PHL","PAN","BHS","MDA","BGR","UKR","HRV","SVN","AUT","FIN","SWE","TTO","VEN","OMN","GEO","ARM","CHL","PER","MNG","IRN","TUR","PRT","ESP","TUN","ISR","CYP","KGZ","TJK","BHR","MRT","BMU","BRB","DNK","NLD","BEL","POL","CZE","SVK","HUN","DEU","ITA","CHN","MAC","CHE","IRL","SGP","TWN","JPN","USA","CAN","KAZ","RUS","BRA","COL","MEX","BLR","GBR","HKG","MYS","GNQ","QAT","DZA","NOR","MLT","KOR","GIB","KWT","SAU","ARE","AZE","IRQ","NGA","AGO","LBY","TZA"]
TSP_R1PO = ["2690","2860","5241","1212","2771","2872","1221","2450","0012","0011","9410","2119","2876","0741","2114","2112","2654","0752","2922","0576","2685","6592","6593","6545","2640","8462","8451","8464","6121","2713","4233","6597","6612","4234","2472","4314","2631","2634","2223","0343","0350","0360","0342","4243","7933","2879","0711","2225","2235","6115","2117","2116","2683","2687","2785","2927","6116","0470","0615","3415","0582","1110","2924","0575","2120","2238","0542","1213","1211","6821","2871","2231","0421","6349","2873","0573","0585","5513","2651","6673","2923","6129","2481","2877","6341","0721","0723","2479","2632","2633","6513","6521","6584","2814","6330","2440","6576","2875","2874","3221","3231","2742","2614","6712","8424","8423","8441","2652","0371","4111","0814","6841","3232","6842","0812","2221","0577","2232","8442","6574","8472","8465","8443","8459","8463","0612","2224","2732","4236","7915","6725","6123","6852","5622","5222","2667","2783","2734","6594","5981","5322","6516","8471","6978","6571","8484","6581","8439","8434","8435","8433","0341","0344","0372","2919","0574","6539","6724","0583","0541","0545","0112","2682","0230","0224","2471","2512","3224","0451","2460","0548","2911","2483","2320","7932","3352","8960","5833","334","6713","5225","6716","2816","2815","3222","0115","2686","0482","0111","2222","0742","4232","0813","0459","6871","2890","2239","3414","8421","8422","8429","8431","8452","8928","2733","7852","8461","6259","2659","2655","7521","8811","4241","4249","9710","5541","6651","1124","0616","0118","0141","0460","3351","6611","6861","2613","2782","8432","0422","4312","2926","7931","6515","6518","6899","0440","0412","6114","6512","4113","2681","5323","5623","0544","5629","0571","2731","4235","8483","0565","0579","2712","2517","0452","6412","7251","7259","2482","6343","2226","0430","7912","6419","3353","0411","0564","6546","7731","1123","7169","7643","7188","8997","0751","8482","4242","4244","3223","7914","2111","2820","2882","8851","8989","8981","5413","8973","4245","2929","8481","6534","6589","6522","0483","8991","0484","6932","5114","5224","7911","5621","8941","6872","5157","5146","5514","5156","0015","0223","0240","0116","7213","1222","6891","1122","1121","6731","6732","6595","6596","2672","5543","0620","6424","6417","6951","6618","7711","6517","4313","7761","5921","7612","5823","6635","7723","0914","0488","5911","7163","5913","7922","5239","6672","6812","5145","6811","2234","2784","2741","3413","3330","5121","0589","6671","0811","0572","2786","2516","2518","6415","6418","6413","6935","5722","5232","0586","6575","6831","2519","6411","4239","7928","7923","0129","0114","8946","6351","6931","2789","6924","8812","7764","7599","8720","1223","0722","0712","8122","8932","6863","6851","6644","6549","6613","7131","2711","6353","6359","6342","7268","6519","6582","9510","7112","6511","5249","4311","5335","0142","6744","5989","7436","0561","8731","7842","7611","8121","0251","8510","8211","8219","0013","5169","0121","0113","7161","5415","8996","5416","5147","7271","7272","2772","5122","6122","6583","6514","6783","6543","6118","6542","7248","6535","6544","3345","7224","2666","7621","6665","5822","6352","7518","7132","8732","7783","8951","6631","6344","6416","5922","2925","0980","6421","0481","8931","7921","3354","7938","7223","7628","7622","6281","5842","0149","5123","6781","5231","5912","6130","8212","8922","7119","7111","7219","9610","6832","2881","6623","6760","7919","6591","7148","6733","7753","7751","6652","7752","8921","6423","6422","8972","8852","6674","6552","6553","8998","8939","6770","6749","6532","6560","5821","6551","8942","6975","6624","6973","6633","0252","8993","6822","7263","8310","6974","6354","5221","6793","5852","6646","7523","7754","0014","5542","0546","0730","0819","5530","5417","7267","6954","7368","7187","7841","5851","5838","6745","6746","6727","6747","6252","6251","7868","7782","7283","7493","7239","6912","6911","6794","7432","8822","5839","8741","2511","5914","6880","6960","6282","6428","5334","6921","7822","7631","7861","7372","6112","6254","6210","6289","6664","6579","2332","6658","2671","7411","7422","7211","7449","7281","7371","7362","7913","7849","7732","7129","6998","7757","7758","7434","7591","7525","7763","7768","8983","7722","6993","8842","6538","6531","2665","6645","5835","5834","5138","8710","7133","6643","5843","8821","8813","7269","7367","7361","7369","7252","7244","7246","8742","7788","8924","7243","7512","7528","6648","0619","7491","8935","6991","5139","5233","5721","6577","6632","7162","6994","7721","5419","7741","6572","5332","5411","5837","7416","7264","6253","7924","7929","5841","7511","8748","5154","5982","8999","7233","7234","7641","7832","7821","8830","7415","7638","7853","6649","7712","6647","7414","7412","7452","7413","5825","0913","7435","7429","7421","7245","5836","5829","5161","5831","5832","5112","5111","2331","7831","6996","7212","8743","6639","6637","8745","7423","7441","7451","7784","7442","7373","7284","7499","6997","7139","7810","5414","5312","5311","5163","6541","7851","7138","7149","7144","6642","8994","8933","6666","7522","5723","8947","7649","8841","5826","7648","6940","6953","5824","5331","5849","7742","8744","8959","5155","7439","8749","7762","6641","5827","5113","5162","5137","8974","7428","7492","7247","6785","6782","7126","6638","5148","7431","7781","8982","6536","7642","8952","6992","6999","5223","5983","8124","6573","0611"]

#-Run #2-#
TSP_R2CO = ["TUN","MAR","JOR","SOM","ETH","DJI","BFA","MLI","UGA","MOZ","MWI","ZWE","NCL","CUB","DOM","NIC","GMB","CAF","BEN","GNB","MMR","RWA","BOL","COG","LAO","PRY","PAK","NPL","AFG","TCD","SDN","NER","ZAF","GAB","GHA","CIV","CMR","LBR","SLE","KNA","KIR","WSM","FJI","KHM","LKA","KEN","MDG","SYR","TGO","SEN","ARG","URY","GUY","SUR","GIN","JAM","AUS","FLK","SHN","GRL","SPM","ISL","SYC","MUS","BLZ","GTM","ECU","CRI","HND","SLV","HTI","BGD","BDI","YEM","NZL","EST","LVA","LTU","BIH","ALB","LBN","MKD","ZMB","GRC","UZB","TKM","IND","PNG","EGY","PRK","VNM","THA","IDN","PHL","PAN","BHS","MDA","BGR","UKR","HRV","SVN","AUT","FIN","SWE","TTO","VEN","OMN","GEO","ARM","CHL","PER","MNG","IRN","TUR","PRT","ESP","ITA","DNK","NLD","CYP","KGZ","TJK","BHR","MRT","BMU","BRB","IRL","CHE","MAC","HKG","CHN","CZE","POL","HUN","SVK","CAN","KAZ","RUS","BRA","USA","ISR","BLR","BEL","DEU","JPN","TWN","KOR","GIB","MLT","NOR","DZA","QAT","GNQ","NGA","MYS","SGP","GBR","MEX","COL","AZE","IRQ","AGO","LBY","SAU","KWT","ARE","TZA"]
TSP_R2PO = ["1213","1212","2771","2872","1221","2450","0012","0011","9410","2119","2876","0741","2114","2112","2654","0752","2922",
"0576","2685","6592","6593","6545","2640","8462","8451","8464","6121","2713","4233","6597","6612","4234","2472","4314",
"2631","2634","2223","0343","0350","0360","0342","4243","7933","2879","0711","2225","2235","6115","2117","2116","2683",
"2687","2785","2927","6116","0470","0615","3415","0582","1110","2924","0575","2120","2238","0542","6673","2651","8465",
"6574","8472","2633","2632","0574","6539","6724","3221","3231","2742","2614","6712","8424","1211","5241","2860","2690",
"0421","6349","2873","0573","0585","5513","2923","6129","2481","2877","6341","0721","0723","2479","0722","2320","0577",
"2221","0111","2222","0742","4232","0813","0459","2875","6576","6871","2890","2231","6812","2871","6821","2712","2517",
"0115","2686","0482","6512","2652","0371","4111","0814","2874","6841","3232","6842","0812","0460","6521","6513","6584",
"2814","6330","2440","4235","5622","5222","2667","2783","2734","6594","5981","5322","6516","8471","6978","6571","8484",
"8423","8441","0612","8463","8459","8443","8442","2232","2815","2816","6716","5225","6713","3413","5621","7911","5249",
"2512","2682","0112","0230","0224","2471","0118","0141","2911","2483","0548","2460","3224","0451","6343","2224","2732",
"4236","7915","6725","6123","6852","0541","0583","2919","0372","0344","0341","6546","7731","1123","0564","0411","6671",
"2655","2659","7521","8811","4241","4249","9710","5541","6651","1124","0616","4113","2681","5323","5623","0544","5629",
"0571","2731","8435","8434","8439","8452","8928","2733","7852","8461","6259","8482","0751","6575","6831","0586","0579",
"6611","6861","2613","8429","8422","8421","8431","8433","8432","0422","4312","2926","0545","7931","6515","6518","6899",
"0440","0412","6114","0116","0240","0223","2482","6342","3223","7914","2111","2820","2882","8851","8989","8981","5413",
"8973","4245","2929","8481","6534","6589","6522","0483","8991","0484","6932","5114","5224","4244","4242","4313","7761",
"5921","7213","1222","6891","1122","1121","6731","6732","7912","0430","2226","0452","6412","7251","7259","6418","6415",
"2518","7621","6665","2666","3345","7224","6544","7612","5823","6635","7711","6517","6581","6519","6582","0914","0488",
"5911","7723","9510","7163","5913","7922","2239","3414","5233","6811","5145","5514","5156","0015","5922","2519","6411",
"4239","7928","2234","2784","2741","3330","5121","0589","8483","0565","2672","6596","6595","8731","7842","7611","8121",
"0251","6935","5722","6413","6353","6351","6931","2789","6924","7169","7643","7188","8997","8742","6551","7764","8812",
"7599","8720","1223","2782","2786","6672","0572","5232","6417","6424","6951","6618","0481","0620","5543","6421","8931",
"7921","3354","7938","7932","3352","8960","5833","334","5157","5146","6872","8941","8822","5839","8741","6822","3351",
"6999","5239","7923","0129","0114","8946","7161","0013","5169","0121","0113","5415","8996","5416","5147","7268","6359",
"0142","6744","5989","7436","2516","2881","6623","6760","3353","6419","6591","7919","7148","3222","5835","0712","8122",
"8932","6863","6851","6644","6549","6613","7131","2711","6352","7518","7132","8732","7783","8951","6631","6344","6416",
"2925","0980","0149","5842","6254","6210","5912","5231","5123","6781","5221","0811","4311","7112","6511","8510","8211",
"8219","8212","8922","5335","6130","7219","9610","6832","6514","6783","6583","6543","6118","6542","7248","6535","7753",
"7751","6652","7752","8921","6423","6422","8972","8852","6674","6552","6553","8998","8939","6770","6749","6532","6560",
"5821","5542","0546","0561","6624","6973","6633","0252","8993","2511","0014","7272","2772","5122","6122","2332","7119",
"7111","7283","7782","7868","6251","5822","0913","5851","7187","7841","5838","6745","6746","6727","6747","6252","6794",
"5852","6646","7523","7754","7421","7245","5836","7768","8983","7722","6993","8842","6538","7271","5411","5417","7267",
"6954","7368","6572","7741","5419","5837","5334","6912","6911","6921","7822","7631","7861","7372","7432","7757","7758",
"6112","8947","0619","0819","0730","6733","7913","7449","7281","7371","7362","7361","7367","6643","5843","8821","8813",
"7269","8959","2671","7411","7422","7211","6428","6647","6577","5721","7233","8999","5530","7223","7628","7622","6281",
"6253","7416","7264","7924","7929","5841","7511","8748","5154","5982","5829","5138","2665","6645","6658","6998","7162",
"6632","5849","7133","8710","5112","5832","5831","5161","7641","7832","7821","8830","7525","7591","7763","7712","6649",
"7853","7243","7512","7528","6648","8743","6994","7721","7246","7244","7252","7369","7129","7732","7849","6289","6664",
"6579","7414","6793","6997","6991","5139","6354","6974","8310","7263","5332","8744","7435","7412","7452","7413","5825",
"6531","6536","8982","7638","7415","7851","5311","5312","5414","8974","7144","6642","8924","7788","7649","8841","5826",
"7648","6940","7451","7441","7234","7493","7239","5331","5824","7212","6996","7831","7810","6282","6960","6880","6975",
"8942","7642","7784","7434","7429","7439","7373","7284","7499","6573","5834","5914","5162","7742","7138","7149","7762",
"6641","5827","2331","5111","5113","5137","5163","6541","6785","7247","7781","6639","6637","8745","7423","7442","6782",
"7491","8935","8994","8933","6666","7522","5723","8124","6992","7126","6638","5148","7431","7139","8749","7492","7428",
"5983","5223","8952","5155","6953","0611",]

#-BEA_TSP-RCA-1000-#
# Method: BEA_TSP  method="repetitive_nn"
# Data Used: RCA Data (None < 1) Only top 10 data from each country-#
# Iterations: 100,000

TSP_R3CO = ["MAC","PAK","NPL","AFG","TCD","SDN","SOM","JOR","TGO","SYR","MDG","KEN","RWA","MMR","PRY","ETH","DJI","BFA","MLI","UGA","TZA","MWI","ZWE","NCL","CUB","DOM",
"NIC","GMB","CAF","BEN","GNB","MDA","SLE","KNA","KIR","WSM","PHL","LKA","BDI","YEM","NZL","EST","LVA","GUY","SUR","GIN","JAM","AUS","NER","ZAF","GAB","GHA",
"CIV","CMR","LAO","PNG","LBR","COG","BOL","PER","ISL","SPM","FLK","SHN","GRL","SYC","MUS","FJI","KHM","HTI","BGD","SLV","BLZ","GTM","ECU","CRI","CYP","HND",
"PAN","BHS","GIB","BMU","MRT","MNG","IRN","SEN","ARG","BRA","MOZ","TJK","UZB","TKM","NOR","CHL","ZMB","MKD","GRC","TUN","MAR","EGY","LBN","ALB","BIH","HRV",
"SVN","BGR","KGZ","PRK","VNM","THA","IDN","MYS","AZE","UKR","SVK","HUN","GEO","ARM","BRB","GBR","KAZ","RUS","SWE","FIN","DNK","HKG","CHE","IRL","LTU","CAN",
"BLR","ISR","DEU","FRA","ESP","PRT","TUR","BEL","URY","GNQ","QAT","TTO","VEN","OMN","BHR","ARE","KWT","SAU","IRQ","AGO","LBY","DZA","NGA","IND","COL","NLD",
"MEX","JPN","MLT","TWN","SGP","KOR","CHN","USA","ITA","AUT","CZE","POL"]
TSP_R3PO = ["0252","2926","2927","2654","0752","6592","2922","0576","2685","2683","2687","2785","0741","2114","2112","2876","2879","7933","0342","0360",
"2681","0112","2682","2117","9410","2119","0012","0011","2450","1221","0611","2771","1212","1213","1211","5241","2860","2690","0421","6349",
"2873","0573","0615","3415","8464","6121","2713","4233","6597","6612","4234","2472","4314","2631","2634","2223","0343","0350","0341","6546",
"2633","2632","6115","2225","2235","0542","6673","2923","2651","8465","2659","2655","7521","2872","6716","5225","6713","3413","5621","7911",
"0577","2221","0711","2238","0470","2116","6116","4312","4242","2231","6812","2871","0814","4111","0371","2652","0612","8441","2640","6545",
"6593","6594","2924","0575","2712","6821","6899","0440","2222","0111","2686","0115","0482","6512","4113","8459","2479","0721","0723","0722",
"6341","2877","2481","2483","2911","0582","2460","1110","0548","1123","7731","0564","0585","5513","8462","8451","8423","6519","8439","8442",
"2232","6521","6513","6584","2814","6330","2440","6576","2875","6871","2874","6841","3232","6842","0812","0742","4232","0459","0813","0616",
"1124","6891","1122","1222","0451","3224","2512","0230","0224","2471","0118","0141","7768","5157","5146","6872","8941","8822","4249","9710",
"5629","5222","5622","6852","6123","2742","3221","3231","2614","6712","8424","8422","8463","6574","8472","6951","6424","6344","2671","6760",
"7914","6129","6635","7711","9510","7723","0914","0488","5911","5323","5623","0544","0571","0541","2890","3414","2239","7223","7932","2320",
"2815","2816","0129","0114","2111","3223","4244","4313","8482","6259","8443","8928","2733","7852","8452","8433","8434","8435","8431","7263",
"4239","0452","6412","7251","2120","0013","5169","0121","0113","7161","5415","5514","5156","0015","8996","8851","2882","8989","8421","8981",
"5413","5147","7268","5416","7271","5411","6666","2613","8429","2782","6932","2741","2234","2784","0460","3351","9610","5249","6351","6931",
"2789","0372","0344","2919","6130","0565","8483","4235","2783","2667","5322","5981","6516","6672","7163","5913","7922","5239","7267","6544",
"3345","2666","2518","7621","6665","6575","7612","2226","2519","6411","7928","7915","4236","2224","2732","0574","6539","5841","7144","8960",
"2734","5833","3352","7938","7628","7761","6724","5921","0422","4245","2929","5122","7752","7611","7842","6747","8121","6252","7491","8731",
"8951","6631","6863","6518","6515","2731","6595","8461","7224","2517","0586","7753","7751","6960","8993","6359","6978","334","6551","7764",
"0579","8812","7599","0583","5823","3330","5121","5114","5112","8710","5138","7119","7247","6553","6811","0116","8998","6671","6549","6644",
"5541","7412","7131","5982","5154","7643","7169","7188","8997","8742","6924","5155","7853","7722","8983","6649","7648","6543","6118","0483",
"6542","7248","8842","6114","6535","7421","5852","6646","5722","7523","7754","3353","6725","7912","6419","6624","0572","8946","5831","5832",
"5161","5111","5113","6353","6511","0751","8510","6589","4241","8720","5543","7435","7264","6880","6352","6642","1121","7924","5530","6975",
"8942","2672","7187","7841","2482","6343","6342","2820","0545","7931","7259","6418","6415","7757","7783","6744","5989","6591","8813","6643",
"5843","8821","7367","7133","7269","7361","8841","7921","7591","8811","7162","7272","7233","7234","3354","5417","7432","7525","6953","8932",
"2665","8931","8991","5835","3222","7923","6822","7832","8732","7518","7132","6648","8922","8921","6413","5851","5838","7821","8973","6935",
"6611","6861","5837","6583","8933","8994","7522","8999","5723","7511","5232","6417","5123","6781","6954","2881","6623","0546","1223","7913",
"2786","2516","7861","0589","6851","5221","5721","0811","2511","6831","0411","0481","6731","5145","6940","6581","7213","0251","0223","0712",
"5842","4243"]


#-BEA_TSP-RCA-1000-#
# Method: BEA_TSP  method="repetitive_nn"-#
# Data Used: Yu RCA Data (None < 0) with Only top 5 data from each country-#
# Iterations: 1000-#

TSP_R4CO = ["ZAF","CHE","HKG","ARE","SAU","RUS","NGA","IRQ","KWT","AGO","NOR","LBY","IRN","KAZ","QAT","DZA","VEN","COL","AZE","MEX","DEU","JPN","CAN","KOR","TWN","MYS",
"SGP","NLD","IND","BEL","GBR","IRL","USA","FRA","ITA","CHN","VNM","IDN","AUS","BRA","ARG","UKR","TUR","PER","CHL","ZMB","BGR","BLR","GRC","SWE","ESP","CZE",
"SVK","POL","HUN","AUT","ISR","DNK","SVN","JOR","MAR","PHL","CRI","MLT","LTU","BHR","FIN","PRT","BGD","KHM","LKA","KEN","ECU","OMN","GNQ","COG","GAB","YEM",
"EGY","TTO","EST","CYP","CIV","GHA","SDN","PNG","TZA","MLI","LBN","BOL","TKM","MMR","THA","PAK","TUN","HND","GTM","ETH","NIC","NZL","URY","PRY","LVA","BHS",
"PAN","HRV","MOZ","ISL","TJK","BIH","DOM","SLV","HTI","MUS","SYC","MRT","MNG","LAO","UZB","GEO","NCL","ZWE","GUY","KGZ","BFA","BEN","CMR","ALB","TCD","BLZ",
"CUB","JAM","GIN","SUR","TGO","UGA","RWA","SLE","LBR","PRK","GRL","FLK","MDG","SYR","GNB","MDA","ARM","MKD","MWI","NER","GIB","BRB","FJI","BDI","DJI","SOM",
"AFG","NPL","KNA","MAC","GMB","CAF","SHN","KIR","BMU","WSM","SPM","SEN"]

TSP_R4PO = ["2632","3414","3330","334","7764","7932","8710","7810","7849","7924","5417","9710","8851","5416","5156","5514","8996","8973","2929","2631",
"6513","6584","0111","2815","3222","3413","4242","2320","7525","7522","7643","7649","7641","8510","7599","8942","8459","8310","1121","5530",
"7929","2222","7149","5989","8720","7821","7611","7731","7523","8211","7832","8813","2926","5112","7938","0721","0723","0577","5831","5121",
"5832","5161","2927","5111","0545","0579","6821","2871","2517","0611","2816","0114","0813","0440","0711","8439","8462","8423","8451","8441",
"8429","0360","0573","7763","8983","7768","7722","5833","6940","0741","6259","5623","6841","0412","2873","2681","0342","0341","6831","6725",
"4236","7915","6732","6595","0371","7415","7638","7284","7721","7269","7234","7788","8841","6251","7132","1110","6749","6672","6812","6716",
"2879","7436","6744","5621","5225","6713","5823","8219","7452","7139","7493","9410","2225","0223","0224","0112","0230","2471","0240","0113",
"7161","2120","6811","2882","7144","1124","8960","5147","8472","8463","8928","8452","8464","0011","2634","5629","2713","5222","0544","5622",
"7162","6412","6415","6418","2482","6413","2226","4239","2872","3354","3223","6871","2875","2874","0814","0344","0575","5221","2472","0542",
"6673","5413","6822","6842","6991","6123","6612","6521","6522","6589","6899","1212","2771","2890","2224","2820","6353","1222","3232","5834",
"2731","0571","4235","0565","0589","5232","8931","6424","8822","5839","5146","5157","8435","1211","6783","6592","8939","1221","0585","0980",
"5169","7492","0914","0612","3352","0712","2440","6115","2922","0012","0548","1123","2876","0460","5241","6861","3221","6712","8431","8424",
"2782","6931","2331","2483","2877","6341","0722","4111","0350","0343","4243","7712","6746","4314","6546","5541","2112","2117","8811","2682",
"5323","0752","0574","2919","0372","2789","6130","2687","2683","2785","4232","6114","5913","6954","8742","6551","7757","7783","7421","2450",
"6116","0812","6342","2460","6343","2783","2734","7861","0421","2860","2690","7842","6252","7491","6727","6731","7621","6330","0149","0541",
"4234","2221","1213","7933","7852","5513","8741","7131","8941","8743","8921","6978","7272","7233","8433","5922","5113","2741","6953","2481",
"6992","6951","7416","0488","6993","7169","7758","7923","0615","0484","7224","3345","4249","4312","7711","2119","2116","7283","0015","8212",
"2924","8471","6514","6960","0411","8465","8482","7851","6998","7188","5542","3351","5145","6651","0422"]


#-Plots-#

DATASET_DIR = TARGET_DATASET_DIR['nberbaci96']
DATASET = 'D'
RESULTS_DIR = CHAPTER_RESULTS[2]
YEAR = 2012

print "[INFO] Computing Plots for the Year %s from NBER datasets %s ..." % (YEAR, DATASET)

data = pd.read_hdf(DATASET_DIR+"nberbaci-export-sitcr2l4-1962to2012-harmonised.h5", DATASET)
data = data.loc[data.year == YEAR]
data = data.rename(columns={'eiso3c':'country','sitc4':'productcode', 'value':'export'})
data = data.reset_index()
del data["index"]
del data["year"]
data = data.set_index(["country", "productcode"])

s = ProductLevelExportSystem()
s.from_df(data, country_classification="ISO3c", product_classification="SITCR2", compile_dtypes=["DataFrame"], year=YEAR)
s.rca_matrix(complete_data=True)
s.mcp_matrix()	

#-Sorted Matrix-#
for RN,RCO,RPO in [(1,TSP_R1CO,TSP_R1PO),(2,TSP_R2CO,TSP_R2PO),(3,TSP_R3CO,TSP_R3PO),(4,TSP_R4CO, TSP_R3PO)]:
	co = pd.Series(RCO).to_frame().reset_index()
	co = co.rename(columns={0:'iso3c','index':'order'})
	co = co.set_index('iso3c')['order']
	po = pd.Series(RPO).to_frame().reset_index()
	po = po.rename(columns={0:'sitc','index':'order'})
	po = po.set_index('sitc')['order']
	s.mcp_matrix()
	if RN == 4:
		s.mcp = s.sorted_matrix(s.mcp, row_sortby=co.copy(), column_sortby=po.copy(), row_ascending=False)
	else:
		s.mcp = s.sorted_matrix(s.mcp, row_sortby=co.copy(), column_sortby=po.copy())
	fig = s.plot_mcp(row_sortby_label="TSP Seriation", column_sortby_label="TSP Seriation")
	plt.tight_layout()
	plt.savefig(RESULTS_DIR + 'nberbaci96_mcp_seriation_R(%s)_yr%s_dataset(%s).png'%(RN,YEAR,DATASET), dpi=400, bbox_inches="tight")
	plt.clf()

	#-Scaled Plots-#
	#-Trade Shares-#
	cntryshare = s.country_shares()
	cntryshare = cntryshare.filter(co.index, axis=0)
	prodshare = s.product_shares()
	prodshare = prodshare.filter(po.index,axis=0)
	fig = s.plot_scaled_mcp_heatmap(s.mcp, cpdata_name="{0,1}", row_scaleby=cntryshare, column_scaleby=prodshare, row_label=('TSP Seriation','Export Value Share'), column_label=('TSP Seriation','Product Value Share'))
	plt.tight_layout()
	plt.savefig(RESULTS_DIR + 'nberbaci96_mcp_seriation(%s)_y(cntryshare)_x(prodshare)_yr%s_dataset(%s).png'%(RN,YEAR,DATASET), dpi=400, bbox_inches='tight')
	plt.clf()
	#-RCA Version-#
	s.rca = s.rca.filter(co.index,axis=0)
	s.rca = s.rca.filter(po.index,axis=1)
	if RN == 4:
		s.rca = s.sorted_matrix(s.rca, row_sortby=co.copy(), column_sortby=po.copy(), row_ascending=False)
	else:
		s.rca = s.sorted_matrix(s.rca, row_sortby=co.copy(), column_sortby=po.copy())
	fig = s.plot_scaled_mcp_heatmap(s.rca, cpdata_name="RCA", row_scaleby=cntryshare, column_scaleby=prodshare, low_value_cutoff=1, high_value_cutoff=4, row_label=('TSP Seriation','Export Value Share'), column_label=('TSP Seriation','Product Value Share'))
	plt.tight_layout()
	plt.savefig(RESULTS_DIR + 'nberbaci96_rca_seriation(%s)_y(cntryshare)_x(prodshare)_yr%s_dataset(%s).png'%(RN,YEAR,DATASET), dpi=400, bbox_inches='tight')
	plt.clf()