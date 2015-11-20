"""
Chapter 5: Product Fragmentation
================================

Compute Product Fragmentation Dataset with Final and Parts and Components Indicators

Dataset
-------
WITS (UN-COMTRADE) DATASET

Sata Files
----------
chapter_5_productfragmentation-wits-sitcr3.do

"""

from __future__ import division

import os
import gc
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt

from pyeconlab import DynamicProductLevelExportSystem
from dataset_info import CHAPTER_RESULTS
SOURCE_DIR = CHAPTER_RESULTS[5] 						#-Data is made using stata-#
RESULTS_DIR = CHAPTER_RESULTS[5]

#-Meta Data and Classifications-#
R3toR2 = pd.read_stata(SOURCE_DIR+"un-sitcr3-sitcr2.dta").set_index("sitcr3l5")
pc = pd.read_stata(SOURCE_DIR+"athukorala-pc-sitcr3l5.dta").set_index("sitcr3l5")
lall = pd.read_stata(SOURCE_DIR+"lall-techclassification.dta").set_index("sitcr2l3") 	#Interface <lall,sitc3,sitcdescription>. Note: This is SITC rev 2 but at the 3 digit level!
cntryreg = pd.read_stata(SOURCE_DIR+"un-countryinfo-regions-development.dta")

# ------------------ #
# ---- ANALYSIS ---- #
# ------------------ #

#-Load Data-#

#-WITS (UN COMTRADE) EXPORT DATA (EXPORTER REPORTS)-#
data = pd.read_stata(SOURCE_DIR+"wits-sitcr3-dataset/"+"wits-sitcr3-export-exporterreports-dataset.dta")
data = data.rename(columns={'eiso3c':'country'})
#-Yr2013 appears incomplete-#
data = data.loc[data.year <= 2012]

#-Locate Manufactured Trade-#
manuf = data.loc[data.sitccat == 'M'].reset_index(drop=True)

#-Analysis-#

#-Total P&C Exports in Total World Export-#
world = data.groupby(["year","pc"]).sum()["export"]
plotdata = world.unstack()
plotdata = plotdata.rename(columns={0:'Other Exports', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.plot()
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr3-total_pc_total_exports.png", dpi=400)
plt.clf()
#-Percent P&C-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Other Exports"])
fig = plotdata["%"].plot(ylim=0, title="Percent (P&C) in Total World Exports")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-percent_pc_total_exports.png", dpi=400)
plt.clf()

#-Total P&C Exports in Total Manufacturing Exports-#
plotdata = manuf.groupby(["year","pc"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products (Manufacturing)', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.plot()
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-total_pc_total_manuf_exports.png", dpi=400)
plt.clf()
#-Percent P&C-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Final Products (Manufacturing)"])
fig = plotdata["%"].plot(ylim=0, title="Percent (P&C) in Total Manufacturing Exports")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-percent_pc_total_manuf_exports.png", dpi=400)
plt.clf()

#-Area Analysis-#

#-Total P&C Exports in Manufacturing Exports (by Area)-#
plotdata = manuf.groupby(["areaname","year","pc"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.ix["Asia"].plot(title="Asia")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-total_pc_total_manuf_exports-Asia.png", dpi=400)
plt.clf()

#-Percent of P&C Exports in Manufacturing Export (by Area)-#
plotdata = manuf.groupby(["areaname","year","pc"]).sum()
plotdata["totx"] = plotdata.groupby(level=["areaname", "year"])["export"].transform(np.sum)
plotdata["%x"] = plotdata["export"] / plotdata["totx"]
plotdata = plotdata["%x"].unstack()
plotdata = plotdata.rename(columns={0:'Final Products', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.ix["Asia"].plot(title="Asia")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-percent_pc_in_manuf_exports-Asia.png", dpi=400)
plt.clf()
#-Area %share in Manufacturing Trade (P&C)-#
fig2 = plotdata["Parts and Components"].unstack(level="areaname").plot(title="Percent P&C in Total Manufacturing Trade (by Area)")
fig2.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig2.set_ylabel("Percent")
fig2.set_ylim(bottom=0)
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-percent_pc_in_manuf_exports-UNArea.png", dpi=400, bbox_inches="tight")
plt.clf()

#~~Region Analysis~~#

#-Total P&C Exports in Manufacturing Exports (by Region)-#
plotdata = manuf.groupby(["regionname","year","pc"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.ix["Eastern Asia"].plot(title="Total P&C and Manufacturing Export (Eastern Asia)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-total_pc_tota_manuf_exports-EAsia.png", dpi=400)
plt.clf()
fig = plotdata.ix["South-Eastern Asia"].plot(title="Total P&C and Manufacturing Export (South-Eastern Asia)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-total_pc_tota_manuf_exports-SEAsia.png", dpi=400)
plt.clf()

#-Region %share in Manufacturing Exports-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Final Products"])
fig = plotdata["%"].unstack(level="regionname")[["Eastern Asia", "South-Eastern Asia"]].plot(title="PC % share in Manufacturing Exports (by Region)", ylim=(0,0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-percent_pc_in_manuf_exports-EAsia+SEAsia.png", dpi=400)
plt.clf()
#-Table-#
table = plotdata["%"].unstack(level="regionname")
table.to_excel(RESULTS_DIR+"tables/"+"wits-sitcr2-percent_pc_in_manuf_exports-UNRegions.xlsx")

#~~Country Analysis~~#

#-Total Exports-#

#-Total P&C in Total Exports (By Country)-#
plotdata = data.groupby(["country","year","pc"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.ix["JPN"].plot(title="Total P&C and Total Export (Japan)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-total_pc_and_total_exports-JPN.png", dpi=400)
plt.clf()
fig = plotdata.ix["CHN"].plot(title="Total P&C and Total Export (China)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-total_pc_and_total_exports-CHN.png", dpi=400)
plt.clf()

#-Country %share in Total Exports (By Country)-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Final Products"])
fig = plotdata["%"].unstack(level="country")[["CHN", "JPN", "KOR"]].plot(title="Total P&C %share of Total Export (Countries)")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-percent_pc_in_total_exports-CHN+JPN+TWN+KOR.png", dpi=400, bbox_inches="tight")
plt.clf()
fig = plotdata["%"].unstack(level="country")[["IDN", "MYS", "PHL", "SGP", "THA", "VEN"]].plot(title="Total P&C %share of Total Export (Countries)")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-percent_pc_in_total_exports-IDN+MYS+PHL+SGP+THA+VEN.png", dpi=400, bbox_inches="tight")
plt.clf()
#-Table-#
table = plotdata["%"].unstack(level="country").T
table.to_excel(RESULTS_DIR+"tables/"+"wits-sitcr2-percent_pc_in_total_exports-Countries.xlsx")
SELECTION = ["AUS","USA","CHN","JPN","IDN","IND","THA","MYS","PHL","VEN"]
table.ix[SELECTION].to_excel(RESULTS_DIR+"tables/"+"wits-sitcr2-percent_pc_in_total_exports-SelectionCntry.xlsx")

#-Manufacturing Exports Only-#

#-Total P&C Exports in Manufacturing Exports (by Country)-#
plotdata = manuf.groupby(["country","year","pc"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.ix["JPN"].plot(title="Total P&C and Manufacturing Export (Japan)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-total_pc_and_manuf_exports-JPN.png", dpi=400)
plt.clf()
fig = plotdata.ix["CHN"].plot(title="Total P&C and Manufacturing Export (China)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-total_pc_and_manuf_exports-CHN.png", dpi=400)
plt.clf()

#-Country %share in Manufacturing Export-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Final Products"])
fig = plotdata["%"].unstack(level="country")[["CHN", "JPN", "KOR"]].plot(title="Total P&C %share of Manufacturing Export (Countries)")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-percent_pc_in_manuf_exports-CHN+JPN+KOR.png", dpi=400, bbox_inches="tight")
plt.clf()
fig = plotdata["%"].unstack(level="country")[["IDN", "MYS", "PHL", "SGP", "THA", "VEN"]].plot(title="Total P&C %share of Manufacturing Export (Countries)")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-percent_pc_in_manuf_exports-IDN+MYS+PHL+SGP+THA+VEN.png", dpi=400, bbox_inches="tight")
plt.clf()
#-Table-#
table = plotdata["%"].unstack(level="country").T
table.to_excel(RESULTS_DIR+"tables/"+"wits-sitcr2-percent_pc_in_manuf_exports-Countries.xlsx")
SELECTION = ["AUS","USA","CHN","JPN","IDN","IND","THA","MYS","PHL","VEN"]
table.ix[SELECTION].to_excel(RESULTS_DIR+"tables/"+"wits-sitcr2-percent_pc_in_manuf_exports-SelectionCntry.xlsx")


#------------#
#-TRADE DATA-#
#------------#

#------------------#
#-Intra-Asia Trade-#
#------------------#

store = pd.HDFStore(RESULTS_DIR+"wits-sitcr3-dataset/wits-sitcr3-trade-1995-2013-yearly.h5") 	#-Already has Regions Coded-#
for year in xrange(1995,2013+1,2):
	print "Processing year %s ..."%year
	df = store.get("Y%s"%year)
	if year == 1995:
		data = df
	else:
		data = data.append(df)
	del df
	gc.collect()		
data.reset_index(inplace=True)
del data["index"]
store.close()

# for year in xrange(1995,2013+1,1):
# 	print "Processing year %s ..."%year
# 	df = pd.read_stata(SOURCE_DIR+"baci-sitcr3-trade-%s.dta"%(year))
# 	df = df.rename(columns={'sitcr3' : 'productcode'})
# 	if year == 1995:
# 		data = df
# 	else:
# 		data = data.append(df)
# 	del df
# 	gc.collect()
# data.reset_index(inplace=True)
# del data["index"]
# #-Country Region Information-#
# data = data.merge(cntryreg[["iso3c","areaname"]], left_on=["eiso3c"], right_on=["iso3c"], how="inner") 	#-how="left"
# data.rename(columns={'areaname':'eareaname'})
# del data["iso3c"]
# data = data.merge(cntryreg[["iso3c","areaname"]], left_on=["iiso3c"], right_on=["iso3c"], how="inner") 	#-how="left"
# data.rename(columns={'areaname':'iareaname'})
# del data["iso3c"]

#-Intra-East Asian Trade-#
easia = data.loc[(data.eregionname == "Eastern Asia")&(data.iregionname == "Eastern Asia")].reset_index()
seasia = data.loc[(data.eregionname == "South-Eastern Asia")&(data.iregionname == "South-Eastern Asia")].reset_index() 
asia = easia.append(seasia).reset_index()
del asia["index"]
asia_exp = asia.groupby(["year", "productcode", "eiso3c"], as_index=False).sum()
asia_imp = asia.groupby(["year", "productcode", "iiso3c"], as_index=False).sum()
#-Merge in P&C-#
#-Add Parts and Components (SITCR3)-#
pccodes = set(pc.index)
asia_exp["pc"] = asia_exp["productcode"].apply(lambda x: 1 if x in pccodes else 0)
asia_imp["pc"] = asia_imp["productcode"].apply(lambda x: 1 if x in pccodes else 0)
#-Add ProductCode Levels-#
for level in [4,3,2,1]:
	asia_exp["sitcr3l%s"%level] = asia_exp["productcode"].apply(lambda x: x[0:level])
	asia_imp["sitcr3l%s"%level] = asia_imp["productcode"].apply(lambda x: x[0:level])
#-Add SITC Basic Categories-#
sitc_groups = { 		#-'P' = Primary, 'O' = Oil, 'M' = Manufacturing, 'S' = Special
	'0' : 'P',
	'1' : 'P',
	'2' : 'P',
	'3' : 'O',
	'4' : 'P',
	'5' : 'M',
	'6' : 'M',
	'68': 'P', 			#Manually Code
	'7' : 'M',
	'8' : 'M',
	'9' : 'S',
}
asia_exp["sitccat"] = asia_exp["sitcr3l1"].apply(lambda x: sitc_groups[x])
asia_exp.loc[asia_exp.sitcr3l2 == "68", "sitccat"] = "P"
asia_imp["sitccat"] = asia_imp["sitcr3l1"].apply(lambda x: sitc_groups[x])
asia_imp.loc[asia_imp.sitcr3l2 == "68", "sitccat"] = "P"

manuf = asia_exp.loc[asia_exp.sitccat == 'M'].reset_index()
del manuf["index"]

#-Intra-Regional Regional Analysis-#

#-Total P&C Exports in Manufacturing Exports-#
plotdata = manuf.groupby(["year","pc"]).sum()["value"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.plot(title="Intra East Asia + South-East Asia)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-total_pc_in_manuf_exports-Intra-SAsia+SEAsia.png", dpi=400)
plt.cla()
#-Total P&C Percent of Total Manufacturing Exports-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Final Products"])
fig = plotdata["%"].plot(title="PC % share in Manufacturing Exports (Intra East and South-East Asia)", ylim=(0,0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-percent_pc_in_manuf_exports-Intra-SAsia+SEAsia.png", dpi=400)

#~Intra-Regional Country Analysis-#

#-Total P&C Export in Manufacturing Exports-#
manuf = manuf.rename(columns={"eiso3c":"country"})
plotdata = manuf.groupby(["country","year","pc"]).sum()["value"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products', 1:'Parts and Components'})
plotdata.columns.name = ""
#-Total P&C Percent of Total Manufacturing Exports-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Final Products"])
fig = plotdata["%"].unstack(level="country")[["CHN", "JPN", "TWN", "KOR"]].plot(title="PC % share in Manufacturing Exports (Intra East and South-East Asia)", ylim=(0,0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-percent_pc_in_manuf_exports-Intra-SAsia+SEAsia-CHN+JPN+TWN+KOR.png", dpi=400)
#-Table-#
table = plotdata["%"].unstack(level="country").T
SELECTION = ["CHN","JPN","IDN","IND","THA","MYS","PHL","VEN"]
table.ix[SELECTION].to_excel(RESULTS_DIR+"tables/wits-sitcr2-percent_pc_in_manuf_exports-Intra-SAsia+SEAsia-SelectionCntry.xlsx")




#----------------#
#-Setup a System-#
#----------------#

#-Export Data-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci-sitcr3-export-%s.dta"%(year))
	df = df.rename(columns={'eiso3c' : 'country', 'sitcr3' : 'productcode', 'value':'export'})
	df = df.set_index(["year"])
	if year == 1995:
		data = df
	else:
		data = data.append(df)
s = DynamicProductLevelExportSystem()
s.from_df(data)

#-2013 Cross Section-#
xs = s[2000]
xs.rca_matrix(complete_data=True)
xs.mcp_matrix()
xs.proximity_matrix()
xs.compute_pci()
xs.auto_adjust_pci_sign(product_datum=('33400','-ve')) 	#-Oil is negative, un-complex-#

prox = xs.proximity
pci = xs.pci.to_dict()

#-Parts and Components Product Code Sets-#
allcodes = set(prox.index)
pccodes = set(pc.index)
othercodes = allcodes.difference(pccodes)

#-Tables of Parts and Components Relatedness (Proximity)-#
print prox.unstack().describe()
pc_prox = prox.filter(items=pccodes, axis=0).filter(items=pccodes, axis=1)
print pc_prox.unstack().describe()
other_prox = prox.filter(items=othercodes, axis=0).filter(items=othercodes, axis=1)
print other_prox.unstack().describe()

#-Plot Proximity Matrix Sorted by P&C and Final Goods-#
prox = prox.stack().reset_index()
prox = prox.rename(columns={0:'prox'})
#-P&C-#
prox["pc1"] = prox["productcode1"].apply(lambda x: 1 if x in pccodes else 0)
prox["pc2"] = prox["productcode2"].apply(lambda x: 1 if x in pccodes else 0)
#-PCI-#
prox["pci1"] = prox["productcode1"].apply(lambda x: pci[x])
prox["pci2"] = prox["productcode2"].apply(lambda x: pci[x])
#-Organise Data-#
prox = prox.set_index(["pc1","pci1","productcode1","pc2","pci2","productcode2"])
prox = prox.unstack(level=["pc2","pci2","productcode2"])
prox.columns = prox.columns.droplevel()
prox = prox.sort(axis=1)

#-Proximity Plots-#
from reference.ProductSpace import plot_proximity_simple
prox = prox.applymap(lambda x: 0.6 if x >= 0.6 else x)
fig = plot_proximity_simple(prox)
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-proximity-sortby(pc-pci)-yr2000.png", dpi=400)
plt.clf()
#-Plot of Parts and Components-#
fig = plot_proximity_simple(prox.ix[1][1])
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-proximity(pc)-sortby(pci)-yr2000.png", dpi=400)
plt.clf()
#-Plot All Other Products-#
fig = plot_proximity_simple(prox.ix[0][0])
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-proximity(not-pc)-sortby(pci)-yr2000.png", dpi=400)
plt.clf()
#-Plot P&C Versus Other Products-#
fig = plot_proximity_simple(prox.ix[1][0])
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"wits-sitcr2-proximity(pc-vs-not-pc)-sortby(pci)-yr2000.png", dpi=400)
plt.clf()





#--------------#
#--OTHER WORK--#
#--------------#

#-Construct BACI SITCR2 Conversion Using RAW SITCR2 Data as a check against Stata-#

#->2.USE BACI SITCR2 CONVERSION FROM STATA<-#
#-BACI SITCR2-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"sitcr3/"+"baci-sitcr3-export-%s.dta"%(year))
	df = df.rename(columns={'eiso3c' : 'country', 'sitcr3' : 'productcode', 'value':'export'})
	if year == 1995:
		data = df
	else:
		data = data.append(df)
data.reset_index(inplace=True)
del data["index"]

#-Add ProductCode Levels-#
for level in [4,3,2,1]:
	data["sitcr3l%s"%level] = data["productcode"].apply(lambda x: x[0:level])
#-Add SITC Basic Categories-#
sitc_groups = { 		#-'P' = Primary, 'O' = Oil, 'M' = Manufacturing, 'S' = Special
	'0' : 'P',
	'1' : 'P',
	'2' : 'P',
	'3' : 'O',
	'4' : 'P',
	'5' : 'M',
	'6' : 'M',
	'68': 'P', 			#Manually Code
	'7' : 'M',
	'8' : 'M',
	'9' : 'S',
}
data["sitccat"] = data["sitcr3l1"].apply(lambda x: sitc_groups[x])
data.loc[data.sitcr3l2 == "68", "sitccat"] = "P"

#-Add Parts and Components (SITCR3)-#
pccodes = set(pc.index)
data["pc"] = data["productcode"].apply(lambda x: 1 if x in pccodes else 0)

#-SITCR2-#
sitcr3tositcr2 = R3toR2["sitcr2l5"].to_dict()
def concord_sitcr2(x):
	try:
		return sitcr3tositcr2[x]
	except:
		if x == "33400": 			#-Special Case if SITC rev 3 data is concorded from HS-#
			return x
		else:
			return "CHECK"
data["sitcr2l5"] = data["productcode"].apply(lambda x: concord_sitcr2(x))
if len(data[data.sitcr2l5 == "CHECK"]) > 0:
	raise ValueError("Issue with Concordance --- need to check!")
data["sitcr2l3"] = data["sitcr2l5"].apply(lambda x: x[0:3])

#-Lall Technology Classification-#
sitcr2l3tolall = lall["lall"].to_dict()
def concord_lall(x):
	try:
		return sitcr2l3tolall[x]
	except:
		return "D" #DROP
data["Lall"] = data["sitcr2l3"].apply(lambda x: concord_lall(x))
print data[data.Lall == "D"].sitcr2l3.unique()
# Result: ['351', '883', '892', '896', '941', '961', '971']
# 351 = Electric Current
# 883 = Cinema and Films
# 892 = Printed Matter
# 896 = Works of Art, Collectors and Antiques
# 941 = Zoo Animals
# 961 = Coins
# 971 = Gold
data = data.loc[data.Lall != "D"].reset_index() 				#Drop Codes Note Captured by Lall Technology Classification
del data["index"]

#-Country Information-#
data = data.merge(cntryreg[["iso3c","regionname", "areaname","moredev","lessdev","leastdev"]], left_on=["country"], right_on=["iso3c"], how="inner") 	#-how="left"
#Dropped Items: array(['ANT', 'ATF', 'CCK', 'CXR', 'IOT', 'NFK', 'NTZ', 'PCN', 'ROM','TMP', 'UMI', 'YUG', 'ZAR'], dtype=object)

#-Once you drop Lall == "SP" from this data it is the same as the STATA DATASET-#



#-Convert WITS CSV Files to STATA DTA Files-#
import pandas as pd
import glob
SOURCE_DIR = os.path.expanduser("~/work-data/datasets/f46c46a0f79ab9d11ec6c27a27622c10/")
fls = glob.glob(SOURCE_DIR+"export/"+'*_Export.csv')
for fl in fls:
	data = pd.read_csv(fl, dtype={'ProductCode':str})
	fln = fl.split('.')[0]+'.dta'
	data.to_stata(fln)


#- STATA IS MUCH FASTER AT THIS TYPE OF WORK EXCEPT STRINGS -#

##------------------------------------------##
##--Construct WITS Compilation of Datasets--##
##------------------------------------------##

import pandas as pd
import glob
SOURCE_DIR = "/home/matthewmckay/work-data/datasets/f46c46a0f79ab9d11ec6c27a27622c10/"

#-Exporter Reports-#
fls = glob.glob(SOURCE_DIR+"export/"+'*_Export.csv')   	#Move to exports/*.csv?
print len(fls)
exdata= pd.read_csv(fls[0])
for fl in fls[1:]:
    exdata = exdata.append(pd.read_csv(fl)) 
#-Save to HDF, Stata-# (CSV?)
exdata.to_hdf(SOURCE_DIR+"cache/"+"wits-sitcr3-export.h5", "export")
exdata.to_stata(SOURCE_DIR+"cache/"+"wits-sitcr3-export.dta")

#-ReExport Reports-#
fls = glob.glob(SOURCE_DIR+"reexport/"+"*_ReExport.csv")   	#Move to exports/*.csv?
print len(fls)
exdata= pd.read_csv(fls[0])
for fl in fls[1:]:
    exdata = exdata.append(pd.read_csv(fl)) 
#-Save to HDF, Stata-# (CSV?)
exdata.to_hdf(SOURCE_DIR+"cache/"+"wits-sitsr3-reexports.h5", "export")
exdata.to_stata(SOURCE_DIR+"cache/"+"wits-sitcr3-reexports.dta")

#---------------------------#
#-WITS UN COMTRADE ANALYSIS-#
#---------------------------#

SOURCE_WITS = "/home/matthewmckay/work-temp-local/"
trade = pd.read_stata(SOURCE_WITS+"wits-sitcr3-export.dta")
trade = pd.read_csv(SOURCE_WITS+"wits-sitcr3-export.csv")
trade = trade.rename(columns={'reporteriso3':'eiso3c', 'partneriso3':'iiso3c', 'tradevalue':'value'})
exp = trade.groupby(["eiso3c", "year", "productcode"]).sum()["value"].reset_index()
exp = exp.rename(columns={'eiso3c':'country'})
exp.name = "export"
#imp = trade.groupby(["iiso3c", "year", "productcode"]).sum()["value"]
#imp = imp.rename(columns={'iiso3c':'country'})
#del trade
#gc.collect()