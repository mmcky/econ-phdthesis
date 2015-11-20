"""
Chapter 5: Product Fragmentation
================================

Compute Product Fragmentation Dataset with Final and Parts and Components Indicators

Dataset
-------
BACI HS1992 Data

STATA Files
-----------
chapter_5_productfragmentation-baci92.do [Builds Dataset]

"""

from __future__ import division

import os
import gc
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt

from pyeconlab import WDI
from dataset_info import SOURCE_DIR
WDI_DIR = SOURCE_DIR["wdi"]
wdi = WDI(source_dir=WDI_DIR)

from pyeconlab import DynamicProductLevelExportSystem
from dataset_info import CHAPTER_RESULTS
SOURCE_DIR = CHAPTER_RESULTS[5] 						#-Data is made using stata-#
RESULTS_DIR = CHAPTER_RESULTS[5]

#-Classifications-#
R3toR2 = pd.read_stata(SOURCE_DIR+"un-sitcr3-sitcr2.dta").set_index("sitcr3l5")
pc = pd.read_stata(SOURCE_DIR+"athukorala-pc-sitcr3l5.dta").set_index("sitcr3l5")
lall = pd.read_stata(SOURCE_DIR+"lall-techclassification.dta").set_index("sitcr2l3") 	#Interface <lall,sitc3,sitcdescription>. Note: This is SITC rev 2 but at the 3 digit level!
cntryreg = pd.read_stata(SOURCE_DIR+"un-countryinfo-regions-development.dta")
pscomm = pd.read_stata(SOURCE_DIR+"productspace-communityname-concord.dta").set_index("pscommunity")
psnodes = pd.read_stata(RESULTS_DIR+"productspace-sitcr2-node-data.dta")

#-----------------------------------------------#
#-Construct H5 Files of SITC R3 BACI TRADE DATA-#
#-----------------------------------------------#

#-Yearly Raw SITCR2 Conversion Files for merging within Python-#
store = pd.HDFStore(RESULTS_DIR+"baci92-sitcr3/"+"baci92-sitcr3-trade-1995-2013-yearly.h5", complevel=9, complib="zlib")
for year in xrange(1995,2013+1,1):
	print "Processing year %s ..."%year
	data = pd.read_stata(SOURCE_DIR+"baci92-sitcr3/"+"baci92-sitcr3-trade-%s.dta"%(year))
	data = data.rename(columns={'sitcr3' : 'productcode'})
	#-Country Region Information-#
	data = data.merge(cntryreg[["iso3c","regionname","areaname"]], left_on=["eiso3c"], right_on=["iso3c"], how="inner") 	#-how="left"
	data = data.rename(columns={'regionname':'eregionname', 'areaname':'eareaname'})
	del data["iso3c"]
	data = data.merge(cntryreg[["iso3c","regionname","areaname"]], left_on=["iiso3c"], right_on=["iso3c"], how="inner") 	#-how="left"
	data = data.rename(columns={'regionname':'iregionname', 'areaname':'iareaname'})
	del data["iso3c"]
	store.put("Y%s"%year, data, format="table")
	del data
	gc.collect()
store.close()

#-H5 Dataset of STATA Compiled Trade DATASET-#
#-Includes: Parts and Components, Manufactured Goods, Lall and Leamer Product Classifications and ProductSpace Communities-#
store = pd.HDFStore(RESULTS_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-trade-1995-2013-yearly.h5", complevel=9, complib="zlib")
for year in xrange(1995,2013+1,1):
	print "Processing year %s ..."%year
	data = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-trade-%s-dataset.dta"%(year))
	store.put("Y%s"%year, data, format="table")
	del data
	gc.collect()
store.close() 

#-------------------------------------------------------------------------------------------------------------#

# ---- ANALYSIS ---- #

#-Load Data-#

#-Work with BACI EXPORT Data-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-export-%s-dataset.dta"%(year))
	if year == 1995:
		data = df
	else:
		data = data.append(df)
data.reset_index(inplace=True, drop=True)
data["export"] = data["export"]*1000 			#Scale Exports by 1000

#-Locate Manufactured Trade-#
manuf = data.loc[data.sitccat == 'M'].reset_index(drop=True)

#----------#
#-Analysis-#
#----------#

#-----------------#
#-Within Analysis-#
#-----------------#

#-Total P&C Exports in Total World Export-#
world = data.groupby(["year","pc"]).sum()["export"]
plotdata = world.unstack()
plotdata = plotdata.rename(columns={0:'Other Exports', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.plot()
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-total_pc_total_exports.png", dpi=400)
plt.clf()
#-Percent P&C-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Other Exports"])
fig = plotdata["%"].plot(ylim=0, title="Percent (P&C) in Total World Exports")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_total_exports.png", dpi=400)
plt.clf()

#-Total P&C Exports in Total Manufacturing Exports-#
plotdata = manuf.groupby(["year","pc"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products (Manufacturing)', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.plot()
fig.set_ylabel("US$ Trillion")
fig.set_xlabel("Year")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-total_pc_total_manuf_exports.png", dpi=400)
plt.clf()
#-Percent P&C-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Final Products (Manufacturing)"])
fig2 = plotdata["%"].plot(ylim=0, title="Percent (P&C) in Total Manufacturing Exports")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_total_manuf_exports.png", dpi=400)
plt.clf()
#-Combined Plot-#
plotdata = manuf.groupby(["year","pc"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'FM', 1:'PC'})
plotdata.columns.name = ""
plotdata["%PC"] = (plotdata["PC"] / (plotdata["PC"] + plotdata["FM"]))*100
fig = plotdata.plot(secondary_y=["%PC"], xticks=xrange(1995,2013+1,2))
fig.set_ylabel("US$ Trillion")
fig.set_xlabel("Year")
fig.right_ax.set_ylabel("Percent PC (of Manufactures)")
fig.right_ax.set_ylim((0,100))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-pc_total_manuf_exports_withpercent-thesis.png", dpi=400)

#-Parts and Components in Developing Country Share-#
devmanuf = manuf.loc[manuf.lessdev == 1]
#-Combined Plot-#
plotdata = devmanuf.groupby(["year","np"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'OM', 1:'NP'})
plotdata.columns.name = ""
plotdata["%NP"] = (plotdata["NP"] / (plotdata["NP"] + plotdata["OM"]))*100
fig = plotdata.plot(secondary_y=["%NP"], xticks=xrange(1995,2013+1,2))
fig.set_ylabel("US$ Trillion")
fig.set_xlabel("Year")
fig.right_ax.set_ylabel("Percent NP (of Manufactures)")
fig.right_ax.set_ylim((0,100))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-pc_total_manuf_exports_withpercent-thesis.png", dpi=400)


#-Area Analysis-#

#-Total P&C Exports in Manufacturing Exports (by Area)-#
plotdata = manuf.groupby(["areaname","year","pc"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.ix["Asia"].plot(title="Asia")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-total_pc_total_manuf_exports-Asia.png", dpi=400)
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
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_in_manuf_exports-Asia.png", dpi=400)
plt.clf()
#-Area %share in Manufacturing Trade (P&C)-#
fig2 = plotdata["Parts and Components"].unstack(level="areaname").plot(title="Percent P&C in Total Manufacturing Trade (by Area)")
fig2.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig2.set_ylabel("Percent")
fig2.set_ylim(bottom=0)
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_in_manuf_exports-UNArea.png", dpi=400, bbox_inches="tight")
plt.clf()
#-Plot For Thesis-#
plotdata = plotdata["Parts and Components"].apply(lambda x: x*100)
plotdata = plotdata.unstack(level="areaname")
plotdata = plotdata.rename(columns={"Latin America and the Caribbean":"South America","Northern America":"North America"})
fig = plotdata["Africa"].plot(style='c--')
fig = plotdata["Asia"].plot(style='g^-')
fig = plotdata["Europe"].plot(style='r-.')
fig = plotdata["South America"].plot(style='b*-')
fig = plotdata["North America"].plot(style='kx-')
fig = plotdata["Oceania"].plot(style='m-')
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent PC (in Manufactures)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_in_manuf_exports-UNArea-thesis.png", dpi=400, bbox_inches="tight")

#~~Region Analysis~~#

#-Total P&C Exports in Manufacturing Exports (by Region)-#
plotdata = manuf.groupby(["regionname","year","pc"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.ix["Eastern Asia"].plot(title="Total P&C and Manufacturing Export (Eastern Asia)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-total_pc_tota_manuf_exports-EAsia.png", dpi=400)
plt.clf()
fig = plotdata.ix["South-Eastern Asia"].plot(title="Total P&C and Manufacturing Export (South-Eastern Asia)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-total_pc_tota_manuf_exports-SEAsia.png", dpi=400)
plt.clf()

#-Region %share in Manufacturing Exports-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Final Products"])
fig = plotdata["%"].unstack(level="regionname")[["Eastern Asia", "South-Eastern Asia"]].plot(title="PC % share in Manufacturing Exports (by Region)", ylim=(0,0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_in_manuf_exports-EAsia+SEAsia.png", dpi=400)
plt.clf()
#-Table-#
table = plotdata["%"].unstack(level="regionname")
table.to_excel(RESULTS_DIR+"tables/"+"baci92-percent_pc_in_manuf_exports-UNRegions.xlsx")

#-Thesis Plots-#
plotdata = plotdata["%"].unstack(level="regionname") * 100
plotdata.columns.name = ""
plotdata = plotdata.rename(columns={'Eastern Asia':'East Asia', 'South-Eastern Asia':'South-East Asia', 'Southern Asia':'South Asia'})
fig = plotdata["East Asia"].plot(style='g-', legend=True)
fig = plotdata["South-East Asia"].plot(style='g:', legend=True)
fig = plotdata["South Asia"].plot(style='g*--', legend=True)
fig.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=3)
fig.set_ylabel("Percent PC (in Manufactures)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_tota_manuf_exports-East+SoutEast+SouthAsia-thesis.png", dpi=400, bbox_inches="tight")


#~~Country Analysis~~#

#-Total Exports-#

#-Total P&C in Total Exports (By Country)-#
plotdata = data.groupby(["country","year","pc"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.ix["JPN"].plot(title="Total P&C and Total Export (Japan)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-total_pc_and_total_exports-JPN.png", dpi=400)
plt.clf()
fig = plotdata.ix["CHN"].plot(title="Total P&C and Total Export (China)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-total_pc_and_total_exports-CHN.png", dpi=400)
plt.clf()

#-Country %share in Total Exports (By Country)-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Final Products"])
fig = plotdata["%"].unstack(level="country")[["CHN", "JPN", "TWN", "KOR"]].plot(title="Total P&C %share of Total Export (Countries)")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_in_total_exports-CHN+JPN+TWN+KOR.png", dpi=400, bbox_inches="tight")
plt.clf()
fig = plotdata["%"].unstack(level="country")[["IDN", "MYS", "PHL", "SGP", "THA", "VNM"]].plot(title="Total P&C %share of Total Export (Countries)")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_in_total_exports-IDN+MYS+PHL+SGP+THA+VNM.png", dpi=400, bbox_inches="tight")
plt.clf()
#-Table-#
table = plotdata["%"].unstack(level="country").T
table.to_excel(RESULTS_DIR+"tables/"+"baci92-percent_pc_in_total_exports-Countries.xlsx")
SELECTION = ["AUS","USA","CHN","JPN","IDN","IND","THA","MYS","PHL","VNM"]
table.ix[SELECTION].to_excel(RESULTS_DIR+"tables/"+"baci92-percent_pc_in_total_exports-SelectionCntry.xlsx")

#-Manufacturing Exports Only-#

#-Total P&C Exports in Manufacturing Exports (by Country)-#
plotdata = manuf.groupby(["country","year","pc"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products', 1:'Parts and Components'})
plotdata.columns.name = ""
fig = plotdata.ix["JPN"].plot(title="Total P&C and Manufacturing Export (Japan)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-total_pc_and_manuf_exports-JPN.png", dpi=400)
plt.clf()
fig = plotdata.ix["CHN"].plot(title="Total P&C and Manufacturing Export (China)")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-total_pc_and_manuf_exports-CHN.png", dpi=400)
plt.clf()

#-Country %share in Manufacturing Export-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Final Products"])
fig = plotdata["%"].unstack(level="country")[["CHN", "JPN", "TWN", "KOR"]].plot(title="Total P&C %share of Manufacturing Export (Countries)")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_in_manuf_exports-CHN+JPN+TWN+KOR.png", dpi=400, bbox_inches="tight")
plt.clf()
fig = plotdata["%"].unstack(level="country")[["IDN", "MYS", "PHL", "SGP", "THA", "VNM"]].plot(title="Total P&C %share of Manufacturing Export (Countries)")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_in_manuf_exports-IDN+MYS+PHL+SGP+THA+VNM.png", dpi=400, bbox_inches="tight")
plt.clf()
#-Table-#
table = plotdata["%"].unstack(level="country").T
table.to_excel(RESULTS_DIR+"tables/"+"baci92-percent_pc_in_manuf_exports-Countries.xlsx")
SELECTION = ["AUS","USA","CHN","JPN","IDN","IND","THA","MYS","PHL","VNM"]
table.ix[SELECTION].to_excel(RESULTS_DIR+"tables/"+"baci92-percent_pc_in_manuf_exports-SelectionCntry.xlsx")

#-Chart for Thesis-#
plotdata = plotdata["%"].unstack(level="country")*100
fig = plotdata["CHN"].plot(style='r-')
fig = plotdata["JPN"].plot(style='g--')
fig = plotdata["TWN"].plot(style='b*-')
fig = plotdata["KOR"].plot(style='c^-')
fig.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=4)
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.set_ylabel("Percent PC (in Manufactures)")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_and_manuf_exports-CHN+JPN+TWN+KOR-thesis.png", dpi=400, bbox_inches="tight")
plt.clf()
fig = plotdata["IDN"].plot(style='rs-')
fig = plotdata["MYS"].plot(style='gx--')
fig = plotdata["PHL"].plot(style='b*-')
fig = plotdata["SGP"].plot(style='c^-')
fig = plotdata["THA"].plot(style='m+:')
fig = plotdata["VNM"].plot(style='yo-')
fig.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=6)
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.set_ylabel("Percent PC (in Manufactures)")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_and_manuf_exports-IDN+MYS+PHL+SGP+THA+VNM-thesis.png", dpi=400, bbox_inches="tight")

#-Product Decompositions-#

#-Percent PC within Lall Categories-#
plotdata = data.groupby(["year","lall","pc"]).sum()["export"].unstack(level="pc")
plotdata.columns = ["OP","PC"]
plotdata.columns.name = ""
plotdata["%"] = (plotdata["PC"]/(plotdata["PC"]+plotdata["OP"]))*100
plotdata = plotdata["%"].fillna(0).unstack(level="lall")
plotdata.columns.name = ""
plotdata = plotdata[["PP","RB1","RB2","LT1","LT2","MT1","MT2","MT3","HT1","HT2"]]
plotdata["RB"] = plotdata[["RB1","RB2"]].mean(axis=1)
plotdata["LT"] = plotdata[["LT1","LT2"]].mean(axis=1)
fig = plotdata["HT2"].plot(style="ro--")
fig = plotdata["HT1"].plot(style="r^-")
fig = plotdata["MT3"].plot(style="mo--")
fig = plotdata["MT2"].plot(style="m^-.")
fig = plotdata["MT1"].plot(style="m*-")
fig = plotdata["LT"].plot(style="y^-")
fig = plotdata["RB"].plot(style="go-")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent PC within Lall Category")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_and_manuf_exports-lallcat-thesis.png", dpi=400, bbox_inches="tight")
plt.clf()

#-Country Concentration within Lall Type of Parts and Components Vs Other Goods-#
plotdata = data.groupby(["country","year","lall","pc"]).sum()["export"].unstack(level="pc")
plotdata.columns = ["OP","PC"]
plotdata.columns.name = ""
plotdata["%"] = (plotdata["PC"]/(plotdata["PC"]+plotdata["OP"]))*100
plotdata["pctot"] = plotdata["PC"].groupby(level=["country","year"]).transform(np.sum)
plotdata["%pctot"] = (plotdata["PC"]/plotdata["pctot"])*100
#-Japan-#
jpn = plotdata.ix["JPN"]["%pctot"].unstack(level="lall")
jpn.plot()

#-Lall Category Share (Composition) of Parts and Components Exports-#
plotdata = data.groupby(["year","lall","pc"]).sum()["export"].unstack(level="pc")
plotdata.columns = ["OP","PC"]
plotdata.columns.name = ""
plotdata = plotdata["PC"].dropna().to_frame()
plotdata = plotdata["PC"].groupby(level="year").apply(lambda x: 100*x/float(x.sum())) #-Percent Composition-#
plotdata = plotdata.unstack(level="lall")
plotdata = plotdata[["RB1","RB2","LT1","LT2","MT1","MT2","MT3","HT1","HT2"]]
plotdata["RB"] = plotdata[["RB1","RB2"]].mean(axis=1)
plotdata["LT"] = plotdata[["LT1","LT2"]].mean(axis=1)
fig = plotdata["HT2"].plot(style="ro--")
fig = plotdata["HT1"].plot(style="r^-")
fig = plotdata["MT3"].plot(style="mo--")
fig = plotdata["MT2"].plot(style="m^-.")
fig = plotdata["MT1"].plot(style="m*-")
fig = plotdata["LT"].plot(style="y^-")
fig = plotdata["RB"].plot(style="go-")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Composition of PC Export (by Value) in Lall Categories")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-composition_pc-lallcat-thesis.png", dpi=400, bbox_inches="tight")
plt.clf()

#-Lall Category Share (Composition) of Parts and Components Exports [By Country]-#
plotdata = data.groupby(["country","year","lall","pc"]).sum()["export"].unstack(level="pc")
plotdata.columns = ["OP","PC"]
plotdata.columns.name = ""
plotdata = plotdata["PC"].dropna().to_frame()
plotdata = plotdata["PC"].groupby(level=["country","year"]).apply(lambda x: 100*x/float(x.sum())) #-Percent Composition-#
plotdata = plotdata.unstack(level="lall")
plotdata = plotdata[["RB1","RB2","LT1","LT2","MT1","MT2","MT3","HT1","HT2"]]
plotdata["RB"] = plotdata[["RB1","RB2"]].mean(axis=1)
plotdata["LT"] = plotdata[["LT1","LT2"]].mean(axis=1)
cntry = "JPN"
cdata = plotdata.ix[cntry]
fig = cdata["HT2"].plot(style="ro--")
fig = cdata["HT1"].plot(style="r^-")
fig = cdata["MT3"].plot(style="mo--")
fig = cdata["MT2"].plot(style="m^-.")
fig = cdata["MT1"].plot(style="m*-")
fig = cdata["LT"].plot(style="y^-")
fig = cdata["RB"].plot(style="go-")
fig.set_title(cntry)
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Composition of PC Export (by Value) in Lall Categories")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-composition_pc-lallcat-country(%s)-thesis.png"%cntry, dpi=400, bbox_inches="tight")
plt.clf()

#-Decomposition within Leamer Categories-#
plotdata = data.groupby(["year","leamer","pc"]).sum()["export"].unstack(level="pc")
plotdata.columns = ["OP","PC"]
plotdata.columns.name = ""
plotdata["%"] = (plotdata["PC"]/(plotdata["PC"]+plotdata["OP"]))*100
plotdata = plotdata["%"].fillna(0).unstack(level="leamer")
plotdata.columns.name = ""
plotdata = plotdata[["1","2","3","4","5","6","7","8","9","10"]]
plotdata = plotdata.rename(columns={'7':'Labour Intensive','8':'Capital Intensive','9':'Machinery','10':'Chemical'})
fig = plotdata["Labour Intensive"].plot(style="bo-")
fig = plotdata["Capital Intensive"].plot(style="m^-")
fig = plotdata["Machinery"].plot(style="r*-")
fig = plotdata["Chemical"].plot(style="g^-")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent PC (in Leamer Category)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_and_manuf_exports-leamercat-thesis.png", dpi=400, bbox_inches="tight")

#-Decomposition within Product Space Categories-#
plotdata = data.groupby(["year","pscommunity","pc"]).sum()["export"].unstack(level="pc")
plotdata.columns = ["OP","PC"]
plotdata.columns.name = ""
plotdata["%"] = (plotdata["PC"]/(plotdata["PC"]+plotdata["OP"]))*100
plotdata = plotdata["%"].fillna(0).unstack(level="pscommunity")
plotdata.columns.name = ""
plotdata = plotdata.rename(columns=pscomm.to_dict()["description"])
plotdata = plotdata.rename(columns={'Construction Materials & Equipment':'Construction Equipment'})
fig = plotdata["Aircraft"].plot(style="ro-")
fig = plotdata["Electronics"].plot(style="b+-")
fig = plotdata["Machinery"].plot(style="g^-")
fig = plotdata["Construction Equipment"].plot(style="kx-")
fig = plotdata["Petrochemicals"].plot(style="m.-")
fig = plotdata["Textile & Fabrics"].plot(style="cs-")
fig = plotdata["Garments"].plot(style="yp-")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent PC (in Product Space Community)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_and_manuf_exports-selectpscommunities-thesis.png", dpi=400, bbox_inches="tight")

#-Product Space Share of Parts and Components Trade-#
plotdata = data.groupby(["year","pscommunity","pc"]).sum()["export"].unstack(level="pc")
plotdata.columns = ["OP","PC"]
plotdata.columns.name = ""
plotdata = plotdata["PC"].dropna().to_frame()
plotdata = plotdata["PC"].groupby(level="year").apply(lambda x: 100*x/float(x.sum())) #-Percent Composition-#
plotdata = plotdata.unstack(level="pscommunity")
plotdata = plotdata.rename(columns=pscomm.to_dict()["description"])
plotdata = plotdata.rename(columns={'Construction Materials & Equipment':'Construction Equipment'})
fig = plotdata["Machinery"].plot()
fig = plotdata["Electronics"].plot()
fig = plotdata["Aircraft"].plot()
fig = plotdata["Construction Equipment"].plot()
fig = plotdata["Chemicals and Health"].plot()
fig = plotdata["Boilers"].plot()
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Share (% Export Value) by Product Space Community")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-composition_pc-selectpscommunities-thesis.png", dpi=400, bbox_inches="tight")
pllt.clf()

#-Composition of P&C within SITC Chapter Level 2-#
data["sitc2"] = data["productcode"].apply(lambda x: x[0:2])
plotdata = data.groupby(["country","sitc2","year","pc"]).sum()["export"].unstack(level="pc")
plotdata.columns = ["OP","PC"]
plotdata.columns.name = ""
plotdata["pctot"] = plotdata["PC"].groupby(level=["country","year"]).transform(np.sum)
plotdata["%"] = (plotdata["PC"]/(plotdata["pctot"]))*100
plotdata = plotdata["%"].unstack(level="sitc2").fillna(0)
#-Comparable Across Countries-#
# fig = cdata["78"].plot(style="r+-")
# fig = cdata["77"].plot(style="g.--")
# fig = cdata["76"].plot(style="bo:")
# fig = cdata["75"].plot(style="c*-.")
# fig = cdata["74"].plot(style="yp-")
# fig = cdata["73"].plot(style="ms--")
# fig = cdata["72"].plot(style="rx:")
# fig = cdata["71"].plot(style="gD-.")
#-Japan-#
cdata = plotdata.ix["JPN"]
s = cdata.max()
s.sort(ascending=False) 	 #Top 5 ITems
print s[0:5].index
fig = cdata["77"].plot(style="g.--")
fig = cdata["75"].plot(style="c*-.")
fig = cdata["78"].plot(style="r+-")
fig = cdata["71"].plot(style="gD-.")
fig = cdata["72"].plot(style="rx:")
fig.set_title("JPN")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent Share of PC Exports")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-composition_pc_in_exports-JPN-thesis.png", dpi=400, bbox_inches="tight")
plt.cla()
#-China-#
cdata = plotdata.ix["CHN"]
s = cdata.max()
s.sort(ascending=False) 	 #Top 5 ITems
print s[0:5].index
fig = cdata["75"].plot(style="c*-.")
fig = cdata["76"].plot(style="bo:")
fig = cdata["77"].plot(style="g.--")
fig = cdata["71"].plot(style="gD-.")
fig = cdata["74"].plot(style="yp-")
fig.set_title("CHN")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent Share of PC Exports")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-composition_pc_in_exports-CHN-thesis.png", dpi=400, bbox_inches="tight")
plt.cla()
#-Taiwan-#
cdata = plotdata.ix["TWN"]
s = cdata.max()
s.sort(ascending=False) 	 #Top 5 ITems
print s[0:5].index
fig = cdata["75"].plot(style="c*-.")
fig = cdata["77"].plot(style="g.--")
fig = cdata["76"].plot(style="bo:")
fig = cdata["78"].plot(style="r+-")
fig = cdata["74"].plot(style="yp-")
fig.set_title("TWN")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent Share of PC Exports")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-composition_pc_in_exports-TWN-thesis.png", dpi=400, bbox_inches="tight")
plt.cla()
#-THA-#
cdata = plotdata.ix["THA"]
s = cdata.max()
s.sort(ascending=False) 	 #Top 5 ITems
print s[0:5].index
fig = cdata["75"].plot(style="c*-.")
fig = cdata["77"].plot(style="g.--")
fig = cdata["78"].plot(style="g^-")
fig = cdata["76"].plot(style="bo:")
fig = cdata["74"].plot(style="yp-")
fig.set_title("THA")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent Share of PC Exports")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-composition_pc_in_exports-THA-thesis.png", dpi=400, bbox_inches="tight")
plt.cla()
#-IDN-#
cdata = plotdata.ix["IDN"]
s = cdata.max()
s.sort(ascending=False) 	 #Top 5 ITems
print s[0:5].index
fig = cdata["77"].plot(style="g.--")
fig = cdata["75"].plot(style="c*-.")
fig = cdata["76"].plot(style="bo:")
fig = cdata["78"].plot(style="g^-")
fig = cdata["72"].plot(style="rx:")
fig.set_title("IDN")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent Share of PC Exports")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-composition_pc_in_exports-IDN-thesis.png", dpi=400, bbox_inches="tight")
plt.cla()
#-MYS-#
cdata = plotdata.ix["MYS"]
s = cdata.max()
s.sort(ascending=False) 	 #Top 5 ITems
print s[0:5].index
fig = cdata["75"].plot(style="c*-.")
fig = cdata["77"].plot(style="g.--")
fig = cdata["76"].plot(style="bo:")
fig = cdata["72"].plot(style="rx:")
fig = cdata["71"].plot(style="gD-.")
fig.set_title("MYS")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent Share of PC Exports")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-composition_pc_in_exports-MYS-thesis.png", dpi=400, bbox_inches="tight")
plt.cla()
#-PHL-#
cdata = plotdata.ix["PHL"]
s = cdata.max()
s.sort(ascending=False) 	 #Top 5 ITems
print s[0:5].index
fig = cdata["77"].plot(style="g.--")
fig = cdata["75"].plot(style="c*-.")
fig = cdata["76"].plot(style="bo:")
fig = cdata["78"].plot(style="g^-")
fig = cdata["71"].plot(style="gD-.")
fig.set_title("PHL")
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent Share of PC Exports")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-composition_pc_in_exports-PHL-thesis.png", dpi=400, bbox_inches="tight")
plt.cla()

#------------------#
#-Network Products-#
#------------------#

#
#-Plots
#

#-Work with BACI EXPORT Data-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-export-%s-dataset.dta"%(year))
	if year == 1995:
		data = df
	else:
		data = data.append(df)
data.reset_index(inplace=True, drop=True)
data["export"] = data["export"]*1000 			#Scale Exports by 1000

#-Locate Manufactured Trade-#
manuf = data.loc[data.sitccat == 'M'].reset_index(drop=True)

#-----------------#
#-Within Analysis-#
#-----------------#

#-Total Network Products in Total Manufacturing Exports-#
plotdata = manuf.groupby(["year","np"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'Final Products (Manufacturing)', 1:'Network Products'})
plotdata.columns.name = ""
fig = plotdata.plot()
fig.set_ylabel("US$ Trillion")
fig.set_xlabel("Year")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-total_np_total_manuf_exports.png", dpi=400)
plt.clf()
#-Percent Network Products-#
plotdata["%"] = plotdata["Network Products"] / (plotdata["Network Products"] + plotdata["Final Products (Manufacturing)"])
fig2 = plotdata["%"].plot(ylim=0, title="Percent (Network Products) in Total Manufacturing Exports")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_np_total_manuf_exports.png", dpi=400)
plt.clf()
#-Combined Plot-#
plotdata = manuf.groupby(["year","np"]).sum()["export"]
plotdata = plotdata.unstack()
plotdata = plotdata.rename(columns={0:'FM', 1:'NP'})
plotdata.columns.name = ""
plotdata["%NP"] = (plotdata["NP"] / (plotdata["NP"] + plotdata["FM"]))*100
fig = plotdata.plot(secondary_y=["%NP"], xticks=xrange(1995,2013+1,2))
fig.set_ylabel("US$ Trillion")
fig.set_xlabel("Year")
fig.right_ax.set_ylabel("Percent NP (of Manufactures)")
fig.right_ax.set_ylim((0,100))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-np_total_manuf_exports_withpercent-thesis.png", dpi=400)

#-Decomposition Network Trade (P&C and Final Assembly Products)-#
df = manuf.copy()
df["ptype"] = df["pc"].apply(lambda x: "pc" if x == 1 else "other")
df["ptype"] = df[["ptype","fap"]].apply(lambda row: "fap" if row["fap"] == 1 else row["ptype"], axis=1)
df = df.groupby(["year","ptype"])["export"].sum()
dfp = df.groupby("year").apply(lambda x: 100*x/float(x.sum()))
fig = dfp.unstack().plot()

#-Share of Parts and Components in Network Products-#
df = manuf.copy()
df1 = df.groupby(["year","country","np"]).sum()["export"].unstack()
df1.columns = ["np=0","np=1"]
df1 = df1["np=1"]
df2 = df.groupby(["year","country","pc"]).sum()["export"].unstack()
df2.columns = ["pc=0","pc=1"]
df2 = df2["pc=1"]
df3 = df2/df1
df3 = df3.reorder_levels([1,0])
df3.ix["JPN"].plot(ylim=0)

#------------------#
#-Between Analysis-#
#------------------#

#/Plots/pd.options.display.mpl_style = 'default'#

#-World Shares-#

#-Network Products-#
netp = data.loc[data.np == 1]

#-Areas (World Share) in Network Product Export-#
df = netp.groupby(["year","areaname"]).sum()["export"]
df = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum())).unstack()
plotdata = df.rename(columns={'Latin America and the Caribbean':'Latin America'})
fig = plotdata["Africa"].plot(style='r+-', legend=True)
fig = plotdata["Asia"].plot(style='b.--', legend=True)
fig = plotdata["Europe"].plot(style='go:', legend=True)
fig = plotdata["Latin America"].plot(style='c*-.', legend=True)
fig = plotdata["Northern America"].plot(style='kp-', legend=True)
fig = plotdata["Oceania"].plot(style='ys--', legend=True)
fig.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=3)
fig.set_ylabel("World Share in Network \nProduct Exports (%)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-np-world_export_shares-byarea-thesis.png", dpi=400, bbox_inches="tight")

#-Region (World Share) in Network Product Export-#
df = netp.groupby(["year","regionname"]).sum()["export"]
plotdata = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum())).unstack()
plotdata = plotdata.rename(columns={'Eastern Asia':'East Asia', 'South-Eastern Asia':'South-East Asia', 
									'Western Europe':'West Europe', 'Northern America':'North America',
									'Eastern Europe':'East Europe', 'Northern Europe':'North Europe',
									'Southern Europe':'South Europe'})
	# #-Area Plot-#
	# fig = plotdata[["East Asia","South-East Asia","North America","Central America","South America","West Europe",
	# 				"East Europe","South Europe"]].plot(kind='area', stacked=True)
	# fig.legend(loc='center left', bbox_to_anchor=(1, 0.5)) 		#-Lines not large enough-Not Time to Fix-#
	# fig.set_ylabel("World Share in Network \nProduct Exports (%)")
	# fig.set_ylim(bottom=0)
	# fig.set_xlabel("Year")
	# fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
	# plt.tight_layout()
#-Line Plot-#
fig = plotdata["East Asia"].plot(style='r+-')
fig = plotdata["South-East Asia"].plot(style='b.-')
fig = plotdata["North America"].plot(style='go-')
fig = plotdata["Central America"].plot(style='c*-')
fig = plotdata["South America"].plot(style='mp-')
fig = plotdata["West Europe"].plot(style='ks-')
fig = plotdata["East Europe"].plot(style='rx--')
fig = plotdata["South Europe"].plot(style='bD--')
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5)) 
fig.set_ylabel("World Share in Network \nProduct Exports (%)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-np-world_export_shares-byregion-thesis.png", dpi=400, bbox_inches="tight")

#-Country (World Share) in Network Product Export-#
df = netp.groupby(["year","country"]).sum()["export"]
plotdata = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum())).unstack()
#-Line Plot-#
fig = plotdata["CHN"].plot(style='r+-')
fig = plotdata["DEU"].plot(style='b.-')
fig = plotdata["USA"].plot(style='go-')
fig = plotdata["JPN"].plot(style='c*-')
fig = plotdata["KOR"].plot(style='mp-')
fig = plotdata["TWN"].plot(style='ys-')
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5)) 
fig.set_ylabel("World Share in Network \nProduct Exports (%)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-np-world_export_shares-bycntry-top6-thesis.png", dpi=400, bbox_inches="tight")

#-Parts and Components-#
pc = data.loc[data.pc == 1]

#-Areas (World Share) in Parts and Component Exports-#
df = pc.groupby(["year","areaname"]).sum()["export"]
df = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum())).unstack()
plotdata = df.rename(columns={'Latin America and the Caribbean':'Latin America'})
fig = plotdata["Africa"].plot(style='r+-', legend=True)
fig = plotdata["Asia"].plot(style='b.--', legend=True)
fig = plotdata["Europe"].plot(style='go:', legend=True)
fig = plotdata["Latin America"].plot(style='c*-.', legend=True)
fig = plotdata["Northern America"].plot(style='kp-', legend=True)
fig = plotdata["Oceania"].plot(style='ys--', legend=True)
fig.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=3)
fig.set_ylabel("World Share in Parts & \nComponents Exports (%)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-pc-world_export_shares-byarea-thesis.png", dpi=400, bbox_inches="tight")

#-Region (World Share) in Parts and Components Export-#
df = pc.groupby(["year","regionname"]).sum()["export"]
plotdata = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum())).unstack()
plotdata = plotdata.rename(columns={'Eastern Asia':'East Asia', 'South-Eastern Asia':'South-East Asia', 
									'Western Europe':'West Europe', 'Northern America':'North America',
									'Eastern Europe':'East Europe', 'Northern Europe':'North Europe',
									'Southern Europe':'South Europe'})
	# #-Area Plot-#
	# fig = plotdata[["East Asia","South-East Asia","North America","Central America","South America","West Europe",
	# 				"East Europe","South Europe"]].plot(kind='area', stacked=True)
	# fig.legend(loc='center left', bbox_to_anchor=(1, 0.5)) 		#-Lines not large enough-Not Time to Fix-#
	# fig.set_ylabel("World Share in Parts & \nComponents Exports (%)")
	# fig.set_ylim(bottom=0)
	# fig.set_xlabel("Year")
	# fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
	# plt.tight_layout()
#-Line Plot-#
fig = plotdata["East Asia"].plot(style='r+-')
fig = plotdata["South-East Asia"].plot(style='b.-')
fig = plotdata["North America"].plot(style='go-')
fig = plotdata["Central America"].plot(style='c*-')
fig = plotdata["South America"].plot(style='mp-')
fig = plotdata["West Europe"].plot(style='ks-')
fig = plotdata["East Europe"].plot(style='rx--')
fig = plotdata["South Europe"].plot(style='bD--')
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5)) 
fig.set_ylabel("World Share in Parts & \nComponents Exports (%)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-pc-world_export_shares-byregion-thesis.png", dpi=400, bbox_inches="tight")

#-Country (World Share) in Network Trade-#
df = pc.groupby(["year","country"]).sum()["export"]
plotdata = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum())).unstack()
s = df.ix[2013]
s.sort(ascending=False)
print s[0:5]
#-Line Plot-#
fig = plotdata["CHN"].plot(style='r+-')
fig = plotdata["DEU"].plot(style='b.-')
fig = plotdata["USA"].plot(style='go-')
fig = plotdata["JPN"].plot(style='c*-')
fig = plotdata["KOR"].plot(style='mp-')
fig = plotdata["TWN"].plot(style='ys-')
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5)) 
fig.set_ylabel("World Share in Parts & \nComponents Exports (%)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-pc-world_export_shares-bycntry-top6-thesis.png", dpi=400, bbox_inches="tight")

#
#-Final Assembly Products-#
#
fap = data.loc[data.fap == 1]

#-Areas (World Share) in Final Assembly Exports-#
df = fap.groupby(["year","areaname"]).sum()["export"]
df = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum())).unstack()
plotdata = df.rename(columns={'Latin America and the Caribbean':'Latin America'})
fig = plotdata["Africa"].plot(style='r+-', legend=True)
fig = plotdata["Asia"].plot(style='b.--', legend=True)
fig = plotdata["Europe"].plot(style='go:', legend=True)
fig = plotdata["Latin America"].plot(style='c*-.', legend=True)
fig = plotdata["Northern America"].plot(style='kp-', legend=True)
fig = plotdata["Oceania"].plot(style='ys--', legend=True)
fig.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=3)
fig.set_ylabel("World Share in Final Assembly \nExports (%)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-fap-world_export_shares-byarea-thesis.png", dpi=400, bbox_inches="tight")

#-Region (World Share) in Final Assembly Export-#
df = fap.groupby(["year","regionname"]).sum()["export"]
plotdata = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum())).unstack()
plotdata = plotdata.rename(columns={'Eastern Asia':'East Asia', 'South-Eastern Asia':'South-East Asia', 
									'Western Europe':'West Europe', 'Northern America':'North America',
									'Eastern Europe':'East Europe', 'Northern Europe':'North Europe',
									'Southern Europe':'South Europe'})
	# #-Area Plot-#
	# fig = plotdata[["East Asia","South-East Asia","North America","Central America","South America","West Europe",
	# 				"East Europe","South Europe"]].plot(kind='area', stacked=True)
	# fig.legend(loc='center left', bbox_to_anchor=(1, 0.5)) 		#-Lines not large enough-Not Time to Fix-#
	# fig.set_ylabel("World Share in Final Assembly \nExports (%)")
	# fig.set_ylim(bottom=0)
	# fig.set_xlabel("Year")
	# fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
	# plt.tight_layout()
#-Line Plot-#
fig = plotdata["East Asia"].plot(style='r+-')
fig = plotdata["South-East Asia"].plot(style='b.-')
fig = plotdata["North America"].plot(style='go-')
fig = plotdata["Central America"].plot(style='c*-')
fig = plotdata["South America"].plot(style='mp-')
fig = plotdata["West Europe"].plot(style='ks-')
fig = plotdata["East Europe"].plot(style='rx--')
fig = plotdata["South Europe"].plot(style='bD--')
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5)) 
fig.set_ylabel("World Share in Final Assembly \nExports (%)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-fap-world_export_shares-byregion-thesis.png", dpi=400, bbox_inches="tight")

#-Country (World Share) in Final Assembly Exports-#
df = fap.groupby(["year","country"]).sum()["export"]
plotdata = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum())).unstack()
s = plotdata.ix[2013]
s.sort(ascending=False)
print s[0:5]
#-Line Plot-#
fig = plotdata["CHN"].plot(style='r+-')
fig = plotdata["DEU"].plot(style='b.-')
fig = plotdata["USA"].plot(style='go-')
fig = plotdata["JPN"].plot(style='c*-')
fig = plotdata["KOR"].plot(style='mp-')
fig = plotdata["TWN"].plot(style='ys-')
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5)) 
fig.set_ylabel("World Share in Final Assembly \nExports (%)")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-fap-world_export_shares-bycntry-top6-thesis.png", dpi=400, bbox_inches="tight")

#
#-Composition Tables-#
#

#--------#
#-EXPORT-#
#--------#

#-BACI EXPORT DATA-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-export-%s-dataset.dta"%(year))
	if year == 1995:
		data = df
	else:
		data = data.append(df)
data.reset_index(inplace=True, drop=True)
data["export"] = data["export"]*1000 			#Scale Exports by 1000

#-Manufactured Trade
manuf = data.loc[data.sitccat == 'M'].reset_index(drop=True)
#-Final Assembly Products
fap = data.loc[data.fap == 1]
#-Parts and Components
pc = data.loc[data.pc == 1]
#-Network Products
netp = data.loc[data.np == 1]

#~~World Shares Table~~#

#-Area World Share Tables-#
#-Between-#
#-Country (World Share) in Total Exports-#
df = data.groupby(["year","areaname"]).sum()["export"]
sh0 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh0 = sh0.unstack(level="year")
sh0.name = "Total"
#-Country (World Share) in Manufacturing Export-#
df = manuf.groupby(["year","areaname"]).sum()["export"]
sh1 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh1 = sh1.unstack(level="year")
sh1.name = "Manufacturing"
#-Country (World Share) in Network Product Export-#
df = netp.groupby(["year","areaname"]).sum()["export"]
sh2 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh2 = sh2.unstack(level="year")
sh2.name = "Network Products"
#-Country (World Share) in Parts and Components-#
df = pc.groupby(["year","areaname"]).sum()["export"]
sh3 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh3 = sh3.unstack(level="year")
sh3.name = "Parts & Components"
#-Country (World Share) in Final Assembly Network Products-#
df = netp.groupby(["year","areaname"]).sum()["export"]
sh4 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh4 = sh4.unstack(level="year")
sh4.name = "Final Assembly"
#-Within-#
#-Country Share of Parts and Components in Total Network Produts-#
df1 = netp.groupby(["year","areaname"]).sum()["export"]
df2 = pc.groupby(["year","areaname"]).sum()["export"]
sh5 = df2/df1*100
sh5 = sh5.unstack(level="year")
sh5.name = "Share PC in Network Products (%)"
#-Construct Export Table-#
years = [1995,2000,2005,2013]
name = sh0.name
sh0 = sh0[years]
sh0.name = name
sh0.columns = pd.MultiIndex.from_arrays([sh0.columns,[sh0.name]*len(years)])
table = sh0
for item in [sh1,sh2,sh3,sh4,sh5]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-export-world_shares-areas-pc-nt-fap-tot-manuf.csv") 	#Excel Writer is Currently Broken

#-Region World Share Tables-#
#-Between-#
#-Country (World Share) in Total Exports-#
df = data.groupby(["year","regionname"]).sum()["export"]
sh0 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh0 = sh0.unstack(level="year")
sh0.name = "Total"
#-Country (World Share) in Manufacturing Export-#
df = manuf.groupby(["year","regionname"]).sum()["export"]
sh1 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh1 = sh1.unstack(level="year")
sh1.name = "Manufacturing"
#-Country (World Share) in Network Product Export-#
df = netp.groupby(["year","regionname"]).sum()["export"]
sh2 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh2 = sh2.unstack(level="year")
sh2.name = "Network Products"
#-Country (World Share) in Parts and Components-#
df = pc.groupby(["year","regionname"]).sum()["export"]
sh3 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh3 = sh3.unstack(level="year")
sh3.name = "Parts & Components"
#-Country (World Share) in Final Assembly Network Products-#
df = netp.groupby(["year","regionname"]).sum()["export"]
sh4 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh4 = sh4.unstack(level="year")
sh4.name = "Final Assembly"
#-Within-#
#-Country Share of Parts and Components in Total Network Produts-#
df1 = netp.groupby(["year","regionname"]).sum()["export"]
df2 = pc.groupby(["year","regionname"]).sum()["export"]
sh5 = df2/df1*100
sh5 = sh5.unstack(level="year")
sh5.name = "Share PC in Network Products (%)"
#-Construct Export Table-#
years = [1995,2000,2005,2013]
name = sh0.name
sh0 = sh0[years]
sh0.name = name
sh0.columns = pd.MultiIndex.from_arrays([sh0.columns,[sh0.name]*len(years)])
table = sh0
for item in [sh1,sh2,sh3,sh4,sh5]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-export-world_shares-regions-pc-nt-fap-tot-manuf.csv") 	#Excel Writer is Currently Broken

#-Country World Share Tables-#
#-Between-#
#-Country (World Share) in Total Exports-#
df = data.groupby(["year","country"]).sum()["export"]
sh0 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh0 = sh0.unstack(level="year")
sh0.name = "Total"
#-Country (World Share) in Manufacturing Export-#
df = manuf.groupby(["year","country"]).sum()["export"]
sh1 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh1 = sh1.unstack(level="year")
sh1.name = "Manufacturing"
#-Country (World Share) in Network Product Export-#
df = netp.groupby(["year","country"]).sum()["export"]
sh2 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh2 = sh2.unstack(level="year")
sh2.name = "Network Products"
#-Country (World Share) in Parts and Components-#
df = pc.groupby(["year","country"]).sum()["export"]
sh3 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh3 = sh3.unstack(level="year")
sh3.name = "Parts & Components"
#-Country (World Share) in Final Assembly Network Products-#
df = netp.groupby(["year","country"]).sum()["export"]
sh4 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh4 = sh4.unstack(level="year")
sh4.name = "Final Assembly"
#-Within-#
#-Country Share of Parts and Components in Total Network Produts-#
df1 = netp.groupby(["year","country"]).sum()["export"]
df2 = pc.groupby(["year","country"]).sum()["export"]
sh5 = df2/df1*100
sh5 = sh5.unstack(level="year")
sh5.name = "Share PC in Network Products (%)"
#-Construct Export Table-#
cntrys = ["JPN","CHN","HKG","TWN","KOR","IDN","MYS","PHL","SGP","THA","VNM","IND","BGD","PAK","CAN","USA","MEX","ARG","BRA","CHL","GBR","DEU","FRA","ITA","RUS"]
years = [1995,2000,2005,2013]
name = sh0.name
sh0 = sh0[years]
sh0.name = name
sh0.columns = pd.MultiIndex.from_arrays([sh0.columns,[sh0.name]*len(years)])
table = sh0
for item in [sh1,sh2,sh3,sh4,sh5]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-export-world_shares-cntry-pc-nt-fap-tot-manuf-all.csv") 	#Excel Writer is Currently Broken
table = table.ix[cntrys]
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-export-world_shares-cntry-pc-nt-fap-tot-manuf-selection.csv") 	#Excel Writer is Currently Broken


#~~Country Share WITHIN Compositions~~#

#-Share of Network Products in Manufacturing Export (By Area)-#
dfmp = manuf.groupby(["year","areaname"]).sum()["export"]
dfpc = pc.groupby(["year","areaname"]).sum()["export"]
dffap = fap.groupby(["year","areaname"]).sum()["export"]
dfnp = netp.groupby(["year","areaname"]).sum()["export"]
#-compositions-#
shpc = dfpc.div(dfmp)*100
shpc = shpc.unstack(level="year")
shpc.name = "Parts & Components"
shfap = dffap.div(dfmp)*100
shfap = shfap.unstack(level="year")
shfap.name = "Final Assembly"
shnp = dfnp.div(dfmp)*100
shnp = shnp.unstack(level="year")
shnp.name = "Total Network Products"
#-Table-#
years = [1995,2000,2005,2013]
name = shpc.name
shpc = shpc[years]
shpc.name = name
shpc.columns = pd.MultiIndex.from_arrays([shpc.columns,[shpc.name]*len(years)])
table = shpc
for item in [shfap,shnp]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-export-area_shares(in_manuf)-pc+fap+np.csv") 

#-Share of Network PRoducts in Manufacturing Export (By Region)-#
dfmp = manuf.groupby(["year","regionname"]).sum()["export"]
dfpc = pc.groupby(["year","regionname"]).sum()["export"]
dffap = fap.groupby(["year","regionname"]).sum()["export"]
dfnp = netp.groupby(["year","regionname"]).sum()["export"]
#-compositions-#
shpc = dfpc.div(dfmp)*100
shpc = shpc.unstack(level="year")
shpc.name = "Parts & Components"
shfap = dffap.div(dfmp)*100
shfap = shfap.unstack(level="year")
shfap.name = "Final Assembly"
shnp = dfnp.div(dfmp)*100
shnp = shnp.unstack(level="year")
shnp.name = "Total Network Products"
#-Table-#
years = [1995,2000,2005,2013]
name = shpc.name
shpc = shpc[years]
shpc.name = name
shpc.columns = pd.MultiIndex.from_arrays([shpc.columns,[shpc.name]*len(years)])
table = shpc
for item in [shfap,shnp]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-export-region_shares(in_manuf)-pc+fap+np.csv") 

#-Share of Network Products in Manufacturing Export (By Country)-#
dfmp = manuf.groupby(["year","country"]).sum()["export"]
dfpc = pc.groupby(["year","country"]).sum()["export"]
dffap = fap.groupby(["year","country"]).sum()["export"]
dfnp = netp.groupby(["year","country"]).sum()["export"]
#-compositions-#
shpc = dfpc.div(dfmp)*100
shpc = shpc.unstack(level="year")
shpc.name = "Parts & Components"
shfap = dffap.div(dfmp)*100
shfap = shfap.unstack(level="year")
shfap.name = "Final Assembly"
shnp = dfnp.div(dfmp)*100
shnp = shnp.unstack(level="year")
shnp.name = "Total Network Products"
#-Table-#
cntrys = ["JPN","CHN","HKG","TWN","KOR","IDN","MYS","PHL","SGP","THA","VNM","IND","BGD","PAK","CAN","USA","MEX","ARG","BRA","CHL","GBR","DEU","FRA","ITA","RUS"]
years = [1995,2000,2005,2013]
name = shpc.name
shpc = shpc[years]
shpc.name = name
shpc.columns = pd.MultiIndex.from_arrays([shpc.columns,[shpc.name]*len(years)])
table = shpc
for item in [shfap,shnp]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-export-cntry_shares(in_manuf)-pc+fap+np-all.csv")
table = table.ix[cntrys]
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-export-cntry_shares(in_manuf)-pc+fap+np-selection.csv") 


#--------#
#-IMPORT-#
#--------#

#-BACI IMPORT DATA-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-import-%s-dataset.dta"%(year))
	if year == 1995:
		data = df
	else:
		data = data.append(df)
data.reset_index(inplace=True, drop=True)
data["import"] = data["import"]*1000 			#Scale Exports by 1000

#-Manufactured Trade
manuf = data.loc[data.sitccat == 'M'].reset_index(drop=True)
#-Final Assembly Products
fap = data.loc[data.fap == 1]
#-Parts and Components
pc = data.loc[data.pc == 1]
#-Network Products
netp = data.loc[data.np == 1]

#~~World Share Tables~~#

#-Area World Share Tables-#
#-Between-#
#-Country (World Share) in Total Imports-#
df = data.groupby(["year","areaname"]).sum()["import"]
sh0 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh0 = sh0.unstack(level="year")
sh0.name = "Total"
#-Country (World Share) in Manufacturing Import-#
df = manuf.groupby(["year","areaname"]).sum()["import"]
sh1 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh1 = sh1.unstack(level="year")
sh1.name = "Manufacturing"
#-Country (World Share) in Network Product Import-#
df = netp.groupby(["year","areaname"]).sum()["import"]
sh2 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh2 = sh2.unstack(level="year")
sh2.name = "Network Products"
#-Country (World Share) in Parts and Components-#
df = pc.groupby(["year","areaname"]).sum()["import"]
sh3 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh3 = sh3.unstack(level="year")
sh3.name = "Parts & Components"
#-Country (World Share) in Final Assembly Network Products-#
df = netp.groupby(["year","areaname"]).sum()["import"]
sh4 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh4 = sh4.unstack(level="year")
sh4.name = "Final Assembly"
#-Within-#
#-Country Share of Parts and Components in Total Network Produts-#
df1 = netp.groupby(["year","areaname"]).sum()["import"]
df2 = pc.groupby(["year","areaname"]).sum()["import"]
sh5 = df2/df1*100
sh5 = sh5.unstack(level="year")
sh5.name = "Share PC in Network Products (%)"
#-Construct Import Table-#
years = [1995,2000,2005,2013]
name = sh0.name
sh0 = sh0[years]
sh0.name = name
sh0.columns = pd.MultiIndex.from_arrays([sh0.columns,[sh0.name]*len(years)])
table = sh0
for item in [sh1,sh2,sh3,sh4,sh5]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-import-world_shares-areas-pc-nt-fap-tot-manuf.csv") 	#Excel Writer is Currently Broken

#-Region World Share Tables-#
#-Between-#
#-Country (World Share) in Total Imports-#
df = data.groupby(["year","regionname"]).sum()["import"]
sh0 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh0 = sh0.unstack(level="year")
sh0.name = "Total"
#-Country (World Share) in Manufacturing Import-#
df = manuf.groupby(["year","regionname"]).sum()["import"]
sh1 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh1 = sh1.unstack(level="year")
sh1.name = "Manufacturing"
#-Country (World Share) in Network Product Import-#
df = netp.groupby(["year","regionname"]).sum()["import"]
sh2 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh2 = sh2.unstack(level="year")
sh2.name = "Network Products"
#-Country (World Share) in Parts and Components-#
df = pc.groupby(["year","regionname"]).sum()["import"]
sh3 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh3 = sh3.unstack(level="year")
sh3.name = "Parts & Components"
#-Country (World Share) in Final Assembly Network Products-#
df = netp.groupby(["year","regionname"]).sum()["import"]
sh4 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh4 = sh4.unstack(level="year")
sh4.name = "Final Assembly"
#-Within-#
#-Country Share of Parts and Components in Total Network Produts-#
df1 = netp.groupby(["year","regionname"]).sum()["import"]
df2 = pc.groupby(["year","regionname"]).sum()["import"]
sh5 = df2/df1*100
sh5 = sh5.unstack(level="year")
sh5.name = "Share PC in Network Products (%)"
#-Construct Import Table-#
years = [1995,2000,2005,2013]
name = sh0.name
sh0 = sh0[years]
sh0.name = name
sh0.columns = pd.MultiIndex.from_arrays([sh0.columns,[sh0.name]*len(years)])
table = sh0
for item in [sh1,sh2,sh3,sh4,sh5]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-import-world_shares-regions-pc-nt-fap-tot-manuf.csv") 	#Excel Writer is Currently Broken


#-Country World Composition Tables-#
#-Between-#
#-Country (World Share) in Total Imports-#
df = data.groupby(["year","country"]).sum()["import"]
sh0 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh0 = sh0.unstack(level="year")
sh0.name = "Total"
#-Country (World Share) in Manufacturing Imports-#
df = manuf.groupby(["year","country"]).sum()["import"]
sh1 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh1 = sh1.unstack(level="year")
sh1.name = "Manufacturing"
#-Country (World Share) in Network Product Imports-#
df = netp.groupby(["year","country"]).sum()["import"]
sh2 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh2 = sh2.unstack(level="year")
sh2.name = "Network Products"
#-Country (World Share) in Parts and Components Imports-#
df = pc.groupby(["year","country"]).sum()["import"]
sh3 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh3 = sh3.unstack(level="year")
sh3.name = "Parts & Components"
#-Country (World Share) in Final Assembly Network Products Imports-#
df = netp.groupby(["year","country"]).sum()["import"]
sh4 = df.groupby(level=["year"]).apply(lambda x: 100*x/float(x.sum()))
sh4 = sh4.unstack(level="year")
sh4.name = "Final Assembly"
#-Within-#
#-Country Share of Parts and Components in Total Network Produts Imports-#
df1 = netp.groupby(["year","country"]).sum()["import"]
df2 = pc.groupby(["year","country"]).sum()["import"]
sh5 = df2/df1*100
sh5 = sh5.unstack(level="year")
sh5.name = "Share PC in Network Products (%)"
#-Construct Export Table-#
cntrys = ["JPN","CHN","HKG","TWN","KOR","IDN","MYS","PHL","SGP","THA","VNM","IND","BGD","PAK","CAN","USA","MEX","ARG","BRA","CHL","GBR","DEU","FRA","ITA","RUS"]
years = [1995,2000,2005,2013]
name = sh0.name
sh0 = sh0[years]
sh0.name = name
sh0.columns = pd.MultiIndex.from_arrays([sh0.columns,[sh0.name]*len(years)])
table = sh0
for item in [sh1,sh2,sh3,sh4,sh5]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-import-world_shares-pc-nt-fap-tot-manuf-all.csv") 	#Excel Writer is Currently Broken
table = table.ix[cntrys]
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-import-world_shares-pc-nt-fap-tot-manuf-selection.csv") 	#Excel Writer is Currently Broken

#~~Country Share WITHIN Compositions~~#

#-Share of Network Products in Manufacturing Import (By Area)-#
dfmp = manuf.groupby(["year","areaname"]).sum()["import"]
dfpc = pc.groupby(["year","areaname"]).sum()["import"]
dffap = fap.groupby(["year","areaname"]).sum()["import"]
dfnp = netp.groupby(["year","areaname"]).sum()["import"]
#-compositions-#
shpc = dfpc.div(dfmp)*100
shpc = shpc.unstack(level="year")
shpc.name = "Parts & Components"
shfap = dffap.div(dfmp)*100
shfap = shfap.unstack(level="year")
shfap.name = "Final Assembly"
shnp = dfnp.div(dfmp)*100
shnp = shnp.unstack(level="year")
shnp.name = "Total Network Products"
#-Table-#
years = [1995,2000,2005,2013]
name = shpc.name
shpc = shpc[years]
shpc.name = name
shpc.columns = pd.MultiIndex.from_arrays([shpc.columns,[shpc.name]*len(years)])
table = shpc
for item in [shfap,shnp]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-import-area_shares(in_manuf)-pc+fap+np.csv") 

#-Share of Network Products in Manufacturing Import (By Region)-#
dfmp = manuf.groupby(["year","regionname"]).sum()["import"]
dfpc = pc.groupby(["year","regionname"]).sum()["import"]
dffap = fap.groupby(["year","regionname"]).sum()["import"]
dfnp = netp.groupby(["year","regionname"]).sum()["import"]
#-compositions-#
shpc = dfpc.div(dfmp)*100
shpc = shpc.unstack(level="year")
shpc.name = "Parts & Components"
shfap = dffap.div(dfmp)*100
shfap = shfap.unstack(level="year")
shfap.name = "Final Assembly"
shnp = dfnp.div(dfmp)*100
shnp = shnp.unstack(level="year")
shnp.name = "Total Network Products"
#-Table-#
years = [1995,2000,2005,2013]
name = shpc.name
shpc = shpc[years]
shpc.name = name
shpc.columns = pd.MultiIndex.from_arrays([shpc.columns,[shpc.name]*len(years)])
table = shpc
for item in [shfap,shnp]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-import-region_shares(in_manuf)-pc+fap+np.csv") 

#-Share of Network Products in Manufacturing Import (By Country)-#
dfmp = manuf.groupby(["year","country"]).sum()["import"]
dfpc = pc.groupby(["year","country"]).sum()["import"]
dffap = fap.groupby(["year","country"]).sum()["import"]
dfnp = netp.groupby(["year","country"]).sum()["import"]
#-compositions-#
shpc = dfpc.div(dfmp)*100
shpc = shpc.unstack(level="year")
shpc.name = "Parts & Components"
shfap = dffap.div(dfmp)*100
shfap = shfap.unstack(level="year")
shfap.name = "Final Assembly"
shnp = dfnp.div(dfmp)*100
shnp = shnp.unstack(level="year")
shnp.name = "Total Network Products"
#-Table-#
cntrys = ["JPN","CHN","HKG","TWN","KOR","IDN","MYS","PHL","SGP","THA","VNM","IND","BGD","PAK","CAN","USA","MEX","ARG","BRA","CHL","GBR","DEU","FRA","ITA","RUS"]
years = [1995,2000,2005,2013]
name = shpc.name
shpc = shpc[years]
shpc.name = name
shpc.columns = pd.MultiIndex.from_arrays([shpc.columns,[shpc.name]*len(years)])
table = shpc
for item in [shfap,shnp]:
	name = item.name
	item = item[years]
	item.name = name
	item.columns = pd.MultiIndex.from_arrays([item.columns,[item.name]*len(years)])
	table = table.merge(item, left_index=True, right_index=True)
table = table.reorder_levels(order=[1,0],axis=1)
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-import-cntry_shares(in_manuf)-pc+fap+np-all.csv") 
table = table.ix[cntrys]
table.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3l5-import-cntry_shares(in_manuf)-pc+fap+np-selection.csv") 



#------------#
#-TRADE DATA-#
#------------#

#------------------#
#-Intra-Asia Trade-#
#------------------#

store = pd.HDFStore(RESULTS_DIR+"baci92-sitcr3-dataset/baci92-sitcr3-trade-1995-2013-yearly.h5") 	#-Already has Regions Coded-#
for year in xrange(1995,2013+1,2):
	print "Processing year %s ..."%year
	df = store.get("Y%s"%year)[["year", "eiso3c", "iiso3c", "productcode", "value", "eregionname","iregionname"]] 	#-Need to Aggregate-#
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
easia = data.loc[(data.eregionname == "Eastern Asia")&(data.iregionname == "Eastern Asia")].reset_index(drop=True)
seasia = data.loc[(data.eregionname == "South-Eastern Asia")&(data.iregionname == "South-Eastern Asia")].reset_index(drop=True) 
asia = easia.append(seasia).reset_index(drop=True)
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
plt.savefig(RESULTS_DIR+"plots/"+"baci92-total_pc_in_manuf_exports-Intra-SAsia+SEAsia.png", dpi=400)
plt.cla()
#-Total P&C Percent of Total Manufacturing Exports-#
plotdata["%"] = plotdata["Parts and Components"] / (plotdata["Parts and Components"] + plotdata["Final Products"])
fig = plotdata["%"].plot(title="PC % share in Manufacturing Exports (Intra East and South-East Asia)", ylim=(0,0.5))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_in_manuf_exports-Intra-SAsia+SEAsia.png", dpi=400)

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
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_in_manuf_exports-Intra-SAsia+SEAsia-CHN+JPN+TWN+KOR.png", dpi=400)
#-Table-#
table = plotdata["%"].unstack(level="country").T
SELECTION = ["CHN","JPN","IDN","IND","THA","MYS","PHL","VNM"]
table.ix[SELECTION].to_excel(RESULTS_DIR+"tables/baci92-percent_pc_in_manuf_exports-Intra-SAsia+SEAsia-SelectionCntry.xlsx")

#-Thesis Plot-#
plotdata = plotdata["%"].unstack(level="country")
fig = plotdata.CHN.plot(style='r+-')
fig = plotdata.JPN.plot(style='b.-')
fig = plotdata.IDN.plot(style='go-')
fig = plotdata.KOR.plot(style='c*-')
fig = plotdata.MYS.plot(style='mp-')
fig = plotdata.THA.plot(style='ks-')
fig = plotdata.TWN.plot(style='rx-')
fig.legend(loc='center left', bbox_to_anchor=(1, 0.5))
fig.set_ylabel("Percent PC in Manufacturing Exports")
fig.set_ylim(bottom=0)
fig.set_xlabel("Year")
fig.xaxis.axes.set_xticks(xrange(1995,2013+1,2))
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-percent_pc_in_manuf_exports-Intra-SAsia+SEAsia-SelectCntry.png", dpi=400, bbox_inches='tight')


#------------------------#
#-Product Space Analysis-#
#------------------------#

#------------------#
#-Proximity Values-#
#------------------#

#-Work with BACI EXPORT Data-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-export-%s-dataset.dta"%(year))
	df = df.rename(columns={'eiso3c' : 'country', 'sitcr3' : 'productcode', 'value':'export'})
	df = df.set_index(["year"])
	if year == 1995:
		data = df
	else:
		data = data.append(df)
s = DynamicProductLevelExportSystem()
s.from_df(data)
s.rca_matrices(complete_data=True)
s.mcp_matrices()
s.proximity_matrices()

pccodes = set(data.loc[data.pc == 1].productcode.unique())
fapcodes = set(data.loc[data.fap == 1].productcode.unique())
manufcodes = set(data.loc[data.sitccat == "M"].productcode.unique())
othermanufcodes = manufcodes.difference(fapcodes).difference(pccodes)
othercodes = set(data.productcode.unique()).difference(pccodes).difference(fapcodes).difference(manufcodes)

#-Compute Within and Between Proximity Values in Categories: PC, FAP, Other Products-#
results = {}
for year in sorted(data.index.unique()):
	print "Processing year: %s ..." % year
	prox = s[year].proximity
	prox = prox.stack().reset_index()
	prox = prox.rename(columns={0:'prox'})
	#-Categories-#
	def cathelper(x):
	    if x in pccodes:
	        return "pc"
	    elif x in fapcodes:
	        return "fap"
	    elif x in othermanufcodes:
	    	return "om"
	    else:
	        return "other"
	prox["cat1"] = prox["productcode1"].apply(lambda x: cathelper(x))
	prox["cat2"] = prox["productcode2"].apply(lambda x: cathelper(x))
	result = prox.groupby(["cat1","cat2"]).mean()
	results[year] = result
for year in sorted(results.keys()):
    result = results[year].rename(columns={'prox':year}).reset_index()
    if year == 1995:
        df = result
    else:
        df = df.merge(result, on=["cat1","cat2"])
results = df.set_index(["cat1","cat2"])

#-Results: Show that the Intra and Iter Proximity Relationships are stable from 1995 to 2013 (No Significant Variation)-#

#-Dissagregated by Product Types-#

pscodes = data[["productcode","pscommunity"]].reset_index()
del pscodes["year"]
pscodes = pscodes.drop_duplicates()
pscodes = pscodes.set_index(["productcode"])["pscommunity"].to_dict()

#-Compute Within and Between Proximity Values in Categories: PC, FAP, Other Products-#
results = {}
for year in sorted(data.index.unique()):
	print "Processing year: %s ..." % year
	prox = s[year].proximity
	prox = prox.stack().reset_index()
	prox = prox.rename(columns={0:'prox'})
	#-Categories-#
	def cathelper(x):
	    if x in pccodes:
	        return "pc"
	    elif x in fapcodes:
	        return "fap"
	    else:
	        return "other"
	prox["cat1"] = prox["productcode1"].apply(lambda x: cathelper(x))
	prox["cat2"] = prox["productcode2"].apply(lambda x: cathelper(x))
	prox["ps1"] = prox["productcode1"].apply(lambda x: pscodes[x])
	prox["ps2"] = prox["productcode2"].apply(lambda x: pscodes[x])
	results[year] = prox.groupby(["ps1","cat1","ps2","cat2"]).mean()
for year in sorted(results.keys()):
    result = results[year].rename(columns={'prox':year}).reset_index()
    if year == 1995:
        df = result
    else:
        df = df.merge(result, on=["ps1","cat1","ps2","cat2"])
results = df.set_index(["ps1","cat1","ps2","cat2"])

#-Results: Show that the Intra and Iter Proximity Relationships are still pretty stable from 1995 to 2013 (No Significant Variation)-#
#-Some weak evidence that parts and components in electrical is getting less average proximity to other products in that cluster in recent years-#
#-results.ix[("180","pc","180","other")].plot(ylim=0)

#-Strongest Connections-#
pccodes = set(data.loc[data.pc == 1].productcode.unique())
fapcodes = set(data.loc[data.fap == 1].productcode.unique())
manufcodes = set(data.loc[data.sitccat == "M"].productcode.unique())
othermanufcodes = manufcodes.difference(fapcodes).difference(pccodes)
othercodes = set(data.productcode.unique()).difference(pccodes).difference(fapcodes).difference(manufcodes)

year = 2000
prox = s[year].proximity
prox = prox.stack().reset_index()
prox = prox.rename(columns={0:'prox'})
#-Categories-#
def cathelper(x):
    if x in pccodes:
        return "pc"
    elif x in fapcodes:
        return "fap"
    elif x in othermanufcodes:
    	return "om"
    else:
        return "other"
prox["cat1"] = prox["productcode1"].apply(lambda x: cathelper(x))
prox["cat2"] = prox["productcode2"].apply(lambda x: cathelper(x))
pcprox = prox.loc[(prox.cat1 == "pc")&(prox.prox != 1)]
pcprox = pcprox.sort(columns=["prox"], ascending=False)
pcprox[0:1000].groupby(["cat2"]).count()

#-Top 10 for Each Part and Component-#
pcprox.groupby(["productcode1"]).apply(lambda x: x[0:10].groupby(["cat2"]).count()).groupby(level=["cat2"]).mean()
#-Top1-#
pcprox.groupby(["productcode1"]).apply(lambda x: x[0:1].groupby(["cat2"]).count()).groupby(level=["cat2"]).sum()


#----------#
#-Networks-#
#----------#

#-Export Data-#
#-Work with BACI EXPORT Data-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-export-%s-dataset.dta"%(year))
	df = df.rename(columns={'eiso3c' : 'country', 'sitcr3' : 'productcode', 'value':'export'})
	df = df.set_index(["year"])
	if year == 1995:
		data = df
	else:
		data = data.append(df)
s = DynamicProductLevelExportSystem()
s.from_df(data)

#-2000 Cross Section-#
xs = s[2000]
xs.rca_matrix(complete_data=True)
xs.mcp_matrix()
xs.proximity_matrix()
xs.compute_pci()
xs.auto_adjust_pci_sign(product_datum=('33400','-ve')) 	#-Oil is negative, un-complex-#

prox = xs.proximity.copy()
pci = xs.pci.to_dict()

#-Parts and Components Product Code Sets-#
allcodes = set(prox.index)
pccodes = set(pc.index)
othercodes = allcodes.difference(pccodes)
manuffinal = set(xs.data.loc[xs.data.sitccat == "M"].reset_index().productcode.unique())

#-Tables of Parts and Components Relatedness (Proximity)-#
print prox.unstack().describe()
pc_prox = prox.filter(items=pccodes, axis=0).filter(items=pccodes, axis=1)
print pc_prox.unstack().describe()
other_prox = prox.filter(items=othercodes, axis=0).filter(items=othercodes, axis=1)
print other_prox.unstack().describe()
mf_prox = prox.filter(items=manuffinal, axis=0).filter(items=manuffinal, axis=1)
print mf_prox.unstack().describe()

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
from reference.ProductSpace import plot_proximity, plot_proximity_simple

pprox = prox.copy()
pprox.columns = pprox.columns.droplevel(level=[0,1])
pprox.index = pprox.index.droplevel(level=[0,1])
fig = plot_proximity(pprox, step=10, prox_cutoff=0.6,  sortby="presorted", sortby_text="L1: FP and PC, L2: PCI")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-proximity-sortby(pc-pci)-yr2000-thesis.png", dpi=400)
fig = plot_proximity(pprox, step=10, prox_cutoff=0.6,  sortby="presorted", sortby_text="L1: FP and PC, L2: PCI")
ax = fig.gca()
ax.set_title("")
ax.set_xticks([])
ax.set_yticks([])
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-proximity-sortby(pc-pci)-yr2000-naked-thesis.png", dpi=400)
plt.clf()
#-Plot of Parts and Components-#
pprox = prox.ix[1][1].copy()
pprox.columns = pprox.columns.droplevel(level=[0])
pprox.index = pprox.index.droplevel(level=[0])
fig = plot_proximity(pprox, step=10, prox_cutoff=0.6,  sortby="presorted", sortby_text="PCI")
ax = fig.gca()
ax.set_title("Parts and Components")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-proximity(pc)-sortby(pci)-yr2000-thesis.png", dpi=400)
plt.clf()
#-Plot All Other Products-#
pprox = prox.ix[0][0].copy()
pprox.columns = pprox.columns.droplevel(level=[0])
pprox.index = pprox.index.droplevel(level=[0])
fig = plot_proximity(pprox, step=10, prox_cutoff=0.6,  sortby="presorted", sortby_text="PCI")
plt.clf()
ax = fig.gca()
ax.set_title("Final Manufactures")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-proximity(not-pc)-sortby(pci)-yr2000-thesis.png", dpi=400)
plt.clf()
#-Plot P&C Versus Other Products-#
pprox = prox.ix[1][0].copy()
pprox.columns = pprox.columns.droplevel(level=[0])
pprox.index = pprox.index.droplevel(level=[0])
print pprox.unstack().describe()
fig = plot_proximity_simple(pprox)
ax = fig.gca()
ax.set_title("Parts and Components Vs. Final Manufactures")
ax.set_xticks([])
ax.set_yticks([])
ax.set_xlabel("Final Manufactures [Sorted by PCI]")
ax.set_ylabel("PC [Sorted by PCI]")
plt.tight_layout()
plt.savefig(RESULTS_DIR+"plots/"+"baci92-proximity(pc-vs-not-pc)-sortby(pci)-yr2000-thesis.png", dpi=400)
plt.clf()

#-Minimum Spanning Tree for GEPHI-#
prox = xs.proximity
import networkx as nx
g = nx.from_numpy_matrix(prox.values)
g = nx.relabel_nodes(g, dict(enumerate(prox.columns)))
mst = nx.minimum_spanning_tree(g)
print len(mst.nodes())
print len(mst.edges())
#-Node Attributes-#
psnodes = pd.read_stata(RESULTS_DIR+"productspace-sitcr2-node-data.dta")[["sitcr2l4","xcord","ycord","nodesize","nodecolor","leamercolor"]]
lallnodes = pd.read_stata(RESULTS_DIR+"lall-techclassification.dta")[["lall","lallcolor"]].drop_duplicates().reset_index(drop=True)
data = xs.data.reset_index()[["productcode","pc","sitccat","lall","leamer","pscommunity","sitcr2l5"]].copy(deep=True)
data = data.drop_duplicates().reset_index(drop=True)
data["sitcr2l4"] = data["sitcr2l5"].apply(lambda x: x[0:4])
data = data.merge(psnodes, on=["sitcr2l4"], how="left")
data["nodesizescaled"] = data["nodesize"]/3
data = data.merge(lallnodes, on=["lall"], how="left")
data = data.set_index("lall")
data = data.set_value("SP",col=["lallcolor"], value="#909090")
data = data.reset_index()
concord = data.set_index('productcode')
#-Add Node Attributes to Network-#
for nd in mst.nodes():
	for attr in ["pc","sitccat","lall","leamer","pscommunity","nodesize","xcord","ycord","nodecolor","nodesizescaled","lallcolor","leamercolor"]:
		#-String Attributes-#
		if attr in ["pc","nodesize","xcord","ycord", "nodesizescaled"]:
			val = str(concord.ix[nd][attr])
			if val == "nan":
				val = "0"
			mst.node[nd][attr] = val
		else:
			mst.node[nd][attr] = concord.ix[nd][attr]
#-Fix Special Cases-#
mst.node["33400"]["xcord"] = "4000"
mst.node["33400"]["ycord"] = "6000"
mst.node["33400"]["nodecolor"] = "#7a7474"
mst.node["33400"]["nodesize"] = "200"
mst.node["33400"]["nodesizescaled"] = str(200/3)
mst.node["33400"]["leamercolor"] = "#BA8D04"
mst.node["75270"]["xcord"] = "-900"
mst.node["75270"]["ycord"] = "3300"
mst.node["75270"]["nodecolor"] = "#ff6bb0"
mst.node["75270"]["nodesize"] = "120"
mst.node["75270"]["nodesizescaled"] = str(120/3)
mst.node["75270"]["leamercolor"] = "#FF00D7"
#-Export to GEXF File-#
nx.write_gexf(mst, RESULTS_DIR+'prox-mst.gexf')
#-MST and Edges Above 0.55-#
for n,nb,d in g.edges_iter(data=True):
    d = d['weight']
    if d == 1:
        continue
    if d >= 0.55:
        mst.add_edge(n,nb,attr_dict={'weight':d})
print len(mst.nodes())
print len(mst.edges())
#-Node Attributes-#
psnodes = pd.read_stata(RESULTS_DIR+"productspace-sitcr2-node-data.dta")[["sitcr2l4","xcord","ycord","nodesize","nodecolor","leamercolor"]]
lallnodes = pd.read_stata(RESULTS_DIR+"lall-techclassification.dta")[["lall","lallcolor"]].drop_duplicates().reset_index(drop=True)
data = xs.data.reset_index()[["productcode","pc","sitccat","lall","leamer","pscommunity","sitcr2l5"]].copy(deep=True)
data = data.drop_duplicates().reset_index(drop=True)
data["sitcr2l4"] = data["sitcr2l5"].apply(lambda x: x[0:4])
data = data.merge(psnodes, on=["sitcr2l4"], how="left")
data["nodesizescaled"] = data["nodesize"]/3
data = data.merge(lallnodes, on=["lall"], how="left")
data = data.set_index("lall")
data = data.set_value("SP",col=["lallcolor"], value="#909090")
data = data.reset_index()
concord = data.set_index('productcode')
#-Add Node Attributes to Network-#
for nd in mst.nodes():
	for attr in ["pc","sitccat","lall","leamer","pscommunity","nodesize","xcord","ycord","nodecolor","nodesizescaled","lallcolor","leamercolor"]:
		#-String Attributes-#
		if attr in ["pc","nodesize","xcord","ycord", "nodesizescaled"]:
			val = str(concord.ix[nd][attr])
			if val == "nan":
				val = "0"
			mst.node[nd][attr] = val
		else:
			mst.node[nd][attr] = concord.ix[nd][attr]
#-Fix Special Cases-#
mst.node["33400"]["xcord"] = "4000"
mst.node["33400"]["ycord"] = "6000"
mst.node["33400"]["nodecolor"] = "#7a7474"
mst.node["33400"]["nodesize"] = "200"
mst.node["33400"]["nodesizescaled"] = str(200/3)
mst.node["33400"]["leamercolor"] = "#BA8D04"
mst.node["75270"]["xcord"] = "-900"
mst.node["75270"]["ycord"] = "3300"
mst.node["75270"]["nodecolor"] = "#ff6bb0"
mst.node["75270"]["nodesize"] = "120"
mst.node["75270"]["nodesizescaled"] = str(120/3)
mst.node["75270"]["leamercolor"] = "#FF00D7"
#-Export to GEXF File-#
nx.write_gexf(mst, RESULTS_DIR+'prox-mst-with-prox(0-55).gexf')

#-Network of Parts and Component Products Only-#
pc_prox = prox.filter(items=pccodes, axis=0).filter(items=pccodes, axis=1)
g = nx.from_numpy_matrix(pc_prox.values)
g = nx.relabel_nodes(g, dict(enumerate(pc_prox.columns)))
mst = nx.minimum_spanning_tree(g)
print mst.number_of_nodes()
print mst.number_of_edges()
#-Add Edges Above 0.55-#
for n,nb,d in g.edges_iter(data=True):
    d = d['weight']
    if d == 1:
        continue
    if d >= 0.55:
        mst.add_edge(n,nb,attr_dict={'weight':d})
print mst.number_of_nodes()
print mst.number_of_edges()
#-Node Attributes-#
psnodes = pd.read_stata(RESULTS_DIR+"productspace-sitcr2-node-data.dta")[["sitcr2l4","xcord","ycord","nodesize","nodecolor","leamercolor"]]
lallnodes = pd.read_stata(RESULTS_DIR+"lall-techclassification.dta")[["lall","lallcolor"]].drop_duplicates().reset_index(drop=True)
data = xs.data.reset_index()[["productcode","pc","sitccat","lall","leamer","pscommunity","sitcr2l5"]].copy(deep=True)
data = data.drop_duplicates().reset_index(drop=True)
data["sitcr2l4"] = data["sitcr2l5"].apply(lambda x: x[0:4])
data = data.merge(psnodes, on=["sitcr2l4"], how="left")
data["nodesizescaled"] = data["nodesize"]/3
data = data.merge(lallnodes, on=["lall"], how="left")
data = data.set_index("lall")
data = data.set_value("SP",col=["lallcolor"], value="#909090")
data = data.reset_index()
concord = data.set_index('productcode')
#-Add Node Attributes to Network-#
for nd in mst.nodes():
	for attr in ["pc","sitccat","lall","leamer","pscommunity","nodesize","xcord","ycord","nodecolor","nodesizescaled","lallcolor","leamercolor"]:
		#-String Attributes-#
		if attr in ["pc","nodesize","xcord","ycord", "nodesizescaled"]:
			val = str(concord.ix[nd][attr])
			if val == "nan":
				val = "0"
			mst.node[nd][attr] = val
		else:
			mst.node[nd][attr] = concord.ix[nd][attr]
#-Fix Special Cases-#
mst.node["75270"]["xcord"] = "-900"
mst.node["75270"]["ycord"] = "3300"
mst.node["75270"]["nodecolor"] = "#ff6bb0"
mst.node["75270"]["nodesize"] = "120"
mst.node["75270"]["nodesizescaled"] = str(120/3)
mst.node["75270"]["leamercolor"] = "#FF00D7"
#-Export to GEXF File-#
nx.write_gexf(mst, RESULTS_DIR+'prox-mst-with-prox(0-55)-partsandcomponents.gexf')

#-Network of Final Products Only-#
other_prox = prox.filter(items=othercodes, axis=0).filter(items=othercodes, axis=1)
g = nx.from_numpy_matrix(other_prox.values)
g = nx.relabel_nodes(g, dict(enumerate(other_prox.columns)))
mst = nx.minimum_spanning_tree(g)
print mst.number_of_nodes()
print mst.number_of_edges()
#-Add Edges Above 0.55-#
for n,nb,d in g.edges_iter(data=True):
    d = d['weight']
    if d == 1:
        continue
    if d >= 0.55:
        mst.add_edge(n,nb,attr_dict={'weight':d})
print mst.number_of_nodes()
print mst.number_of_edges()
#-Node Attributes-#
psnodes = pd.read_stata(RESULTS_DIR+"productspace-sitcr2-node-data.dta")[["sitcr2l4","xcord","ycord","nodesize","nodecolor","leamercolor"]]
lallnodes = pd.read_stata(RESULTS_DIR+"lall-techclassification.dta")[["lall","lallcolor"]].drop_duplicates().reset_index(drop=True)
data = xs.data.reset_index()[["productcode","pc","sitccat","lall","leamer","pscommunity","sitcr2l5"]].copy(deep=True)
data = data.drop_duplicates().reset_index(drop=True)
data["sitcr2l4"] = data["sitcr2l5"].apply(lambda x: x[0:4])
data = data.merge(psnodes, on=["sitcr2l4"], how="left")
data["nodesizescaled"] = data["nodesize"]/3
data = data.merge(lallnodes, on=["lall"], how="left")
data = data.set_index("lall")
data = data.set_value("SP",col=["lallcolor"], value="#909090")
data = data.reset_index()
concord = data.set_index('productcode')
#-Add Node Attributes to Network-#
for nd in mst.nodes():
	for attr in ["pc","sitccat","lall","leamer","pscommunity","nodesize","xcord","ycord","nodecolor","nodesizescaled","lallcolor","leamercolor"]:
		#-String Attributes-#
		if attr in ["pc","nodesize","xcord","ycord", "nodesizescaled"]:
			val = str(concord.ix[nd][attr])
			if val == "nan":
				val = "0"
			mst.node[nd][attr] = val
		else:
			mst.node[nd][attr] = concord.ix[nd][attr]
#-Fix Special Cases-#
mst.node["33400"]["xcord"] = "4000"
mst.node["33400"]["ycord"] = "6000"
mst.node["33400"]["nodecolor"] = "#7a7474"
mst.node["33400"]["nodesize"] = "200"
mst.node["33400"]["nodesizescaled"] = str(200/3)
mst.node["33400"]["leamercolor"] = "#BA8D04"
#-Export to GEXF File-#
nx.write_gexf(mst, RESULTS_DIR+'prox-mst-with-prox(0-55)-not-partsandcomponents.gexf')

#-Network of Manufactured Final Products Only-#
mf_prox = prox.filter(items=manuffinal, axis=0).filter(items=manuffinal, axis=1)
g = nx.from_numpy_matrix(mf_prox.values)
g = nx.relabel_nodes(g, dict(enumerate(mf_prox.columns)))
mst = nx.minimum_spanning_tree(g)
print mst.number_of_nodes()
print mst.number_of_edges()
#-Add Edges Above 0.55-#
for n,nb,d in g.edges_iter(data=True):
    d = d['weight']
    if d == 1:
        continue
    if d >= 0.55:
        mst.add_edge(n,nb,attr_dict={'weight':d})
print mst.number_of_nodes()
print mst.number_of_edges()
#-Node Attributes-#
psnodes = pd.read_stata(RESULTS_DIR+"productspace-sitcr2-node-data.dta")[["sitcr2l4","xcord","ycord","nodesize","nodecolor","leamercolor"]]
lallnodes = pd.read_stata(RESULTS_DIR+"lall-techclassification.dta")[["lall","lallcolor"]].drop_duplicates().reset_index(drop=True)
data = xs.data.reset_index()[["productcode","pc","sitccat","lall","leamer","pscommunity","sitcr2l5"]].copy(deep=True)
data = data.drop_duplicates().reset_index(drop=True)
data["sitcr2l4"] = data["sitcr2l5"].apply(lambda x: x[0:4])
data = data.merge(psnodes, on=["sitcr2l4"], how="left")
data["nodesizescaled"] = data["nodesize"]/3
data = data.merge(lallnodes, on=["lall"], how="left")
data = data.set_index("lall")
data = data.set_value("SP",col=["lallcolor"], value="#909090")
data = data.reset_index()
concord = data.set_index('productcode')
#-Add Node Attributes to Network-#
for nd in mst.nodes():
	for attr in ["pc","sitccat","lall","leamer","pscommunity","nodesize","xcord","ycord","nodecolor","nodesizescaled","lallcolor","leamercolor"]:
		#-String Attributes-#
		if attr in ["pc","nodesize","xcord","ycord", "nodesizescaled"]:
			val = str(concord.ix[nd][attr])
			if val == "nan":
				val = "0"
			mst.node[nd][attr] = val
		else:
			mst.node[nd][attr] = concord.ix[nd][attr]
#-Export to GEXF File-#
nx.write_gexf(mst, RESULTS_DIR+'prox-mst-with-prox(0-55)-manufacturing-final.gexf')


#-------------------------------------------------#
#-Normalised Shares of P&C and Final Manufactures-#
#-------------------------------------------------#

#-A measure of degree of Populating the Category Space that is Normalised-#

#-Work with BACI EXPORT Data-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-export-%s-dataset.dta"%(year))
	if year == 1995:
		data = df
	else:
		data = data.append(df)
data.reset_index(inplace=True, drop=True)
data["export"] = data["export"]*1000 			#Scale Exports by 1000

data = data.set_index(["year","country","productcode"])
data["cntryexport"] = data["export"].groupby(level=["year","country"]).transform(np.sum)
data["prodexport"] = data["export"].groupby(level=["year","productcode"]).transform(np.sum)
data["totexport"] = data["export"].groupby(level=["year"]).transform(np.sum)
data["RCA"] = (data["export"]/data["cntryexport"])/(data["prodexport"]/data["totexport"])
data["MCP"] = data["RCA"].apply(lambda x: 1 if x >= 1 else 0)
#-Locate Manufactured Trade-#
manuf = data.loc[data.sitccat == 'M']
num_fmp = manuf.loc[manuf.pc == 0].reset_index()[["productcode","pc"]].drop_duplicates().shape[0]
num_pcmp = manuf.loc[manuf.pc == 1].reset_index()[["productcode","pc"]].drop_duplicates().shape[0]

#-Normalise P&C and Final Products-#
ndata = manuf.reset_index().groupby(["country","year","pc"])["MCP"].sum().unstack(level="pc")
ndata.columns = ["FP","PC"]
ndata["NFP"] = ndata["FP"]/num_fmp
ndata["NPC"] = ndata["PC"]/num_pcmp
ndata = ndata[["NFP","NPC"]] * 100 		#-Percentage-#


#-Value Shares-#


#------------------------------------------------------------------------------#
#-Regression Dataset: GDPPC Vs Normalised Shares of P&C and Final Manufactures-#
#------------------------------------------------------------------------------#

#-TBD-#


#---------------------------------------------------------#
#-Adjustments to ECI and PCI due to Product Fragmentation-#
#---------------------------------------------------------#

#-Work with BACI EXPORT Data-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-export-%s-dataset.dta"%(year))
	if year == 1995:
		data = df
	else:
		data = data.append(df)
data.reset_index(inplace=True, drop=True)
data["export"] = data["export"]*1000 			#Scale Exports by 1000

#-pc contains Product Codes from Preamble-#
fap = data.loc[data.fap == 1].productcode.unique() #Final Assembly Products-#
export = data[["year","country","productcode","export"]].set_index(["year"])

#
#-Adjust Final Assembly Products-#
#

from pyeconlab import DynamicProductLevelExportSystem
s = DynamicProductLevelExportSystem()
s.from_df(export)
xs = s[2000]
xs.rca_matrix(complete_data=True)
xs.mcp_matrix()
xs.compute_eci()
xs.auto_adjust_eci_sign()
xs.compute_pci()
xs.auto_adjust_pci_sign(product_datum=('33400','-ve')) 	#-Oil is negative, un-complex-#)
eci = xs.eci.copy()
pci = xs.pci.copy()

#-Alter Mcp Information on Final Assembly Products based on GDPPC-#
gdppc = wdi.series(wdi.codes.GDPPCPPP)["2000"].to_frame().reset_index()[["iso3c","2000"]].set_index("iso3c")
mcp = xs.mcp.reset_index()
def matcher(x):
	try:
		return gdppc.get_value(index=x, col="2000")
	except:
		if x == "TWN":
			return 17400
		else:
			return np.nan
mcp["gdppc"] = mcp["country"].apply(lambda x: matcher(x))
mcp = mcp.dropna() ##Drop Countries without GDPPC PPP Information-#

#-ECI with Reduced Countries-#
rmcp = mcp.copy()
del rmcp["gdppc"]
xs.mcp = rmcp.set_index(["country"]) #-Transplant Mcp-#
xs.countries = list(xs.mcp.index)
xs.compute_mcc()
xs.compute_mpp()
xs.compute_eci()
xs.auto_adjust_eci_sign()
xs.compute_pci()
xs.auto_adjust_pci_sign(product_datum=('33400','-ve')) 	#-Oil is negative, un-complex-#)
reci = xs.eci.copy()
rpci = xs.pci.copy()

#-Split FAP in Least Developed Economies-#
for product in fap:
	mcp[product+"L"] = mcp[[product,"gdppc"]].apply(lambda row: 1 if (row[product]==1)&(row["gdppc"]<=12736) else 0, axis=1)
	mcp[product+"H"] = mcp[[product,"gdppc"]].apply(lambda row: 1 if (row[product]==1)&(row["gdppc"]>12736) else 0, axis=1)
	del mcp[product]

#-ECI with Reduced Countries and Split Final Assembly Products-#
rsmcp = mcp.copy()
del rsmcp["gdppc"]
xs.mcp = rsmcp.set_index(["country"]) #-Transplant Mcp-#
xs.countries = list(xs.mcp.index)
xs.products = list(xs.mcp.columns)
xs.compute_mcc()
xs.compute_mpp()
xs.compute_eci()
xs.auto_adjust_eci_sign()
xs.compute_pci()
xs.auto_adjust_pci_sign(product_datum=('33400','-ve')) 	#-Oil is negative, un-complex-#)
rseci = xs.eci.copy()
rspci = xs.pci.copy()

#-Compare ECI Ranks-#
meci = pd.DataFrame([eci,reci,rseci]).T
meci.columns = ["ECI","ECI-GDPPC","ECI-GDPPC-FAP"]
meci_rank = meci.dropna().rank(ascending=False).sort(["ECI"])
meci_rank["change"] = meci_rank["ECI"] - meci_rank["ECI-GDPPC-FAP"]
meci_rank = meci_rank.sort(["change"])
meci_rank.to_excel(RESULTS_DIR+"eci-ranks-fap-adjusted-yr2000.xlsx")
table = meci_rank.ix[["PHL","MYS","MEX","CHN","IDN","IND","THA","KOR","JPN","USA","TWN","HKG"]]
table.to_excel(RESULTS_DIR+"eci-ranks-fap-adjusted-yr2000-selectedcntry.xlsx")
#-Thesis Table-#
meci_rank = meci.dropna().rank(ascending=False).sort(["ECI"])
meci_rank["Change"] = meci_rank["ECI-GDPPC"] - meci_rank["ECI-GDPPC-FAP"]
meci_rank = meci_rank.sort(["Change"])
table = meci_rank[["ECI-GDPPC","ECI-GDPPC-FAP","Change"]].ix[["MYS","PHL","MEX","SVK","CHN","IND","HUN","THA","CZE","IDN","VNM","UKR","JPN","USA","DEU","KOR","TWN","HKG"]]
table.to_excel(RESULTS_DIR+"eci-ranks-fap-adjusted-yr2000-selectedcntry-thesis.xlsx")

#-Compare PCI Ranks-#
mpci = pd.DataFrame([pci,rpci,rspci]).T
mpci.columns = ["PCI","PCI-GDPPC","PCI-GDPPC-FAP"]
mpci["PCI"] = mpci["PCI"].ffill()
mpci["PCI-GDPPC"] = mpci["PCI-GDPPC"].ffill()
mpci = mpci.dropna()
mpci_rank = mpci.rank(ascending=False).sort(["PCI"])
mpci_rank["change"] = mpci_rank["PCI"] - mpci_rank["PCI-GDPPC-FAP"]
mpci_rank = mpci_rank.sort(["change"])
mpci_rank.to_excel(RESULTS_DIR+"pci-ranks-fap-adjusted-yr2000.xlsx")
#-Thesis Table-#
mpci_rank = mpci.rank(ascending=False).sort(["PCI-GDPPC"])
mpci_rank["Change"] = mpci_rank["PCI-GDPPC"] - mpci_rank["PCI-GDPPC-FAP"]
mpci_rank = mpci_rank[["PCI-GDPPC","PCI-GDPPC-FAP","Change"]].sort(["Change"])
mpci_rank = mpci.rank(ascending=False)
fapcodes = []
for productcode in fap:
    fapcodes.append(productcode)
    fapcodes.append(productcode+"L")
    fapcodes.append(productcode+"H")
table = mpci_rank.filter(items=fapcodes, axis=0)[["PCI-GDPPC","PCI-GDPPC-FAP"]]
table["Change"] = table["PCI-GDPPC"] - table["PCI-GDPPC-FAP"]
table = table.reset_index()
table["productcode"] = table["index"].apply(lambda x: x[0:5])
table["type"] = table["index"].apply(lambda x: x[5:])
del table["index"]
table = table.set_index(["productcode","type"])
table.to_excel(RESULTS_DIR+"pci-ranks-fap-adjusted-yr2000-faponlylist.xlsx")
table = table.ix[["75210","75220","76419","87413"]]
table.to_excel(RESULTS_DIR+"pci-ranks-fap-adjusted-yr2000-selection-thesis.xlsx")

#
#-Adjust Parts and Components and Final Assembly Products-#
#

from pyeconlab import DynamicProductLevelExportSystem
s = DynamicProductLevelExportSystem()
s.from_df(export)
xs = s[2000]
xs.rca_matrix(complete_data=True)
xs.mcp_matrix()
xs.compute_eci()
xs.auto_adjust_eci_sign()
xs.compute_pci()
xs.auto_adjust_pci_sign(product_datum=('33400','-ve')) 	#-Oil is negative, un-complex-#)
eci = xs.eci.copy()
pci = xs.pci.copy()
productset = set(xs.products)

#-Alter Mcp Information on Parts and Components & Final Assembly Products based on GDPPC-#
gdppc = wdi.series(wdi.codes.GDPPCPPP)["2000"].to_frame().reset_index()[["iso3c","2000"]].set_index("iso3c")
mcp = xs.mcp.reset_index()
def matcher(x):
	try:
		return gdppc.get_value(index=x, col="2000")
	except:
		if x == "TWN":
			return 17400
		else:
			return np.nan
mcp["gdppc"] = mcp["country"].apply(lambda x: matcher(x))
mcp = mcp.dropna() ##Drop Countries without GDPPC PPP Information-#

#-ECI with Reduced Countries-#
rmcp = mcp.copy()
del rmcp["gdppc"]
xs.mcp = rmcp.set_index(["country"]) #-Transplant Mcp-#
xs.countries = list(xs.mcp.index)
xs.compute_mcc()
xs.compute_mpp()
xs.compute_eci()
xs.auto_adjust_eci_sign()
xs.compute_pci()
xs.auto_adjust_pci_sign(product_datum=('33400','-ve')) 	#-Oil is negative, un-complex-#)
reci = xs.eci.copy()
rpci = xs.pci.copy()

#-Split FAP in Least Developed Economies-#
splitcodes = list(set(fap).union(set(pc.index)))

for product in splitcodes:
	if product not in productset:
		continue
	mcp[product+"L"] = mcp[[product,"gdppc"]].apply(lambda row: 1 if (row[product]==1)&(row["gdppc"]<=12736) else 0, axis=1)
	mcp[product+"H"] = mcp[[product,"gdppc"]].apply(lambda row: 1 if (row[product]==1)&(row["gdppc"]>12736) else 0, axis=1)
	del mcp[product]

#-ECI with Reduced Countries and Split Final Assembly Products-#
rsmcp = mcp.copy()
del rsmcp["gdppc"]
xs.mcp = rsmcp.set_index(["country"]) #-Transplant Mcp-#
xs.countries = list(xs.mcp.index)
xs.products = list(xs.mcp.columns)
xs.compute_mcc()
xs.compute_mpp()
xs.compute_eci()
xs.auto_adjust_eci_sign()
xs.compute_pci()
xs.auto_adjust_pci_sign(product_datum=('33400','-ve')) 	#-Oil is negative, un-complex-#)
rseci = xs.eci.copy()
rspci = xs.pci.copy()

#-Compare ECI Ranks-#
meci = pd.DataFrame([eci,reci,rseci]).T
meci.columns = ["ECI","ECI-GDPPC","ECI-GDPPC-PCFAP"]
meci_rank = meci.dropna().rank(ascending=False).sort(["ECI"])
meci_rank["change"] = meci_rank["ECI"] - meci_rank["ECI-GDPPC-PCFAP"]
meci_rank = meci_rank.sort(["change"])
meci_rank.to_excel(RESULTS_DIR+"eci-ranks-pc+fap-adjusted-yr2000.xlsx")
table = meci_rank.ix[["MEX","PHL","MYS","THA","CHN","IDN","JPN","USA","IND","KOR","TWN","HKG"]]
table.to_excel(RESULTS_DIR+"eci-ranks-pc+fap-adjusted-yr2000-selectedcntry.xlsx")

#-Compare PCI Ranks-#
mpci = pd.DataFrame([pci,rpci,rspci]).T
mpci.columns = ["PCI","PCI-GDPPC","PCI-GDPPC-PCFAP"]
mpci["PCI"] = mpci["PCI"].ffill()
mpci["PCI-GDPPC"] = mpci["PCI-GDPPC"].ffill()
mpci = mpci.dropna()
mpci_rank = mpci.rank(ascending=False).sort(["PCI"])
mpci_rank["change"] = mpci_rank["PCI"] - mpci_rank["PCI-GDPPC-PCFAP"]
mpci_rank = mpci_rank.sort(["change"])
mpci_rank.to_excel(RESULTS_DIR+"pci-ranks-pc+fap-adjusted-yr2000.xlsx")


#-----------------------------------#
#-Emergence of Parts and Components-#
#-----------------------------------#

#-End Point Transition Matrices-#
#-!-Experimental-!-#

#-Work with BACI EXPORT Data-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-export-%s-dataset.dta"%(year))
	if year == 1995:
		data = df
	else:
		data = data.append(df)
data.reset_index(inplace=True, drop=True)
data["export"] = data["export"]*1000 			#Scale Exports by 1000

#-pc contains Product Codes from Preamble-#
fapcodes = data.loc[data.fap == 1].productcode.unique() #Final Assembly Products-#
pccodes = data.loc[data.pc == 1].productcode.unique() #Parts and Components-#

pscodes = data[["productcode","pscommunity"]]
pscodes = pscodes.drop_duplicates()
pscodes = pscodes.set_index(["productcode"])["pscommunity"].to_dict()


from pyeconlab import DynamicProductLevelExportSystem
s = DynamicProductLevelExportSystem()
export = data[["year","country","productcode","export"]].set_index(["year"])
s.from_df(export)
s.rca_matrices(complete_data=True)
s.mcp_matrices(cutoff = 2.0)

#-Compute Transitions-#
for year in xrange(1995,1997+1,1):
	if year == 1995:
		mcp = s[year].mcp.copy()
	else:
		mcp = mcp + s[year].mcp
start = mcp.applymap(lambda x: 1 if x >= 1 else 0)
for year in xrange(2011,2013+1,1):
	if year == 2011:
		mcp = s[year].mcp.copy()
	else:
		mcp = mcp + s[year].mcp
finish = mcp.applymap(lambda x: 1 if x >= 1 else 0)
finish = finish.reindex_like(start)
#-Count Transitions-#
transitions = pd.DataFrame(index=start.columns.copy(), columns=start.columns.copy())
transitions.index.name = "1995"
transitions.columns.name = "2013"
for p1 in transitions.index:
	for p2 in transitions.columns:
		transitions.set_value(index=p1, col=p2, value=(start[p1]*finish[p2]).sum())
#-Normalised Transitions-#
den = transitions.sum(axis=1).replace({0:np.nan})
transition_matrix = transitions.div(den, axis=0)

#-GroupBy-#
mat = transition_matrix.stack().reset_index()
mat["cat1"] = mat["1995"].apply(lambda x: cathelper(x))
mat["ps1"] = mat["1995"].apply(lambda x: pscodes[x])
mat["cat2"] = mat["2013"].apply(lambda x: cathelper(x))
mat["ps2"] = mat["2013"].apply(lambda x: pscodes[x])
del mat["1995"]
del mat["2013"]
mat = mat.set_index(["ps1","cat1","ps2","cat2"])

#-Aggregations-#
pscodes = data[["productcode","pscommunity"]]
pscodes = pscodes.drop_duplicates()
pscodes = pscodes.set_index(["productcode"])["pscommunity"].to_dict()

#-Compute Transitions-#
for year in xrange(1995,1997+1,1):
	if year == 1995:
		mcp = s[year].mcp.copy()
	else:
		mcp = mcp + s[year].mcp
start = mcp.applymap(lambda x: 1 if x >= 1 else 0)
for year in xrange(2011,2013+1,1):
	if year == 2011:
		mcp = s[year].mcp.copy()
	else:
		mcp = mcp + s[year].mcp
finish = mcp.applymap(lambda x: 1 if x >= 1 else 0)
finish = finish.reindex_like(start)
#-Categories-#
start = start.T.reset_index()
finish = finish.T.reset_index()
def cathelper(x):
    if x in pccodes:
        return "pc"
    elif x in fapcodes:
        return "fap"
    else:
        return "fp"
start["cat"] = start["productcode"].apply(lambda x: cathelper(x))
start["ps"] = start["productcode"].apply(lambda x: pscodes[x])
finish["cat"] = finish["productcode"].apply(lambda x: cathelper(x))
finish["ps"] = start["productcode"].apply(lambda x: pscodes[x])
#-Collapse-#
start = start.groupby(["ps","cat"]).sum()
start = start.T.applymap(lambda x: 1 if x >= 1 else 0)
finish = finish.groupby(["ps","cat"]).sum()
finish = finish.T.applymap(lambda x: 1 if x >= 1 else 0)
#-Count Transitions-#
transitions = pd.DataFrame(index=start.columns.copy(), columns=start.columns.copy())
transitions.index.name = "1995"
transitions.columns.name = "2013"
for p1 in transitions.index:
	for p2 in transitions.columns:
		transitions.set_value(index=p1, col=p2, value=(start[p1]*finish[p2]).sum())
#-Normalised Transitions-#
den = transitions.sum(axis=1).replace({0:np.nan})
transition_matrix = transitions.div(den, axis=0)

#--------#

#-Use NetworkX Approach to get full sequences-#

from reference.ProductDiffusion import setup_country_product_emergence_graph, generate_country_product_emergence_graph, write_objects_to_gexf, build_product_emergence_network
from reference.ProductSpace import compute_product_changes, from_dict_to_dataframe, reindex_dynamic_dataframe
import networkx as nx

#-Work with BACI EXPORT Data-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-export-%s-dataset.dta"%(year))
	if year == 1995:
		data = df
	else:
		data = data.append(df)
data.reset_index(inplace=True, drop=True)
data["export"] = data["export"]*1000 			#Scale Exports by 1000

#-pc contains Product Codes from Preamble-#
fapcodes = data.loc[data.fap == 1].productcode.unique() #Final Assembly Products-#
pccodes = data.loc[data.pc == 1].productcode.unique() #Parts and Components-#
pscodes = data[["productcode","pscommunity"]]
pscodes = pscodes.drop_duplicates()
pscodes = pscodes.set_index(["productcode"])["pscommunity"].to_dict()

from pyeconlab import DynamicProductLevelExportSystem
s = DynamicProductLevelExportSystem()
export = data[["year","country","productcode","export"]].set_index(["year"])
s.from_df(export)
s.rca_matrices(complete_data=True)
s.mcp_matrices()

#-Build into a Panel DataFrame-#
for year in xrange(1995,2013+1,1):
    mcp = s[year].mcp.unstack().reset_index()
    mcp["year"] = year
    mcp = mcp.rename(columns={0:"mcp"})
    if year == 1995:
        df = mcp
    else:
        df = df.append(mcp)
mcp_df = df.set_index(["year","country","productcode"])
tradesystem, country_list, year_list, product_list  = setup_country_product_emergence_graph(mcp_df, country_filter=['KEN'], verbose=True)
tradesystem = generate_country_product_emergence_graph(mcp_df, tradesystem, country_filter=['KEN'], base_year=1996, end_year=2013, verbose=True, allow_repeats=False)
write_objects_to_gexf(RESULTS_DIR, tradesystem, country_list=['KEN'], prepend_filename='Mcp-All-', verbose=True)

#-NewProducts-#
Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts = s.compute_product_changes()
for item in sorted(Mcp_NewProducts.keys()):
	startyear,nextyear = item.split("-")
	mcp = Mcp_NewProducts[item].unstack().reset_index()
	mcp["year"] = int(nextyear)
	mcp = mcp.rename(columns={0:"mcp"})
	if nextyear == "1996":
		df = mcp
	else:
		df = df.append(mcp)
mcp_df = df.set_index(["year","country","productcode"])
mcp_df = mcp_df.fillna(0)
#-Trial: Kenya-#
tradesystem, country_list, year_list, product_list  = setup_country_product_emergence_graph(mcp_df, country_filter=['KEN'], verbose=True)
tradesystem = generate_country_product_emergence_graph(mcp_df, tradesystem, country_filter=['KEN'], base_year=1996, end_year=2013, verbose=True, allow_repeats=False)
write_objects_to_gexf(RESULTS_DIR, tradesystem, country_list=['KEN'], prepend_filename='Mcp-New-', verbose=True)
#-All Countries-#
tradesystem = generate_country_product_emergence_graph(mcp_df, tradesystem='', country_filter=[], base_year=1996, end_year=2013, verbose=False, allow_repeats=False)
product_network = build_product_emergence_network(tradesystem, verbose=False)
nx.write_gexf(product_network, RESULTS_DIR+"baci92-sitcr3l5-product_emergence-network.gexf")

#-Adjacency Matrix-#
adjacency = nx.to_dict_of_dicts(product_network)
adjacency = pd.DataFrame(adjacency)
def helper(x):
    if type(x) == dict:
        return x['key']
    else:
        return x
adjacency = adjacency.applymap(lambda x: helper(x))
#-Check Product Space and PC, FAP and Final Products-#
adjacency = adjacency.stack().reset_index()
adjacency = adjacency.rename(columns={'level_0':'start', 'level_1':'next',0:'transition'})
def cathelper(x):
    if x in pccodes:
        return "pc"
    elif x in fapcodes:
        return "fap"
    else:
        return "fp"
adjacency["cat1"] = adjacency["start"].apply(lambda x: cathelper(x))
adjacency["ps1"] = adjacency["start"].apply(lambda x: pscodes[x])
adjacency["cat2"] = adjacency["next"].apply(lambda x: cathelper(x))
adjacency["ps2"] = adjacency["next"].apply(lambda x: pscodes[x])
adjacency = adjacency.groupby(["ps1","cat1","ps2","cat2"]).sum()
adjacency = adjacency.unstack(level=["ps2","cat2"])
adjacency.to_csv(RESULTS_DIR+"baci92-sitcr3l5-transitions-matrix-pscomm-pc+fap+fp.csv")

#Remove Lower than 1 edges (no direct emergence across all countries data)
for u,v, edata in product_network.edges(data=True):
    if edata['key'] < 1:
        del product_network.edge[u][v]
nx.write_gexf(product_network, RESULTS_DIR+"baci92-sitcr3l5-product_emergence_network-reduced.gexf")

#---------------------------------------------#
#-Reduce the Dimensionality of the Mcp Matrix-#
#---------------------------------------------#

from reference.ProductDiffusion import generate_country_product_emergence_graph, write_objects_to_gexf, build_product_emergence_network
import networkx as nx

#-Work with BACI EXPORT Data-#
for year in xrange(1995,2013+1,1):
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-export-%s-dataset.dta"%(year))
	if year == 1995:
		data = df
	else:
		data = data.append(df)
data.reset_index(inplace=True, drop=True)
data["export"] = data["export"]*1000 			#Scale Exports by 1000

#-pc contains Product Codes from Preamble-#
fapcodes = data.loc[data.fap == 1].productcode.unique() #Final Assembly Products-#
pccodes = data.loc[data.pc == 1].productcode.unique() #Parts and Components-#
pscodes = data[["productcode","pscommunity"]]
pscodes = pscodes.drop_duplicates()
pscodes = pscodes.set_index(["productcode"])["pscommunity"].to_dict()

from pyeconlab import DynamicProductLevelExportSystem
s = DynamicProductLevelExportSystem()
export = data[["year","country","productcode","export"]].set_index(["year"])
s.from_df(export)
s.rca_matrices(complete_data=True)
s.mcp_matrices()

def cathelper(x):
    if x in pccodes:
        return "pc"
    elif x in fapcodes:
        return "fap"
    else:
        return "fp"
#-NewProducts-#
Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts = s.compute_product_changes()
for item in sorted(Mcp_NewProducts.keys()):
	startyear,nextyear = item.split("-")
	mcp = Mcp_NewProducts[item].unstack().reset_index()
	mcp["year"] = int(nextyear)
	mcp = mcp.rename(columns={0:"mcp"})
	mcp["cat"] = mcp["productcode"].apply(lambda x: cathelper(x))
	mcp["ps"] = mcp["productcode"].apply(lambda x: pscodes[x])
	mcp["productcodes"] = mcp["ps"]+"-"+mcp["cat"]
	mcp = mcp.groupby(["year","country","productcode"]).sum().reset_index()
	mcp["mcp"] = mcp["mcp"].apply(lambda x: 1 if x >= 1 else 0)
	if nextyear == "1996":
		df = mcp
	else:
		df = df.append(mcp)
mcp_df = df.set_index(["year","country","productcode"])
mcp_df = mcp_df.fillna(0)

#-All Countries-#
tradesystem = generate_country_product_emergence_graph(mcp_df, tradesystem='', country_filter=[], base_year=1996, end_year=2013, verbose=False, allow_repeats=False)
#-Write Kenya Example-#
nx.write_gexf(tradesystem["KEN"], RESULTS_DIR+"baci92-sitcr3l5-country-product_emergence-network-KEN-ps+pc+fap+other.gexf")
nx.write_gexf(tradesystem["THA"], RESULTS_DIR+"baci92-sitcr3l5-country-product_emergence-network-THA-ps+pc+fap+other.gexf")
#-Product Diffusion Network-#
product_network = build_product_emergence_network(tradesystem, verbose=False)
nx.write_gexf(product_network, RESULTS_DIR+"baci92-sitcr3l5-product_diffusion-network-ps+pc+fap+other.gexf")
#-Adjacency Matrix-#
adjacency = nx.to_dict_of_dicts(product_network)
adjacency = pd.DataFrame(adjacency)
def helper(x):
    if type(x) == dict:
        return x['key']
    else:
        return x
adjacency = adjacency.applymap(lambda x: helper(x))
adjacency.to_csv(RESULTS_DIR+"baci92-sitcr3l5-product_diffusion-adjacencymatrix-ps+pc+fap+other.csv")


#-------------------------------#
#-Appendix G: Tables and Graphs-#
#-------------------------------#

#-Parts and Components List-#
pc = pd.read_stata(SOURCE_DIR+"athukorala-pc-sitcr3l5.dta").set_index("sitcr3l5")
pc = pc.reset_index().to_excel(RESULTS_DIR+"tables/"+"appendix-partsandcomponentslist.xlsx")

#-List of Countries in Dataset-#
store = pd.HDFStore(RESULTS_DIR+"baci92-sitcr3-dataset/baci92-sitcr3-trade-1995-2013-yearly.h5") 	#-Already has Regions Coded-#
exporters = set()
importers = set()
totalproducts = set()
manufproducts = set()
partsandcomp = set()
tradeflows = 0
for year in xrange(1995,2013+1,1):
	print "Processing year %s ..."%year
	df = store.get("Y%s"%year)
	exporters = exporters.union(set(df.eiso3c.unique()))
	importers = importers.union(set(df.iiso3c.unique()))
	totalproducts = totalproducts.union(set(df.productcode.unique()))
	manufproducts = manufproducts.union(set(df.loc[df.sitccat == 'M'].productcode.unique()))
	partsandcomp = partsandcomp.union(set(df.loc[df.pc == 1].productcode.unique()))
	tradeflows += df.shape[0]+1
	del df
	gc.collect()		
store.close()
#-Pandas-#
cntrys = exporters.union(importers)
cntrys = pd.Series(sorted(list(cntrys)))
exporters = pd.Series(sorted(list(exporters)))
importers = pd.Series(sorted(list(importers)))
totalproducts = pd.Series(sorted(list(totalproducts)))
manufproducts = pd.Series(sorted(list(manufproducts)))
partsandcomp = pd.Series(sorted(list(partsandcomp)))
#-Data Stats Table-#
stats = {
	'Number of Exporters' : len(exporters),
	'Number of Importers' : len(importers),
	'Number of Total Products' : len(totalproducts),
	'Number of Manuf. Products': len(manufproducts),
	'Number of Parts and Comp.': len(partsandcomp),
	'Number of Trade Flows' : tradeflows
}
table1 = pd.Series(stats)
table1.name = "Trade"
#-Export Data-#
exporters = set()
totalproducts = set()
manufproducts = set()
partsandcomp = set()
tradeflows = 0
for year in xrange(1995,2013+1,1):
	print "Processing year %s ..."%year
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-export-%s-dataset.dta"%(year))
	exporters = exporters.union(set(df.country.unique()))
	totalproducts = totalproducts.union(set(df.productcode.unique()))
	manufproducts = manufproducts.union(set(df.loc[df.sitccat == 'M'].productcode.unique()))
	partsandcomp = partsandcomp.union(set(df.loc[df.pc == 1].productcode.unique()))
	tradeflows += df.shape[0]+1
	del df
	gc.collect()		
#-Pandas-#
exporters = pd.Series(sorted(list(exporters)))
totalproducts = pd.Series(sorted(list(totalproducts)))
manufproducts = pd.Series(sorted(list(manufproducts)))
partsandcomp = pd.Series(sorted(list(partsandcomp)))
#-Data Stats Table-#
stats = {
	'Number of Exporters' : len(exporters),
	'Number of Total Products' : len(totalproducts),
	'Number of Manuf. Products': len(manufproducts),
	'Number of Parts and Comp.': len(partsandcomp),
	'Number of Trade Flows' : tradeflows
}
table2 = pd.Series(stats)
table2.name = "Export"
#-Import Data-#
importers = set()
totalproducts = set()
manufproducts = set()
partsandcomp = set()
tradeflows = 0
for year in xrange(1995,2013+1,1):
	print "Processing year %s ..."%year
	df = pd.read_stata(SOURCE_DIR+"baci92-sitcr3-dataset/"+"baci92-sitcr3-import-%s-dataset.dta"%(year))
	importers = importers.union(set(df.country.unique()))
	totalproducts = totalproducts.union(set(df.productcode.unique()))
	manufproducts = manufproducts.union(set(df.loc[df.sitccat == 'M'].productcode.unique()))
	partsandcomp = partsandcomp.union(set(df.loc[df.pc == 1].productcode.unique()))
	tradeflows += df.shape[0]+1
	del df
	gc.collect()		
#-Pandas-#
importers = pd.Series(sorted(list(importers)))
totalproducts = pd.Series(sorted(list(totalproducts)))
manufproducts = pd.Series(sorted(list(manufproducts)))
partsandcomp = pd.Series(sorted(list(partsandcomp)))
#-Data Stats Table-#
stats = {
	'Number of Importers' : len(importers),
	'Number of Total Products' : len(totalproducts),
	'Number of Manuf. Products': len(manufproducts),
	'Number of Parts and Comp.': len(partsandcomp),
	'Number of Trade Flows' : tradeflows
}
table3 = pd.Series(stats)
table3.name = "Import"
#-Table-#
table = pd.DataFrame([table1,table2,table3]).T
table.to_excel(RESULTS_DIR+"tables/"+"baci92-sitcr3-basic-stats.xlsx")

#-Importer/Exporter Table-#
exporters.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3-exporterlist.csv")
importers.to_csv(RESULTS_DIR+"tables/"+"baci92-sitcr3-importerlist.csv")

#-UN Regions-#
cntryreg = pd.read_stata(SOURCE_DIR+"un-countryinfo-regions-development.dta")
table = cntryreg[["iso3c","countryname","regionname","areaname"]]
table = table.set_index("iso3c")
table = table.filter(exporters, axis=0)
table = table.reset_index()
table.sort(columns=["iso3c"])
table.to_excel(RESULTS_DIR+"tables/"+"baci92-sitcr3-country-region-area-definitions.xlsx")



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

#-Add Final Assembly Products-#
data["sitcr3l2"] = data["productcode"].apply(lambda x: x[0:2])
data["fap"] = 0
data["fap"] = data[["sitcr3l2","fap"]].apply(lambda row: 1 if row["sitcr3l2"] == "75" else row["fap"], axis=1) #-Data Processing Machines-#
data["fap"] = data[["sitcr3l2","fap"]].apply(lambda row: 1 if row["sitcr3l2"] == "76" else row["fap"], axis=1) #-Telecommunications-#
data["fap"] = data[["sitcr3l2","fap"]].apply(lambda row: 1 if row["sitcr3l2"] == "77" else row["fap"], axis=1) #-Electrical Machinery-#
data["fap"] = data[["sitcr3l2","fap"]].apply(lambda row: 1 if row["sitcr3l2"] == "78" else row["fap"], axis=1) #-Road Vehicles-#
data["fap"] = data[["sitcr3l2","fap"]].apply(lambda row: 1 if row["sitcr3l2"] == "87" else row["fap"], axis=1) #-Professional Equipment-# 
data["fap"] = data[["sitcr3l2","fap"]].apply(lambda row: 1 if row["sitcr3l2"] == "88" else row["fap"], axis=1) #-Photographic Apparatus-#
data["fap"] = data[["fap","pc"]].apply(lambda row: 0 if (row["pc"] == 1)&(row["fap"]==1) else row["fap"], axis=1) #-Remove Parts and Components-#
del data["sitcr3l2"]

#-Add Network Products-#
data["np"] = data[["pc","fap"]].apply(lambda row: 1 if (row["pc"] == 1)|(row["fap"] == 1) else 0,axis=1)

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
