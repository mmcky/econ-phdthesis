"""
Chapter: Appendix A Dataset
===========================

This contains tables, plots and analysis used in the construction of the Dataset Appendix Chapter

NES and Non-Country Areas 				=> Percentage of Trade Data Contained in Non Country ISO3C Codes
NBER AX Codes as a % of World Exports 	=> Percentage of Trade Data Contained in AX Product Codes

"""
import re
import pandas as pd
import matplotlib.pyplot as plt

from dataset_info import TARGET_RAW_DIR, CHAPTER_RESULTS

RESULTS_DIR = CHAPTER_RESULTS["A"]

#------#
#-NBER-#
#------#

#-Setup-#
#-------#
source_dir = TARGET_RAW_DIR["nber"]
for year in xrange(1962,2000+1,1):
    if year == 1962:
        source_data = pd.read_hdf(source_dir+"nber_year.h5", 'Y%s'%year)
    else:
        source_data = source_data.append(pd.read_hdf(source_dir+"nber_year.h5", 'Y%s'%year))

#-World Values-#
world_values = source_data.loc[(source_data.importer == "World") & (source_data.exporter == "World")] 
world_values = world_values.groupby("year").sum()["value"]


#-NES and Non-Country Areas-#
#---------------------------#
plt.clf()
from pyeconlab.trade.dataset.NBERWTF.meta import countryname_to_iso3c
data = source_data.copy()
data = data.loc[(data.importer != "World") & (data.exporter != "World")].reset_index()
data["EC"] = data["exporter"].apply(lambda x: countryname_to_iso3c[x])
data["IC"] = data["importer"].apply(lambda x: countryname_to_iso3c[x])
data["NC"] = data[["EC", "IC"]].apply(lambda row: 1 if (row["EC"]==".")|(row["IC"]==".") else 0, axis=1)
nc_data = data.loc[data.NC == 1].groupby("year").sum()["value"]
#-Percentage-#
#~Trade~#
result = nc_data.div(world_values)*100
describe = result.describe()
describe.to_csv(RESULTS_DIR + "nber_notcountry_percent_world_trade_table.csv")
pd.DataFrame(describe).to_latex(RESULTS_DIR + "nber_notcountry_percent_world_trade_table.tex")
ax = result.plot(title="NES Trade Flows [% of World Trade]", yticks=[0,1,2,3,4,5])
ax.set_ylabel("Percent of World Trade")
ax.set_xlabel("Year")
plt.savefig(RESULTS_DIR + "nber_notcountry_percent_world_trade_plot.pdf")
plt.clf()
#~Export~#
data_export = data.groupby(["year", "exporter", "EC"]).sum().reset_index()
nc_data_export = data_export.loc[data_export.EC == "."].groupby("year").sum()["value"]
result = nc_data_export.div(world_values)*100
describe = result.describe()
describe.to_csv(RESULTS_DIR + "nber_notcountry_percent_world_export_table.csv")
pd.DataFrame(describe).to_latex(RESULTS_DIR + "nber_notcountry_percent_world_export_table.tex")
ax = result.plot(title="NES Export Flows [% of World Trade]")
ax.set_ylabel("Percent of World Trade")
ax.set_xlabel("Year")
plt.savefig(RESULTS_DIR + "nber_notcountry_percent_world_export_plot.pdf")
plt.clf()
#~Import ... Not Required~#
del data, nc_data, data_export, nc_data_export


#-NBER AX Codes as a % of World Exports-#
#---------------------------------------#
data = source_data.copy()
data["AX"] = data["sitc4"].apply(lambda x: 1 if re.search("[aAxX]", x) else 0)
AX = data.loc[data.AX == 1]
AX = AX.groupby("year").sum()["value"]
#-Percentage-#
result = AX.div(world_values)*100
describe = result.describe()
describe.to_csv(RESULTS_DIR + "nber_ax_percent_world_trade_table.csv")
pd.DataFrame(describe).to_latex(RESULTS_DIR + "nber_ax_percent_world_trade_table.tex")
ax = result.plot(title="AX [% of World Trade]", )
ax.set_ylabel("Percent of World Trade")
ax.set_xlabel("Year")
plt.savefig(RESULTS_DIR + "nber_ax_percent_world_trade_plot.pdf")
del data, AX
