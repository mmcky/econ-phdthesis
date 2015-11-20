"""
Appendix G: Symmetric Vs. Assymetric Proximity Matrices

Year 2000 - NBER DATA

"""

import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt

from pyeconlab import DynamicProductLevelExportSystem

#-Local Imports-#
from dataset_info import TARGET_DATASET_DIR, CHAPTER_RESULTS
DATASET_DIR = TARGET_DATASET_DIR['nber']
RESULTS_DIR = CHAPTER_RESULTS["G"]

data = pd.read_hdf(DATASET_DIR+"nber-export-sitcr2l4-1962to2000.h5", "D")
data = data.rename(columns={'eiso3c' : 'country', 'sitc4' : 'productcode', 'value' : 'export'})
data = data.set_index(["year"])
system = DynamicProductLevelExportSystem()
system.from_df(data)

#-Year 2000-#
ys = system[2000]
ys.rca_matrix(complete_data=True)
ys.mcp_matrix()
ys.compute_pci()
ys.auto_adjust_pci_sign()
pci = ys.pci.copy()

#-Example Proximity Values-#
from pyeconlab.trade.classification import SITCR2
sitc_to_name = SITCR2().code_description_dict()
prox1 = ys.proximity_matrix()
products1 = ["8423", "0711"]
products2 = ["0611", "2927", "8451", "7810", "8441", "6584", "7924"]
exval = prox1.filter(items=products1, axis=0).filter(items=products2, axis=1).T.unstack().to_frame(name="Proximity")
exval.index.names = ["P1", "P2"]
exval = exval.groupby(level="P1").apply(lambda x: x.sort(columns="Proximity", ascending=False))
exval.index = exval.index.droplevel()
exval = exval.reset_index()
exval["P1 Description"] = exval["P1"].apply(lambda x: sitc_to_name[x])
exval["P2 Description"] = exval["P2"].apply(lambda x: sitc_to_name[x])
exval = exval.set_index(["P1","P1 Description","P2","P2 Description"])
exval.to_excel(RESULTS_DIR+"proximity-examples-yr2000-nber-datasetD.xlsx")


#-Symmetric Proximity Analysis-#
prox1 = ys.compute_proximity(matrix_type='symmetric')
fig1 = ys.plot_proximity(prox_cutoff=0.6, sortby=pci, sortby_text="PCI", step=15)
ax = fig1.gca()
ax.set_title("Symmetric Proximity Matrix [Yr: 2000]")
plt.savefig(RESULTS_DIR + "proximity-symmetric-yr2000-nber-datasetD.png" , dpi=600)

products1 = ["8423", "8441", "8451", "6584"]
products2 = ["8423", "8441", "8451", "6584"]
exval = prox1.filter(items=products1, axis=0).filter(items=products2, axis=1).T.unstack().to_frame(name="Proximity")
exval.index.names = ["P1", "P2"]
exval = exval.groupby(level="P1").apply(lambda x: x.sort(columns="Proximity", ascending=False))
exval.index = exval.index.droplevel()
exval = exval.reset_index()
exval = exval.set_index(["P1","P2"]).unstack()
exval = exval.reset_index()
exval["P1 Description"] = exval["P1"].apply(lambda x: sitc_to_name[x])
exval = exval.set_index(["P1", "P1 Description"])
exval.to_excel(RESULTS_DIR+"proximity-symmetric-examples-yr2000-nber-datasetD.xlsx")


#------------------------------#
#-Asymetric Proximity Analysis-#
#------------------------------#
prox2 = ys.compute_proximity(matrix_type="asymmetric")
fig2 = ys.plot_proximity(prox_cutoff=0.6, sortby=pci, sortby_text="PCI", step=15)
ax = fig2.gca()
ax.set_title("Asymmetric Proximity Matrix [Yr: 2000]")
plt.savefig(RESULTS_DIR + "proximity-asymmetric-yr2000-nber-datasetD-value-examples.png" , dpi=600)

products1 = ["8423", "8441", "8451", "6584"]
products2 = ["8423", "8441", "8451", "6584"]
exval = prox2.filter(items=products1, axis=0).filter(items=products2, axis=1).T.unstack().to_frame(name="Proximity")
exval.index.names = ["P1", "P2"]
exval = exval.groupby(level="P1").apply(lambda x: x.sort(columns="Proximity", ascending=False))
exval.index = exval.index.droplevel()
exval = exval.reset_index()
exval = exval.set_index(["P1","P2"]).unstack()
exval = exval.reset_index()
exval["P1 Description"] = exval["P1"].apply(lambda x: sitc_to_name[x])
exval = exval.set_index(["P1", "P1 Description"])
exval.to_excel(RESULTS_DIR+"proximity-asymmetric-yr2000-nber-datasetD-value-examples.xlsx")

#-Histogram Comparing Symmetric and Asymmetric Proximity-#
s1 = prox1.unstack()
s1 = s1.apply(lambda x: np.nan if x == 1 else x)
s1 = s1.apply(lambda x: np.nan if x == 0 else x)
s1v = s1.values
bins = np.linspace(0,1,50)
plt.hist(s1v, bins, alpha=0.5, label="Symmetric") 
s2 = prox2.unstack()
s2 = s2.apply(lambda x: np.nan if x == 1 else x)
s2 = s2.apply(lambda x: np.nan if x == 0 else x)
s2v = s2.values
bins = np.linspace(0,1,50)
plt.hist(s2v, bins, alpha=0.5, label="Asymmetric")
plt.legend(loc="upper right")
ax = plt.gca()
ax.set_xlabel("Proximity Values")
ax.set_ylabel("Frequency")
plt.savefig(RESULTS_DIR+"proximity-symmetric-and-asymmetric-overlayedhistogram-yr2000-nber-datasetD.png", dpi=600) 