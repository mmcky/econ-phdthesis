"""
Analyse Tables, Plots and Construct Meta Data for BACI Data
"""

import os
import gc
import glob
import matplotlib.pyplot as plt
import pandas as pd

#-HS Levels-#
HS96L6 = True

#-SITC Levels-#
SITCR2L5 = True
SITCR2L4 = True
SITCR2L3 = True
SITCR2L2 = True
SITCR2L1 = True

#---------------#
#-Control Logic-#
#---------------#

RAW_SIMPLESTATS_TABLE = True

DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES = True
DATASET_COUNTRYCODE_INTERTEMPORAL_TABLES = True
DATASET_SIMPLESTATS_TABLE = True
DATASET_PERCENTWORLDTRADE_PLOTS = True

#-----#
#-RAW-#
#-----#

from dataset_info import RESULTS_DIR, TARGET_DATASET_DIR
SOURCE_DIR = TARGET_DATASET_DIR["baci96"]
STORE = "raw_baci_hs96-1998-2012.h5"
RESULTS_DIR = RESULTS_DIR["baci96"]

if RAW_SIMPLESTATS_TABLE:

    from pyeconlab.trade.util import describe

    print "Running RAW_SIMPLESTATS_TABLE ..."

    DIR = RESULTS_DIR + "tables/"
    STORE = SOURCE_DIR + STORE

    print "Running STATS on File %s" % STORE
    store = pd.HDFStore(STORE)
    for dataset in sorted(store.keys()):
        dataset = dataset.strip("/")                                #Remove Directory Structure
        print "Computing SIMPLE STATS for dataset: %s" % dataset
        data = pd.read_hdf(STORE, key=dataset)
        productcode = "hs6"
        dataset_table = describe(data, table_name=dataset, productcode=productcode, exporter="eiso3n", importer="iiso3n")
        del data
        gc.collect()
    store.close()
    #-Excel Table-#
    fl = "baciraw-trade-hs6-1998to2012_stats.xlsx"
    dataset_table.to_excel(DIR + fl)
    #-Latex Snippet-#
    fl = "baciraw-trade-hs6-1998to2012_stats.tex"
    with open(DIR + fl, "w") as latex_file:
        latex_file.write(dataset_table.to_latex())

#----------#
#-DATASETS-#
#----------#

from dataset_info import RESULTS_DIR, TARGET_DATASET_DIR
SOURCE_DIR = TARGET_DATASET_DIR["baci96"]
STORES = glob.glob(SOURCE_DIR + "*.h5")
RESULTS_DIR = RESULTS_DIR["baci96"]

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##
## ---> Product Composition Tables <--- ##
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##

STORES = [x for x in STORES if x.split("/")[-1][0:3] != "raw"]  #Filter Out RAW Files

def split_filenames(fl):
    dataset, data_type, classification, years = fl.split("-")
    classification, product_level = classification[:-2], classification[-1:]
    return dataset, data_type, classification, product_level

if DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES:

    print "Running DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES ..."

    DIR = RESULTS_DIR + "intertemporal-productcodes/"

    for store in STORES:
        print "Computing Composition Tables for: %s" % store
        dataset, data_type, classification, product_level = split_filenames(store.split("/")[-1])
        store = pd.HDFStore(store)
        for dataset in store.keys():
            print "Computing table for dataset: %s ..." % dataset
            dataset = dataset.strip("/")
            intertemp_product = store[dataset].groupby(["year", "sitc%s"%product_level]).sum().unstack("year")
            intertemp_product.columns = intertemp_product.columns.droplevel()
            intertemp_product.to_excel(DIR + "intertemporal_product_%s_%sl%s_%s.xlsx"%(data_type, classification, product_level, dataset))
        store.close()

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##
## ---> Country Composition Tables <--- ##
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##

if DATASET_COUNTRYCODE_INTERTEMPORAL_TABLES:

    print "Running DATASET_COUNTRYCODE_INTERTEMPORAL_TABLES ..."

    DIR = RESULTS_DIR + "intertemporal-countrycodes/"

    for store in STORES:
        print "Computing Composition Tables for: %s" % store
        dataset, data_type, classification, product_level = split_filenames(store.split("/")[-1])
        store = pd.HDFStore(store)
        for dataset in store.keys():
            print "Computing table for dataset: %s ..." % dataset
            dataset = dataset.strip("/")
            if data_type == "export":
                intertemp_country = store[dataset].groupby(["year", "eiso3c"]).sum().unstack("year")
            if data_type == "import":
                intertemp_country = store[dataset].groupby(["year", "iiso3c"]).sum().unstack("year")
            else:
                continue
            intertemp_country.columns = intertemp_country.columns.droplevel()
            intertemp_country.to_excel(DIR + "intertemporal_country_%s_%s_%s.xlsx"%(data_type, classification, dataset))
        store.close()

## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##
## ----> SIMPLE STATS TABLES <---- ##
## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##


if DATASET_SIMPLESTATS_TABLE:

    from pyeconlab.trade.util import describe

    print "Running DATASET_SIMPLESTATS_TABLE: ..."

    DIR = RESULTS_DIR + "tables/"

    for dataset_file in STORES:
        print "Running STATS on File %s" % dataset_file
        store = pd.HDFStore(dataset_file)
        for dataset in sorted(store.keys()):
            dataset = dataset.strip("/")                                #Remove Directory Structure
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
        table.to_excel(DIR + fl)
        #-Latex Snippet-#
        fl = dataset_file.split("/")[-1].split(".")[0] + "_stats" + ".tex"
        with open(DIR + fl, "w") as latex_file:
            latex_file.write(table.to_latex())


if DATASET_PERCENTWORLDTRADE_PLOTS:
    
    print "DATASET_PERCENTWORLDTRADE_PLOTS ... "

    DIR = RESULTS_DIR + "plots/percent_world_values/"

    #-World Values-#
    fl = "./output/dataset/baci96/raw_baci_world_yearly-1998to2012.h5"
    world_values = pd.read_hdf(fl, key="World")["value"]

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
            plt.savefig(DIR + "%s_%s_percent_wld.pdf"%(dataset, dataset_file.split('/')[-1].split('.')[0]))
            plt.close()
        store.close()



