"""
Analyse Tables, Plots and Construct Meta Data for NBERBACI Data
"""

from __future__ import division

import os
import gc
import glob
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

#-Years-#
Y6200 = False
Y7400 = True
Y8400 = True

#---------#
#-Control-#
#---------#

DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES = False 
DATASET_COUNTRYCODE_INTERTEMPORAL_TABLES = False
DATASET_SIMPLESTATS_TABLE = True
DATASET_PERCENTWORLDTRADE_PLOTS = False

#-Helper Functions-#

def split_filenames(fl):
    dataset, data_type, classification, years, harmonised = fl.split("-")
    classification, product_level = classification[:-2], classification[-1:]
    return dataset, data_type, classification, product_level

#----------#
#-DATASETS-#
#----------#

#--------------#
#-1962 to 2012-#
#--------------#

if Y6200:

    from dataset_info import RESULTS_DIR, TARGET_DATASET_DIR
    SOURCE_DIR = TARGET_DATASET_DIR["nberbaci96"]
    STORES = glob.glob(SOURCE_DIR + "*.h5")
    RESULTS_DIR = RESULTS_DIR["nberbaci96"]

    ## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##
    ## ---> Product Composition Tables <--- ##
    ## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##

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
                #-Memory Release-#
                del intertemp_product
                gc.collect()
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
                intertemp_country.to_excel(DIR + "intertemporal_country_%s_%sl%s_%s.xlsx"%(data_type, classification, product_level, dataset))
                #-Memory Release-#
                del intertemp_country
                gc.collect()
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
                #-Memory Reduction-#
                del data
                gc.collect()
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


    #-!!-Note: These are not truely valid comparisons as there are ~40 countries droppped from CEPII BACI. -!!-#
    #-!!-But Surprising the values drop to ~90% of values in original dataset-!!-#

    if DATASET_PERCENTWORLDTRADE_PLOTS:
        
        print "DATASET_PERCENTWORLDTRADE_PLOTS ... "

        DIR = RESULTS_DIR + "plots/percent_world_values/"
        
        def join_values(row):
            """ Join Rows and Average if both values are not np.nan """
            if np.isnan(row["NBER"]) and np.isnan(row["BACI"]):
                return np.nan
            elif np.isnan(row["NBER"]):
                return row["BACI"]
            elif np.isnan(row["BACI"]):
                return row["NBER"]
            else:
                return (row["NBER"] + row["BACI"]) / 2

        #-World Values-#
        fl = "./output/dataset/baci96/raw_baci_world_yearly-1998to2012.h5"
        baci_world_values = pd.read_hdf(fl, key="World")["value"]
        baci_world_values.name = "BACI"
        fl = "./output/dataset/nber/raw_nber_world_yearly-1962to2000.h5"
        nber_world_values = pd.read_hdf(fl, key="World")["value"]
        nber_world_values.name = "NBER"
        world_values = pd.DataFrame([baci_world_values, nber_world_values]).T
        world_values["value"] = world_values.apply(lambda row: join_values(row), axis=1)        #-Average Overlap Years-#
        world_values = world_values["value"]

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
                #-Memory Release-#
                del data
                gc.collect()
            store.close()


#--------------#
#-1974 to 2012-#
#--------------#

if Y7400:

    LOCAL_DIR = "Y7400/"

    from dataset_info import RESULTS_DIR, TARGET_DATASET_DIR
    SOURCE_DIR = TARGET_DATASET_DIR["nberbaci96"]
    STORES = glob.glob(SOURCE_DIR + LOCAL_DIR + "*.h5")
    RESULTS_DIR = RESULTS_DIR["nberbaci96"]

    ## ---> Product Composition Tables <--- ##

    if DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES:

        print "Running DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES ..."

        DIR = RESULTS_DIR + "intertemporal-productcodes/" + LOCAL_DIR

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
                #-Memory Release-#
                del intertemp_product
                gc.collect()
            store.close()

    ## ---> Country Composition Tables <--- ##

    if DATASET_COUNTRYCODE_INTERTEMPORAL_TABLES:

        print "Running DATASET_COUNTRYCODE_INTERTEMPORAL_TABLES ..."

        DIR = RESULTS_DIR + "intertemporal-countrycodes/" + LOCAL_DIR

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
                intertemp_country.to_excel(DIR + "intertemporal_country_%s_%sl%s_%s.xlsx"%(data_type, classification, product_level, dataset))
                #-Memory Release-#
                del intertemp_country
                gc.collect()
            store.close()


    ## ----> SIMPLE STATS TABLES <---- ##

    if DATASET_SIMPLESTATS_TABLE:

        from pyeconlab.trade.util import describe

        print "Running DATASET_SIMPLESTATS_TABLE: ..."

        DIR = RESULTS_DIR + "tables/" + LOCAL_DIR

        for dataset_file in STORES:
            print "Running STATS on File %s" % dataset_file
            store = pd.HDFStore(dataset_file)
            for dataset in sorted(store.keys()):
                dataset = dataset.strip("/")                                #Remove Directory Structure
                print "Computing SIMPLE STATS for dataset: %s" % dataset
                data = pd.read_hdf(dataset_file, key=dataset)
                productcode = "".join(dataset_file.split("/")[-1].split("-")[2].split("r2l"))
                dataset_table = describe(data, table_name=dataset, productcode=productcode)
                #-Memory Reduction-#
                del data
                gc.collect()
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



#--------------#
#-1984 to 2012-#
#--------------#

if Y8400:

    LOCAL_DIR = "Y8400/"

    from dataset_info import RESULTS_DIR, TARGET_DATASET_DIR
    SOURCE_DIR = TARGET_DATASET_DIR["nberbaci96"]
    STORES = glob.glob(SOURCE_DIR + LOCAL_DIR + "*.h5")
    RESULTS_DIR = RESULTS_DIR["nberbaci96"]

    ## ---> Product Composition Tables <--- ##

    if DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES:

        print "Running DATASET_PRODUCTCODE_INTERTEMPORAL_TABLES ..."

        DIR = RESULTS_DIR + "intertemporal-productcodes/" + LOCAL_DIR

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
                #-Memory Release-#
                del intertemp_product
                gc.collect()
            store.close()

    ## ---> Country Composition Tables <--- ##

    if DATASET_COUNTRYCODE_INTERTEMPORAL_TABLES:

        print "Running DATASET_COUNTRYCODE_INTERTEMPORAL_TABLES ..."

        DIR = RESULTS_DIR + "intertemporal-countrycodes/" + LOCAL_DIR

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
                #-Memory Release-#
                del intertemp_country
                gc.collect()
            store.close()


    ## ----> SIMPLE STATS TABLES <---- ##

    if DATASET_SIMPLESTATS_TABLE:

        from pyeconlab.trade.util import describe

        print "Running DATASET_SIMPLESTATS_TABLE: ..."

        DIR = RESULTS_DIR + "tables/" + LOCAL_DIR

        for dataset_file in STORES:
            print "Running STATS on File %s" % dataset_file
            store = pd.HDFStore(dataset_file)
            for dataset in sorted(store.keys()):
                dataset = dataset.strip("/")                                #Remove Directory Structure
                print "Computing SIMPLE STATS for dataset: %s" % dataset
                data = pd.read_hdf(dataset_file, key=dataset)
                productcode = "".join(dataset_file.split("/")[-1].split("-")[2].split("r2l"))
                dataset_table = describe(data, table_name=dataset, productcode=productcode)
                #-Memory Reduction-#
                del data
                gc.collect()
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