"""
Analyse Other Datasets
======================

"atlas" -> Atlas of Complexity

"""

import gc
import re
import glob
import pandas as pd
import matplotlib.pyplot as plt

from dataset_info import RESULTS_DIR, TARGET_DATASET_DIR

#-Control-#

ATLAS = True

#-Atlas of Complexity-#
if ATLAS:
    #-Setup Source-#
    SOURCE_DIR = TARGET_DATASET_DIR["atlas"]
    HS_STORES = glob.glob(SOURCE_DIR + "*_hs92_*.h5")
    SITC_STORES = glob.glob(SOURCE_DIR + "*_sitcr2_*.h5")
    RESULTS_DIR = RESULTS_DIR["atlas"]

    #----------------------------------#
    #-ProductCode Intertemporal Tables-#
    #----------------------------------#
    
    print
    print "[INFO] Computing ProductCode Intertemporal Tables ..."

    DIR = RESULTS_DIR + "intertemporal-productcodes/"

    #-SITC DATA-#
    for store in SITC_STORES:
        print "Analysing SITC File: %s ..." % store
        fln = store.split("/")[-1].split(".")[0]
        store = pd.HDFStore(store)
        for dataset in store.keys():
            print "Computing table for dataset: %s ..." % dataset
            dataset = dataset.strip("/")
            product_level = int(dataset[-1])
            intertemp_product = store[dataset].groupby(["year", "sitc%s"%product_level]).sum().unstack("year")
            intertemp_product.columns = intertemp_product.columns.droplevel()
            intertemp_product.to_excel(DIR + "%s_L%s.xlsx"%(fln, product_level))
        store.close()

    #-HS DATA-#
    for store in HS_STORES:
        print "Analysing HS File: %s ..." % store
        fln = store.split("/")[-1].split(".")[0]
        store = pd.HDFStore(store)
        for dataset in store.keys():
            print "Computing table for dataset: %s ..." % dataset
            dataset = dataset.strip("/")
            product_level = int(dataset[-1])
            intertemp_product = store[dataset].groupby(["year", "hs%s"%product_level]).sum().unstack("year")
            intertemp_product.columns = intertemp_product.columns.droplevel()
            intertemp_product.to_excel(DIR + "%s_L%s.xlsx"%(fln, product_level))
        store.close()

    #----------------------------------#
    #-CountryCode Intertemporal Tables-#
    #----------------------------------#

    print
    print "[INFO] Computing CountryCode Intertemporal Tables ..."

    DIR = RESULTS_DIR + "intertemporal-countrycodes/"

    #-SITC-#
    for store in SITC_STORES:
        print "Analysing SITC File: %s ..." % store
        fln = store.split("/")[-1].split(".")[0]
        store = pd.HDFStore(store)
        for dataset in store.keys():
            print "Computing table for dataset: %s ..." % dataset
            product_level = int(dataset[-1])
            if product_level != 4:
                continue
            dataset = dataset.strip("/")
            if re.search("export", fln):
                print "[INFO] Export Data"
                intertemp_country = store[dataset].groupby(["year", "eiso3c"]).sum().unstack("year")
            elif re.search("import", fln):
                print "[INFO] Import Data"
                intertemp_country = store[dataset].groupby(["year", "iiso3c"]).sum().unstack("year")
            else:
                continue
            intertemp_country.columns = intertemp_country.columns.droplevel()
            intertemp_country.to_excel(DIR + "%s.xlsx"%(fln))
        store.close()

    #-HS DATA-#
    for store in HS_STORES:
        print "Analysing HS File: %s ..." % store
        fln = store.split("/")[-1].split(".")[0]
        store = pd.HDFStore(store)
        for dataset in store.keys():
            print "Computing table for dataset: %s ..." % dataset
            dataset = dataset.strip("/")
            if re.search("export", fln):
                print "[INFO] Export Data"
                intertemp_country = store[dataset].groupby(["year", "eiso3c"]).sum().unstack("year")
            elif re.search("import", fln):
                print "[INFO] Import Data"
                intertemp_country = store[dataset].groupby(["year", "iiso3c"]).sum().unstack("year")
            else:
                continue
            intertemp_country.columns = intertemp_country.columns.droplevel()
            intertemp_country.to_excel(DIR + "%s.xlsx"%(fln))
        store.close()

    ## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##
    ## ----> SIMPLE STATS TABLES <---- ##
    ## ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ##


    from pyeconlab.trade.util import describe

    print "Running DATASET_SIMPLESTATS_TABLE: ..."

    DIR = RESULTS_DIR + "tables/"

    #-SITC DATA-#

    for dataset_file in SITC_STORES:
        print "Running (SITC) STATS on File %s" % dataset_file
        store = pd.HDFStore(dataset_file)
        for dataset in sorted(store.keys()):
            product_level = dataset.strip("/")                                #Remove Directory Structure
            print "Computing SIMPLE STATS for dataset: %s" % product_level
            data = pd.read_hdf(dataset_file, key=dataset)
            productcode = "sitc%s"%(product_level[-1])
            dataset_table = describe(data, table_name=product_level, productcode=productcode)
            #-Memory Reduction-#
            del data
            gc.collect()
            if product_level == "L1":
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

    #-HS DATA-#

    for dataset_file in HS_STORES:
        print "Running (HS) STATS on File %s" % dataset_file
        store = pd.HDFStore(dataset_file)
        for dataset in sorted(store.keys()):
            product_level = dataset.strip("/")                                #Remove Directory Structure
            print "Computing SIMPLE STATS for dataset: %s" % product_level
            data = pd.read_hdf(dataset_file, key=dataset)
            productcode = "hs%s"%(product_level[-1])
            dataset_table = describe(data, table_name=product_level, productcode=productcode)
            #-Memory Reduction-#
            del data
            gc.collect()
            if product_level == "L1":
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
            latex_file.write(table.to_latex

    #-------#
    #-PLOTS-#
    #-------#

    #-Intertemporal Number of Positive Productcodes-#

    DIR = RESULTS_DIR + "plots/intertemporal-productcodes-num/"

    for dataset_file in SITC_STORES:
        print "Running (SITC) PLOTS on File %s" % dataset_file
        store = pd.HDFStore(dataset_file)
        for dataset in sorted(store.keys()):
            product_level = dataset.strip("/")                                #Remove Directory Structure
            print "Computing PLOT for dataset: %s" % product_level
            data = pd.read_hdf(dataset_file, key=dataset)
            productcode = "sitc%s"%(product_level[-1])
            if re.search("rca", dataset_file):
                value = "rca"
            else:
                value = "value"
            data_year = data.groupby(["year", productcode], as_index=False).sum().groupby("year").apply(lambda row: row[value].count())
            fig = data_year.plot(title="Dataset: %s (%s)"%(dataset, dataset_file))
            plt.savefig(DIR + "%s_%s_numproducts.pdf"%(dataset_file.split('/')[-1].split('.')[0], product_level))
            plt.close()
            #-Memory Reduction-#
            del data, data_year
            gc.collect()
        store.close()
