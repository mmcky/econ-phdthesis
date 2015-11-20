"""
Compute NBER Datasets
=====================

Author: Matthew McKay (mamckay@gmail.com)

Filename rules: {{ source }}-{{ flow }}-{{ classification }}-{{ years }}-{{ raw/cleaned }}.h5
                This archive includes all varients of this type of trade data
                
                Required Keys
                -------------
                'A' = pd.DataFrame('Dataset A')
                'A-OPTIONS' = Dict{Record of Options}
                'A-DESC' = str


Supporting Python Files 
-----------------------
1. dataset-info.py          Contains Information about the relevant datasets
2. dataset-compile-raw.py   Compiles RAW data files to a single dataset file (HDF Format)

Sources
-------
1.  nber
    md5:    36a376e5a01385782112519bddfac85e
    notes:  NBER World Trade Flow RAW Dataset

Supporting Files
----------------

{SITCR2L4}
1. Need to compute country code intertemporal consistency for Level 4 Data and update pyeconlab

"""

#-External Packages-#
import numpy as np
import pandas as pd
import sys
import gc
import warnings

#-Package Imports-#
from pyeconlab.trade.dataset.NBERWTF.meta import countryname_to_iso3c, iso3c_recodes_for_1962_2000, incomplete_iso3c_for_1962_2000
from pyeconlab.util import concord_data
from pyeconlab.trade.classification import SITC
from pyeconlab.trade.dataset.NBERWTF import construct_sitcr2

#-Local Imports-#
from dataset_info import TARGET_RAW_DIR, TARGET_DATASET_DIR, YEARS #-Dataset Information

#-Update to Pull from PyEconLab?-#
from dataset_construct_nber_options import DATA_DESCRIPTION, RAW_DATA_DESCRIPTION, DATA_OPTIONS, RAW_DATA_OPTIONS

#---------#
#-Control-#
#---------#

#-1962 to 2000-#
Y6200 = True
#-L4-#
SITCR2L4 = True
SITCR2L4_TRADE = True
SITCR2L4_EXPORT = True
SITCR2L4_IMPORT = True
#-L3-#
SITCR2L3 = True
SITCR2L3_TRADE = True
SITCR2L3_EXPORT = True
SITCR2L3_IMPORT = True
#-L2-#
SITCR2L2 = True
SITCR2L2_TRADE = True
SITCR2L2_EXPORT = True
SITCR2L2_IMPORT = True
#-L1-#
SITCR2L1 = True
SITCR2L1_TRADE = True
SITCR2L1_EXPORT = True
SITCR2L1_IMPORT = True

#-RAW DATA-#
RAW_SITCR2L4 = True
RAW_WORLD_YEARLY = True        #Simple Total World Trade
RAW_COUNTRY_YEARLY = True      #Simple Total Country Trade & Various Aggregations
RAW_PRODUCT_YEARLY = True      #Levels 1,2,3,4 & Various Product Code Aggregations

#-1974 to 2000-#
Y7400 = True
Y7400Trade = True
Y7400Export = True
Y7400Import = True

#-1984 to 2000-#
Y8400 = True
Y8400Trade = True
Y8400Export = True
Y8400Import = True 

#-Setup General Directories-#
source_dir = TARGET_RAW_DIR['nber']
target_dir = TARGET_DATASET_DIR['nber']

start_year, end_year = YEARS['nber']

#------------------#
#-Helper Functions-#
#------------------#

def load_raw_dataset(fn, start_year, end_year, verbose=False):
    """
    Load Raw NBER Dataset
    """
    data = pd.DataFrame()
    for year in range(start_year, end_year+1, 1):
        print "Loading Year: %s" % year
        data = data.append(pd.read_hdf(fn, "Y%s"%year))
    data = data.reset_index()
    del data["index"]
    if verbose: print data.year.unique()
    return data

def fix_raw_data(data):
    """
    Apply Fixes to NBER Data for ZWE, MWI
    
    ZWE - Drop Data in 1963,1964
    MWI - Drop Data in 1963,1964

    """
    print "[INFO] Adjustments to RAW DATA based on NBER FAQ ..."
    for year in xrange(1963,1964+1,1):
        for country in ["Malawi", "Zimbabwe"]:
            print
            print "[INFO] Removing %s trade values in year %s ..."%(country, year)
            drop = data.loc[(data.year == year)&((data.exporter == country)|(data.importer == country))]
            print "...... Dropping"
            print drop.to_string()
            print "[INFO] Number of Observations = %s"%data.shape[0]
            print "[INFO] Dropping ... %s observations"%len(drop)
            data = data.drop(drop.index)
            print "[INFO] Number of Observations after drop = %s"%data.shape[0]
            gc.collect()
    return data


#-------------------------#
#-NBER DATA: 1962 to 2000-#
#-------------------------#

if Y6200:

    print "[INFO] Running Y6200 ..."

    #-Source Information-#
    #~~~~~~~~~~~~~~~~~~~~#

    print "---> Loading RAW Data (1962 to 2000) <---"
    print
    fn = source_dir + "nber_year.h5"
    rawdata = load_raw_dataset(fn, start_year, end_year)
    rawdata = fix_raw_data(rawdata)                                 #Adjustments Made for FAQ
    print "---> Loading RAW HK-CHINA Adjustments <---"
    fn, start_year, end_year = (source_dir + "nber_supp_year.h5", 1988, 2000)
    rawdata_hk = load_raw_dataset(fn, start_year, end_year)

    #-Precomputed Data Options--#
    from pyeconlab.trade.dataset.NBERWTF.meta import IntertemporalProducts #Should make a Local Static Copy
    ICP = IntertemporalProducts().IC6200

    #-Construct SITC R2 Level 4 Trade Datasets-#
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    DATA_TYPES = []
    if SITCR2L4_TRADE: DATA_TYPES.append('trade')
    if SITCR2L4_EXPORT: DATA_TYPES.append('export')
    if SITCR2L4_IMPORT: DATA_TYPES.append('import')

    #-!!-Future Work: Collapse these to Level loops-!!-#

    if SITCR2L4:

        print
        print "---> COMPUTING SITC REVISION 2 LEVEL 4 DATASETS <---"
        print
        # from pyeconlab.trade.dataset.NBERWTF import construct_sitcr2l4
        for data_type in DATA_TYPES:
            #-Setup Store-#
            fn = "nber-%s-sitcr2l4-1962to2000.h5" % data_type                                    #-Write File: {{ source }}-{{ flow }}-{{ classification }}-{{ years }}.h5-#
            store = pd.HDFStore(target_dir+fn, complevel=9, complib='zlib')
            #-Compute Datasets-#
            for dataset in sorted(DATA_OPTIONS.keys()):
                print "\n[SITCR2L4] Computing Dataset %s for %s" % (dataset, data_type)
                #-IF Adjust Hong Kong Data then Add Data to the Tuple-#
                if DATA_OPTIONS[dataset]['adjust_hk'] == True: 
                    DATA_OPTIONS[dataset]['adjust_hk'] = (True, rawdata_hk)                     #Add Data for construct_sitcr2l3 function, then remove so not saved as an attribute
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = (False, None)
                #-Intertemporal Product Codes-#
                if DATA_OPTIONS[dataset]['intertemp_productcode']:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = (True, ICP[4])
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = (False, None)
                #-Compute Data-#
                data = construct_sitcr2(rawdata.copy(deep=True), data_type=data_type, level=4, values_only=True, **DATA_OPTIONS[dataset])
                store.put(dataset, data, format='table')
                #-Remove Hong Kong Data-#
                if DATA_OPTIONS[dataset]['adjust_hk'][0] == True:
                    DATA_OPTIONS[dataset]['adjust_hk'] = True
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = False
                if DATA_OPTIONS[dataset]['intertemp_productcode'][0] == True:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = True
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = False
                store.get_storer(dataset).attrs.options = DATA_OPTIONS[dataset]
                store.get_storer(dataset).attrs.data_type = data_type
                store.get_storer(dataset).attrs.description = DATA_DESCRIPTION[dataset]
            print store
            #-Close-#
            store.close()


    #-Construct SITC R2 Level 3 Trade Datasets-#
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    DATA_TYPES = []
    if SITCR2L3_TRADE: DATA_TYPES.append('trade')
    if SITCR2L3_EXPORT: DATA_TYPES.append('export')
    if SITCR2L3_IMPORT: DATA_TYPES.append('import')

    if SITCR2L3:
        print
        print "---> COMPUTING SITC REVISION 2 LEVEL 3 DATASETS <---"
        print

        #-Import this as a Function from pyeconlab-#
        # from pyeconlab.trade.dataset.NBERWTF import construct_sitcr2l3
        for data_type in DATA_TYPES:
            #-Setup Store-#
            fn = "nber-%s-sitcr2l3-1962to2000.h5" % data_type                                    #-Write File: {{ source }}-{{ flow }}-{{ classification }}-{{ years }}.h5-#
            store = pd.HDFStore(target_dir+fn, complevel=9, complib='zlib')
            #-Compute Datasets-#
            for dataset in sorted(DATA_OPTIONS.keys()):
                print "\n[SITCR2L3] Computing Dataset %s for %s" % (dataset, data_type)
                #-IF Adjust Hong Kong Data then Add Data to the Tuple-#
                if DATA_OPTIONS[dataset]['adjust_hk'] == True: 
                    DATA_OPTIONS[dataset]['adjust_hk'] = (True, rawdata_hk)                     #Add Data for construct_sitcr2l3 function, then remove so not saved as an attribute
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = (False, None)
                #-Intertemporal Product Codes-#
                if DATA_OPTIONS[dataset]['intertemp_productcode']:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = (True, ICP[3])
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = (False, None)
                #-Compute Data-#
                data = construct_sitcr2(rawdata.copy(deep=True), data_type=data_type, level=3, values_only=True, **DATA_OPTIONS[dataset])
                store.put(dataset, data, format='table')
                #-Remove Hong Kong Data-#
                if DATA_OPTIONS[dataset]['adjust_hk'][0] == True:
                    DATA_OPTIONS[dataset]['adjust_hk'] = True
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = False
                if DATA_OPTIONS[dataset]['intertemp_productcode'][0] == True:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = True    
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = False                             
                store.get_storer(dataset).attrs.options = DATA_OPTIONS[dataset]
                store.get_storer(dataset).attrs.data_type = data_type
                store.get_storer(dataset).attrs.description = DATA_DESCRIPTION[dataset]
            print store
            #-Close-#
            store.close()


    #-Construct SITC R2 Level 2 Trade Datasets-#
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    DATA_TYPES = []
    if SITCR2L2_TRADE: DATA_TYPES.append('trade')
    if SITCR2L2_EXPORT: DATA_TYPES.append('export')
    if SITCR2L2_IMPORT: DATA_TYPES.append('import')

    if SITCR2L2:
        print
        print "---> COMPUTING SITC REVISION 2 LEVEL 2 DATASETS <---"
        print
        #-Import this as a Function from pyeconlab-#
        # from pyeconlab.trade.dataset.NBERWTF import construct_sitcr2l2
        for data_type in DATA_TYPES:
            #-Setup Store-#
            fn = "nber-%s-sitcr2l2-1962to2000.h5" % data_type                                    #-Write File: {{ source }}-{{ flow }}-{{ classification }}-{{ years }}.h5-#
            store = pd.HDFStore(target_dir+fn, complevel=9, complib='zlib')
            #-Compute Datasets-#
            for dataset in sorted(DATA_OPTIONS.keys()):
                print "[SITCR2L2] Computing Dataset %s for %s" % (dataset, data_type)
                #-IF Adjust Hong Kong Data then Add Data to the Tuple-#
                if DATA_OPTIONS[dataset]['adjust_hk'] == True: 
                    DATA_OPTIONS[dataset]['adjust_hk'] = (True, rawdata_hk)                     #Add Data for construct_sitcr2l2 function, then remove so not saved as an attribute
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = (False, None)
                #-Intertemporal Product Codes-#
                if DATA_OPTIONS[dataset]['intertemp_productcode']:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = (True, ICP[2])
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = (False, None)
                #-Compute Data-#
                data = construct_sitcr2(rawdata.copy(deep=True), data_type=data_type, level=2, values_only=True, **DATA_OPTIONS[dataset])
                store.put(dataset, data, format='table')
                #-Remove Hong Kong Data-#
                if DATA_OPTIONS[dataset]['adjust_hk'][0] == True:
                    DATA_OPTIONS[dataset]['adjust_hk'] = True        
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = False
                if DATA_OPTIONS[dataset]['intertemp_productcode'][0] == True:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = True 
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = False                                            
                store.get_storer(dataset).attrs.options = DATA_OPTIONS[dataset]
                store.get_storer(dataset).attrs.data_type = data_type
                store.get_storer(dataset).attrs.description = DATA_DESCRIPTION[dataset]
            print store
            #-Close-#
            store.close()


    #-Construct SITC R2 Level 1 Trade Datasets-#
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
    DATA_TYPES = []
    if SITCR2L1_TRADE: DATA_TYPES.append('trade')
    if SITCR2L1_EXPORT: DATA_TYPES.append('export')
    if SITCR2L1_IMPORT: DATA_TYPES.append('import')

    if SITCR2L1:
        print
        print "---> COMPUTING SITC REVISION 2 LEVEL 1 DATASETS <---"
        print
        #-Import this as a Function from pyeconlab-#
        # from pyeconlab.trade.dataset.NBERWTF import construct_sitcr2l3
        for data_type in DATA_TYPES:
            #-Setup Store-#
            fn = "nber-%s-sitcr2l1-1962to2000.h5" % data_type                                    #-Write File: {{ source }}-{{ flow }}-{{ classification }}-{{ years }}.h5-#
            store = pd.HDFStore(target_dir+fn, complevel=9, complib='zlib')
            #-Compute Datasets-#
            for dataset in sorted(DATA_OPTIONS.keys()):
                print "[SITCR2L3] Computing Dataset %s for %s" % (dataset, data_type)
                #-IF Adjust Hong Kong Data then Add Data to the Tuple-#
                if DATA_OPTIONS[dataset]['adjust_hk'] == True: 
                    DATA_OPTIONS[dataset]['adjust_hk'] = (True, rawdata_hk)                     #Add Data for construct_sitcr2l1 function, then remove so not saved as an attribute
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = (False, None)
                #-Intertemporal Product Codes-#
                if DATA_OPTIONS[dataset]['intertemp_productcode']:
                    print "[INFO] This operation cannot occur at this level ... continuing"
                    continue
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = (False, None)
                #-Compute Data-#
                data = construct_sitcr2(rawdata.copy(deep=True), data_type=data_type, level=1, values_only=True, **DATA_OPTIONS[dataset])
                store.put(dataset, data, format='table')
                #-Remove Hong Kong Data-#
                if DATA_OPTIONS[dataset]['adjust_hk'][0] == True:
                    DATA_OPTIONS[dataset]['adjust_hk'] = True       
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = False
                if DATA_OPTIONS[dataset]['intertemp_productcode'][0] == True:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = True 
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = False                     
                store.get_storer(dataset).attrs.options = DATA_OPTIONS[dataset]
                store.get_storer(dataset).attrs.data_type = data_type
                store.get_storer(dataset).attrs.description = DATA_DESCRIPTION[dataset]
            print store
            #-Close-#
            store.close()


#---------------#
#- RAW DATASET -#
#---------------#

def load_all_rawdata(start_year, end_year):
    print "---> Loading RAW Data (1962 to 2000) <---"
    print
    fn = source_dir + "nber_year.h5"
    rawdata = load_raw_dataset(fn, start_year, end_year)
    rawdata = fix_raw_data(rawdata)                                 #Adjustments Made for FAQ
    print "---> Loading RAW HK-CHINA Adjustments <---"
    fn, start_year, end_year = (source_dir + "nber_supp_year.h5", 1988, 2000)
    rawdata_hk = load_raw_dataset(fn, start_year, end_year)
    return rawdata, rawdata_hk

if RAW_SITCR2L4:
    print
    print "---> COMPUTING SITC REVISION 2 LEVEL 4 RAW DATASETS <---"
    print
    rawdata, rawdata_hk = load_all_rawdata(start_year, end_year)
    #-Setup Store-#
    fn = "raw_nber-sitcr2l4-1962to2000.h5"    #-Write File: {{ source }}-{{ flow }}-{{ classification }}-{{ years }}.h5-#
    store = pd.HDFStore(target_dir+fn, complevel=9, complib='zlib')
    #-Compute Datasets-#
    for dataset in sorted(RAW_DATA_OPTIONS.keys()):
        print "[SITCR2L4] Computing Dataset %s for %s" % (dataset, 'trade')
        #-IF Adjust Hong Kong Data then Add Data to the Tuple-#
        if RAW_DATA_OPTIONS[dataset]['adjust_hk'] == True: 
            RAW_DATA_OPTIONS[dataset]['adjust_hk'] = (True, rawdata_hk)                     #Add Data for construct_sitcr2l3 function, then remove so not saved as an attribute
        else:
            RAW_DATA_OPTIONS[dataset]['adjust_hk'] = (False, None)
        #-Intertemporal Product Codes-#
        if RAW_DATA_OPTIONS[dataset]['intertemp_productcode']:
            RAW_DATA_OPTIONS[dataset]['intertemp_productcode'] = (True, ICP[4])
        else:
            RAW_DATA_OPTIONS[dataset]['intertemp_productcode'] = (False, None)
        #-Compute Data-#
        data = construct_sitcr2(rawdata.copy(deep=True), data_type='trade', level=4, **RAW_DATA_OPTIONS[dataset])
        store.put(dataset, data, format='table')
        #-Remove Hong Kong Data-#
        if RAW_DATA_OPTIONS[dataset]['adjust_hk'][0] == True:
            RAW_DATA_OPTIONS[dataset]['adjust_hk'] = True
        else:
            RAW_DATA_OPTIONS[dataset]['adjust_hk'] = False
        if RAW_DATA_OPTIONS[dataset]['intertemp_productcode'][0] == True:
            RAW_DATA_OPTIONS[dataset]['intertemp_productcode'] = True
        else:
            RAW_DATA_OPTIONS[dataset]['intertemp_productcode'] = False
        store.get_storer(dataset).attrs.options = RAW_DATA_OPTIONS[dataset]
        store.get_storer(dataset).attrs.data_type = 'trade'
        store.get_storer(dataset).attrs.description = RAW_DATA_DESCRIPTION[dataset]
    print store
    #-Close-#
    store.close()

    del data
    gc.collect()

#-Notes
# ~~~~~
# This Captures Total Export and Total Import Values in the NBER Dataset. 
# Total TRADE = X + M = 2 x Values Below

if RAW_WORLD_YEARLY:
    print
    print "---> COMPUTING WORLD YEARLY VALUES FROM RAW NBER DATASET <---"
    print 
    rawdata, rawdata_hk = load_all_rawdata(start_year, end_year)
    #-Setup Store-#
    fn = "raw_nber_world_yearly-1962to2000.h5"
    store = pd.HDFStore(target_dir+fn, complevel=9, complib='zlib')
    #-Compute Values Using World-#
    data = rawdata.copy(deep=True)
    data = data[["year", "exporter", "importer", "value"]].loc[(data.exporter == "World")&(data.importer == "World")].groupby(["year"]).sum()
    store.put("World", data, format='table')
        ##--!!--CHECK THIS THEN DELETE--!!-##
        # #-Compute Values Dropping World-#
        # #-This method should be equivalent - check then delete!-#
        # data = rawdata.copy(deep=True)
        # data = data[["year", "exporter", "importer", "value"]].loc[(data.exporter != "World")&(data.importer != "World")].groupby(["year"]).sum()
        # store.put("World2", data, format='table')
        ##--!!--CHECK THIS THEN DELETE--!!-##
    print store
    store.close()
    del rawdata, rawdata_hk
    gc.collect()

if RAW_COUNTRY_YEARLY:
    print
    print "---> COMPUTING COUNTRY YEARLY VALUES FROM RAW NBER DATASET <---"
    print
    
    from pyeconlab.trade.dataset.NBERWTF.meta import countryname_to_iso3c

    rawdata, rawdata_hk = load_all_rawdata(start_year, end_year)
    #-Setup Store-#
    fn = "raw_nber_country_year-1962to2000.h5"
    store = pd.HDFStore(target_dir+fn, complevel=9, complib='zlib')
    #-Compute Export Values Using World-#
    data = rawdata.copy(deep=True)
    data = data[["year", "exporter", "importer", "value"]].loc[(data.exporter != "World")&(data.importer == "World")].groupby(["year", "exporter"]).sum().reset_index()
    data['eiso3c'] = data['exporter'].apply(lambda x: countryname_to_iso3c[x])
    data = data.set_index(["year", "eiso3c", "exporter"])  #.unstack("year")
    # data.columns = data.columns.droplevel()
    store.put("CountryExport", data, format='table')
    ##--!!--CHECK THIS THEN DELETE--!!-##
    #-Country Values not using World-#
    #-This method should be equivalent - check then delete!-#
    data = rawdata.copy(deep=True)
    data = data[["year", "exporter", "importer", "value"]].loc[(data.exporter != "World")&(data.importer != "World")].groupby(["year", "exporter"]).sum().reset_index()
    data['eiso3c'] = data['exporter'].apply(lambda x: countryname_to_iso3c[x])
    data = data.set_index(["year", "eiso3c", "exporter"])   #.unstack("year")
    # data.columns = data.columns.droplevel()
    store.put("CNTRY2", data, format='table')
    ##--!!--CHECK THIS THEN DELETE--!!-##
    #-Compute Import Values Using World-#
    data = rawdata.copy(deep=True)
    data = data[["year", "exporter", "importer", "value"]].loc[(data.exporter != "World")&(data.importer == "World")].groupby(["year", "importer"]).sum().reset_index()
    data['iiso3c'] = data['importer'].apply(lambda x: countryname_to_iso3c[x])
    data = data.set_index(["year", "iiso3c", "importer"])  #.unstack("year")
    # data.columns = data.columns.droplevel()
    store.put("CountryImport", data, format='table')
    print store
    store.close()
    del rawdata, rawdata_hk
    gc.collect()

    #-Need Aggregation Version(?): No as they can be computed from this RAW DATA without reference to original RAW-#

if RAW_PRODUCT_YEARLY:
    print
    print "---> COMPUTING PRODUCT YEAR VALUES FROM RAW NBER DATASET"
    print

    rawdata, rawdata_hk = load_all_rawdata(start_year, end_year)
    #-Setup Store-#
    fn = "raw_nber_product_year-1962to2000.h5"
    store = pd.HDFStore(target_dir+fn, complevel=9, complib='zlib')
    #-Compute Values Using World-#
    data = rawdata.copy(deep=True)
    data = data[["year", "exporter", "importer", "sitc4", "value"]].loc[(data.exporter == "World")&(data.importer == "World")].groupby(["year", "sitc4"]).sum().reset_index()
    store.put("L4", data, format='table')
    for level in [3,2,1]:
        data["sitc%s"%level] = data["sitc%s"%(level+1)].apply(lambda x: x[0:level])
        data = data.groupby(["year", "sitc%s"%level]).sum().reset_index()
        store.put("L%s"%level, data, format='table')
    ##--!!--CHECK THIS THEN DELETE--!!-##
    #-Country Values not using World-#
    #-This method should be equivalent - check then delete!-#
    data = rawdata.copy(deep=True)
    data = data[["year", "exporter", "importer", "sitc4", "value"]].loc[(data.exporter != "World")&(data.importer != "World")].groupby(["year", "sitc4"]).sum().reset_index()
    store.put("L42", data, format='table')
    for level in [3,2,1]:
        data["sitc%s"%level] = data["sitc%s"%(level+1)].apply(lambda x: x[0:level])
        data = data.groupby(["year", "sitc%s"%level]).sum().reset_index()
        store.put("L%s2"%level, data, format='table')
    ##--!!--CHECK THIS THEN DELETE--!!-##
    print store
    store.close()
    del rawdata, rawdata_hk
    gc.collect()


#------------------------------------- OTHER YEARS --------------------------------------------#


#--------------#
#-1974 to 2000-#
#--------------# 

local_dir = "Y7400/"

if Y7400:

    print
    print "[INFO] Running Y7400 ..."

    #-Source Information-#

    print "---> Loading RAW Data <---"
    print
    fn = source_dir + "nber_year.h5"
    rawdata = load_raw_dataset(fn, 1974, 2000)
    rawdata = fix_raw_data(rawdata)                                 #Adjustments Made for FAQ
    print "---> Loading RAW HK-CHINA Adjustments <---"
    fn, start_year, end_year = (source_dir + "nber_supp_year.h5", 1988, 2000)
    rawdata_hk = load_raw_dataset(fn, start_year, end_year)

    #-Precomputed Data Options--#
    from pyeconlab.trade.dataset.NBERWTF.meta import IntertemporalProducts #Should make a Local Static Copy
    ICP = IntertemporalProducts().IC7400

    from dataset_construct_nber_options import DATA_DESCRIPTION, DATA_OPTIONS           #-Update to Pull from PyEconLab?-#
 
    DATA_TYPES = []
    if Y7400Trade: DATA_TYPES.append('trade')
    if Y7400Export: DATA_TYPES.append('export')
    if Y7400Import: DATA_TYPES.append('import')

    #-Construct SITC R2 Level 4,3,2 Trade Datasets-#
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

    for level in [4,3,2,1]:
        print
        print "---> [74to00] COMPUTING SITC REVISION 2 LEVEL %s DATASETS <---" % level
        print
        for data_type in DATA_TYPES: 
            #-Setup Store-#
            fn = "nber-%s-sitcr2l%s-1974to2000.h5" % (data_type,level)                             #-Write File: {{ source }}-{{ flow }}-{{ classification }}-{{ years }}.h5-#
            store = pd.HDFStore(target_dir+local_dir+fn, complevel=9, complib='zlib')
            #-Compute Datasets-#
            for dataset in sorted(DATA_OPTIONS.keys()):
                print
                print "[SITCR2L4] Computing Dataset %s for %s" % (dataset, data_type)
                #-IF Adjust Hong Kong Data then Add Data to the Tuple-#
                if DATA_OPTIONS[dataset]['adjust_hk'] == True: 
                    DATA_OPTIONS[dataset]['adjust_hk'] = (True, rawdata_hk)                     #Add Data for construct_sitcr2l3 function, then remove so not saved as an attribute
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = (False, None)
                #-Intertemporal Product Codes-#
                if DATA_OPTIONS[dataset]['intertemp_productcode']:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = (True, ICP[level])
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = (False, None)
                #-Compute Data-#
                data = construct_sitcr2(rawdata.copy(deep=True), data_type=data_type, level=level, values_only=True, **DATA_OPTIONS[dataset])
                store.put(dataset, data, format='table')
                #-Remove Hong Kong Data-#
                if DATA_OPTIONS[dataset]['adjust_hk'][0] == True:
                    DATA_OPTIONS[dataset]['adjust_hk'] = True
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = False
                if DATA_OPTIONS[dataset]['intertemp_productcode'][0] == True:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = True
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = False
                store.get_storer(dataset).attrs.options = DATA_OPTIONS[dataset]
                store.get_storer(dataset).attrs.data_type = data_type
                store.get_storer(dataset).attrs.description = DATA_DESCRIPTION[dataset]
            print store
            #-Close-#
            store.close()

    del rawdata
    gc.collect()



#--------------#
#-1984 to 2000-#
#--------------#

local_dir = "Y8400/"

if Y8400:

    print
    print "[INFO] Running Y8400 ..."

    #-Precomputed Data Options--#
    from pyeconlab.trade.dataset.NBERWTF.meta import IntertemporalProducts #Should make a Local Static Copy
    ICP = IntertemporalProducts().IC8400

    #-Source Information-#
    #~~~~~~~~~~~~~~~~~~~~#

    print "---> Loading RAW Data <---"
    print
    fn = source_dir + "nber_year.h5"
    rawdata = load_raw_dataset(fn, 1984, 2000)
    rawdata = fix_raw_data(rawdata)                                 #Adjustments Made for FAQ
    print "---> Loading RAW HK-CHINA Adjustments <---"
    fn, start_year, end_year = (source_dir + "nber_supp_year.h5", 1988, 2000)
    rawdata_hk = load_raw_dataset(fn, start_year, end_year)

    #-Construct SITC R2 Level 4,3,2 Trade Datasets-#
    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

    DATA_TYPES = []
    if Y8400Trade: DATA_TYPES.append('trade')
    if Y8400Export: DATA_TYPES.append('export')
    if Y8400Import: DATA_TYPES.append('import')

    for level in [4,3,2,1]:
        print
        print "---> [84to00] COMPUTING SITC REVISION 2 LEVEL %s DATASETS <---" % level
        print
        for data_type in DATA_TYPES: 
            #-Setup Store-#
            fn = "nber-%s-sitcr2l%s-1984to2000.h5" % (data_type,level)                             #-Write File: {{ source }}-{{ flow }}-{{ classification }}-{{ years }}.h5-#
            store = pd.HDFStore(target_dir+local_dir+fn, complevel=9, complib='zlib')
            #-Compute Datasets-#
            for dataset in sorted(DATA_OPTIONS.keys()):
                print
                print "[SITCR2L4] Computing Dataset %s for %s" % (dataset, data_type)
                #-IF Adjust Hong Kong Data then Add Data to the Tuple-#
                if DATA_OPTIONS[dataset]['adjust_hk'] == True: 
                    DATA_OPTIONS[dataset]['adjust_hk'] = (True, rawdata_hk)                     #Add Data for construct_sitcr2l3 function, then remove so not saved as an attribute
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = (False, None)
                #-Intertemporal Product Codes-#
                if DATA_OPTIONS[dataset]['intertemp_productcode']:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = (True, ICP[level])
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = (False, None)
                #-Compute Data-#
                data = construct_sitcr2(rawdata.copy(deep=True), data_type=data_type, level=level, values_only=True, **DATA_OPTIONS[dataset])
                store.put(dataset, data, format='table')
                #-Remove Hong Kong Data-#
                if DATA_OPTIONS[dataset]['adjust_hk'][0] == True:
                    DATA_OPTIONS[dataset]['adjust_hk'] = True
                else:
                    DATA_OPTIONS[dataset]['adjust_hk'] = False
                if DATA_OPTIONS[dataset]['intertemp_productcode'][0] == True:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = True
                else:
                    DATA_OPTIONS[dataset]['intertemp_productcode'] = False
                store.get_storer(dataset).attrs.options = DATA_OPTIONS[dataset]
                store.get_storer(dataset).attrs.data_type = data_type
                store.get_storer(dataset).attrs.description = DATA_DESCRIPTION[dataset]
            print store
            #-Close-#
            store.close()

    del rawdata
    gc.collect()