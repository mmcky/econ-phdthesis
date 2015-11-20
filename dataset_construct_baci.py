"""
Compute BACI Datasets
=====================

Author: Matthew McKay (mamckay@gmail.com)

Filename rules: {{ source }}-{{ flow }}-{{ classification }}-{{ years }}-{{ raw/cleaned }}-{{ type }}-{{ id }}
				source-flow-classification-years-raw/cleaned-type-id

Supporting Scripts
------------------
1. dataset-info.py 			Contains Information about the relevant datasets
2. dataset-compile-raw.py 	Compiles RAW data files to a single dataset file

Sources
-------
2. 	baci
	md5: e988b6544563675492b59f397a8cb6bb
	notes: BACI Trade RAW Dataset [HS96]

Supporting Files
----------------
TBD

"""

import numpy as np
import pandas as pd
from pyeconlab.util import concord_data
import gc

#----------#
#- BACI96 -#
#----------#

#-Dataset Information-#
from dataset_info import TARGET_RAW_DIR, TARGET_DATASET_DIR, YEARS

#-Setup Local Environment-#
#~~~~~~~~~~~~~~~~~~~~~~~~~#
SOURCE_DIR = TARGET_RAW_DIR['baci96']
TARGET_DIR = TARGET_DATASET_DIR['baci96']
start_year, end_year = YEARS['baci96']

#-Helper Functions-#
#~~~~~~~~~~~~~~~~~~#

def load_raw_dataset(fn, start_year, end_year, verbose=True):
    """
    Load Raw BACI Dataset
    """
    data = pd.DataFrame()
    for year in range(start_year, end_year+1, 1):
        print "Loading Year: %s" % year
        data = data.append(pd.read_hdf(fn, "Y%s"%year))
    if verbose: print data.t.unique()
    return data

#-Source Information-#
#~~~~~~~~~~~~~~~~~~~~#
print
print "---> Loading RAW Data <---"
fn = SOURCE_DIR + "baci_year.h5"
rawdata = load_raw_dataset(fn, start_year, end_year)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#-Construct SITC Revision 2 Datasets-#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

CONSTRUCT_SITC_DATASETS = True

if CONSTRUCT_SITC_DATASETS:

    from pyeconlab.trade.dataset.CEPIIBACI import SITC_DATASET_DESCRIPTION, SITC_DATASET_OPTIONS
    from pyeconlab.trade.dataset.CEPIIBACI import construct_sitc

    LEVELS = [1,2,3,4,5]
    DATA_TYPES = ["trade", "export", "import"]

    for level in LEVELS:
        #-Import this as a Function from pyeconlab-#
        print
        print "---> COMPUTING SITC REVISION 2 LEVEL %s DATASETS <---" % level
        print
        for data_type in DATA_TYPES:
            #-Setup Store-#
            fn = "baci-%s-sitcr2l%s-%sto%s.h5" % (data_type, level, start_year, end_year)                                    #-Write File: {{ source }}-{{ flow }}-{{ classification }}-{{ years }}.h5-#
            store = pd.HDFStore(TARGET_DIR+fn, complevel=9, complib='zlib')
            #-Compute Datasets-#
            for dataset in sorted(SITC_DATASET_OPTIONS.keys()):
                print "[SITCR2L%s] Computing Dataset %s for %s" % (level, dataset, data_type)
                #-Compute Data-#
                #INTERFACE: def construct_sitc(data, data_classification, data_type, level, revision, check_concordance=True, adjust_units=False, concordance_institution="un", multiindex=True, verbose=True):#
                data = construct_sitc(rawdata.copy(deep=True), data_classification="HS96", data_type=data_type, level=level, revision=2, **SITC_DATASET_OPTIONS[dataset])
                store.put(dataset, data, format='table')
                store.get_storer(dataset).attrs.options = SITC_DATASET_OPTIONS[dataset]
                store.get_storer(dataset).attrs.data_type = data_type
                store.get_storer(dataset).attrs.description = SITC_DATASET_DESCRIPTION[dataset]
                print
            #-Close-#
            store.close()
        del data
        gc.collect()

#----------#
#-RAW DATA-#
#----------#

RAW_DATA = True
RAW_WORLD_YEARLY = True       
RAW_COUNTRY_YEARLY = True     
RAW_PRODUCT_YEARLY = True

#-Adjust RAW Data to have common interface names-#
stdnames = {'t' : 'year', 'i' : 'eiso3n', 'j' : 'iiso3n', 'v' : 'value', 'q' : 'quantity'}
rawdata = rawdata.rename_axis(stdnames, axis=1)

if RAW_DATA:
    print
    print "---> SAVING RAW DATA (WITH STANDARD COLUMNS NAMES) <---"
    print
    fn = "raw_baci_hs96-1998-2012.h5"
    store = pd.HDFStore(TARGET_DIR+fn, complevel=9, complib='zlib')
    store.put('RAW', rawdata, format='table')
    store.close()


if RAW_WORLD_YEARLY:
    
    ## Shold this be filtered through a countries only filter? ##

    print
    print "---> COMPUTING WORLD YEARLY VALUES FROM RAW BACI DATASET <---"
    print 
    fn = "raw_baci_world_yearly-1998to2012.h5"
    store = pd.HDFStore(TARGET_DIR+fn, complevel=9, complib='zlib')
    world_values = rawdata[["year", "value"]].groupby(["year"]).sum()
    store.put('World', world_values, format='table')
    store.close()
    del world_values
    gc.collect()

if RAW_COUNTRY_YEARLY:
    print
    print "---> COMPUTING COUNTRY YEARLY VALUES FROM RAW BACI DATASET <---"
    print
    #-Setup Store-#
    fn = "raw_baci_country_year-1998to2012.h5"
    store = pd.HDFStore(TARGET_DIR+fn, complevel=9, complib='zlib')
    #-Import ISO3C-#
    from pyeconlab.trade.dataset.CEPIIBACI.meta import hs96_iso3n_to_iso3c
    rawdata['eiso3c'] = rawdata['eiso3n'].apply(lambda x: concord_data(hs96_iso3n_to_iso3c, x, issue_error=np.nan))     #Is this Complete?
    rawdata['iiso3c'] = rawdata['iiso3n'].apply(lambda x: concord_data(hs96_iso3n_to_iso3c, x, issue_error=np.nan))     #Is this Complete?
    #-Country Exports-#
    exports = rawdata[["year", "eiso3c", "value"]].groupby(["year", "eiso3c"]).sum().reset_index()
    store.put("CountryExports", exports, format='table')
    #-Country Imports-#
    imports = rawdata[["year", "iiso3c", "value"]].groupby(["year", "iiso3c"]).sum().reset_index()
    store.put("CountryImports", imports, format='table')
    store.close()
    del exports
    del imports
    gc.collect()

if RAW_PRODUCT_YEARLY:
    
    ## Shold this be filtered through a countries only filter? ##

    print
    print "---> COMPUTING PRODUCT YEAR VALUES FROM RAW BACI DATASET (HS and SITC)"
    print
    #-Setup Store-#
    fn = "raw_baci_product_year-1998to2012.h5"
    store = pd.HDFStore(TARGET_DIR+fn, complevel=9, complib='zlib')
    #-HS-#
    for level in [6,5,4,3,2,1]:
        print "Computing HS%s Product Year Values ..."%level
        data = rawdata.copy(deep=True)
        if level != 6:
            data["hs%s"%level] = data["hs6"].apply(lambda x: x[0:level])
        product_trade = data[["year", "hs%s"%level, "value"]].groupby(["year", "hs%s"%level]).sum().reset_index()
        store.put("HS96L%s"%level, product_trade, format='table')
        del data
        del product_trade
        gc.collect()

    #-SITC-#
    from pyeconlab.trade.concordance import HS_To_SITC
    concordance = HS_To_SITC(hs="HS96", sitc="SITCR2", hs_level=6, sitc_level=5, source_institution='un', verbose=True).concordance
    for level in [5,4,3,2,1]:
        print "Computing SITC%s Product Year Values ..."%level
        data = rawdata.copy(deep=True)
        data['sitc5'] = data['hs6'].apply(lambda x: concord_data(concordance, x, issue_error=np.nan))
        if level != 5:
            data["sitc%s"%level] = data["sitc5"].apply(lambda x: x[0:level])
        product_trade = data[["year", "sitc%s"%level, "value"]].groupby(["year", "sitc%s"%level]).sum().reset_index()
        store.put("SITCR2L%s"%level, product_trade, format='table')
        del data
        del product_trade
        gc.collect()
    store.close()
    