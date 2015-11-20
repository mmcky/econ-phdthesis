"""

Construct Combined NBER and BACI Datasets

Step 1: Produce Harmonised BACI Datasets
Step 2: Join NBER and BACI Datasets Together (with merged years = "average")

Supporting Files
----------------
static/intertemporal.py or from pyeconlab.trade.dataset.NBERWTF.meta import intertemporal

"""

from __future__ import division

import glob
import re
import gc
import pandas as pd
import numpy as np
#-Local Repo Imports-#
from dataset_info import RESULTS_DIR, TARGET_DATASET_DIR
from pyeconlab.util import concord_data

#---------#
#-CONTROL-#
#---------#

Y6200 = True
Y7400 = True
Y8400 = True   

#------------------#
#-Helper Functions-#
#------------------#

def harmonise_data(df, data_type, level, intertemp_productcode=(False, None), intertemp_cntrycode=False, drop_incp_cntrycode=False, adjust_units=False, verbose=True):
        """
        Construct a Harmonised Dataset between NBER and BACI

        Parameters
        ----------
        df                  :   DataFrame
                                Pandas DataFrame containing the raw data
        data_type           :   str
                                Specify what type of data 'trade', 'export', 'import'
        level               :   int
                                Specify Level of Final dataset (i.e. SITC Level 1, 2, 3, or 4)
        intertemp_productcode : Tuple(bool, dict), optional(default=False, None)
                                Apply an Intertemporal Product Code System drop a conversion dictionary (IC["drop"] = [], IC["collapse"] = [])
                                Note this will override the drop_nonsitcr2 option
        intertemp_cntrycode :   bool, optional(default=False)
                                Generate Intertemporal Consistent Country Units (from meta)
        drop_incp_cntrycode :   bool, optional(default=False)
                                Drop Incomplete Country Codes (from meta)
        adjust_units        :   bool, optional(default=False)
                                Adjust units by a factor of 1000 to specify in $'s

        Notes
        -----
            1. This consists of code snippets from construct_dataset_sitcr2.py

        """
        
        #-Intertemporal ProductCodes-#
        if intertemp_productcode[0]:
            if verbose: print "[INFO] Computing Intertemporally Consistent ProductCodes ..."
            #-This Method relies on meta data computed by pyeconlab nberwtf constructor-#
            IC = intertemp_productcode[1]               #Dict("drop" and "collapse" code lists)
            #-Drop Codes-#
            drop_codes = IC["drop"]
            if verbose: 
                print "Dropping the following productcodes ..."
                print drop_codes
            keep_codes = set(df['sitc%s'%level].unique()).difference(set(drop_codes))
            df = df.loc[df["sitc%s"%level].isin(keep_codes)].copy(deep=True)
            #-Collapse Codes-#
            collapse_codes = IC["collapse"]
            if verbose:
                print "Collapsing the following productcodes ..."
                print collapse_codes
            collapse_codes = {x[0:level-1] for x in collapse_codes}     #-Simplify Computations-#
            for code in collapse_codes:
                df["sitc%s"%level] = df["sitc%s"%level].apply(lambda x: code if x[0:level-1] == code else x)  #code+'0'
            #-Recodes-#
            recodes = IC["recode"]
            recode_codes = recodes.keys()
            if verbose: 
                print "Recoding the following productcodes ..."
                print recode_codes
            for code in recode_codes:
                df["sitc%s"%level] = df["sitc%s"%level].apply(lambda x: recodes[x] if x in recode_codes else x)
            #-Reset Collapsed Codes-#
            df = df.groupby(list(df.columns.drop("value"))).sum()
            df = df.reset_index()

        #-Adjust Country Codes to be Intertemporally Consistent-#
        if intertemp_cntrycode:
            #-Export-#
            if data_type == 'export' or data_type == 'exports':
                if verbose: print "[INFO] Imposing dynamically consistent eiso3c recodes across 1962-2000"
                df['eiso3c'] = df['eiso3c'].apply(lambda x: concord_data(iso3c_recodes_for_1962_2000, x, issue_error=False))    #issue_error = false returns x if no match
                df = df[df['eiso3c'] != '.']
                df = df.groupby(['year', 'eiso3c', 'sitc%s'%level]).sum().reset_index()
            #-Import-#
            elif data_type == 'import' or data_type == 'imports':
                if verbose: print "[INFO] Imposing dynamically consistent iiso3c recodes across 1962-2000"
                df['iiso3c'] = df['iiso3c'].apply(lambda x: concord_data(iso3c_recodes_for_1962_2000, x, issue_error=False))    #issue_error = false returns x if no match
                df = df[df['iiso3c'] != '.']
                df = df.groupby(['year', 'iiso3c', 'sitc%s'%level]).sum().reset_index()
            #-Trade-#
            else:
                if verbose: print "[INFO] Imposing dynamically consistent iiso3c and eiso3c recodes across 1962-2000"
                df['iiso3c'] = df['iiso3c'].apply(lambda x: concord_data(iso3c_recodes_for_1962_2000, x, issue_error=False))    #issue_error = false returns x if no match
                df['eiso3c'] = df['eiso3c'].apply(lambda x: concord_data(iso3c_recodes_for_1962_2000, x, issue_error=False))    #issue_error = false returns x if no match
                df = df[df['iiso3c'] != '.']
                df = df[df['eiso3c'] != '.']
                df = df.groupby(['year', 'eiso3c', 'iiso3c', 'sitc%s'%level]).sum().reset_index()
        
        #-Drop Incomplete Country Codes-#
        if drop_incp_cntrycode:
            if verbose: print "[INFO] Dropping countries with incomplete data across 1962-2000"
            #-Export-#
            if data_type == 'export' or data_type == 'exports':
                df['eiso3c'] = df['eiso3c'].apply(lambda x: concord_data(incomplete_iso3c_for_1962_2000, x, issue_error=False))     #issue_error = false returns x if no match
                df = df[df['eiso3c'] != '.']
            #-Import-#
            elif data_type == 'import' or data_type == 'imports':
                df['iiso3c'] = df['iiso3c'].apply(lambda x: concord_data(incomplete_iso3c_for_1962_2000, x, issue_error=False))     #issue_error = false returns x if no match
                df = df[df['iiso3c'] != '.']
            #-Trade-#
            else:
                df['iiso3c'] = df['iiso3c'].apply(lambda x: concord_data(incomplete_iso3c_for_1962_2000, x, issue_error=False))     #issue_error = false returns x if no match
                df['eiso3c'] = df['eiso3c'].apply(lambda x: concord_data(incomplete_iso3c_for_1962_2000, x, issue_error=False))     #issue_error = false returns x if no match
                df = df[df['iiso3c'] != '.']
                df = df[df['eiso3c'] != '.']
            df = df.reset_index()
            del df['index']
       
        #-Adjust Units from 1000's to $'s-#
        if adjust_units:
            if verbose: print "[INFO] Adjusting 'value' units to $'s"
            df['value'] = df['value']*1000         #Default: Keep in 1000's
        
        #-Return Dataset-#
        if verbose: print "[INFO] Finished Computing Harmonised Dataset (%s) ..." % (data_type) 
        return df

def join_values(row):
    """ Join Rows and Average if both values are not np.nan """
    if np.isnan(row["value_x"]) and np.isnan(row["value_y"]):
        return np.nan
    elif np.isnan(row["value_x"]):
        return row["value_y"]
    elif np.isnan(row["value_y"]):
        return row["value_x"]
    else:
        return (row["value_x"] + row["value_y"]) / 2


#------------------------#
#-NBER DATA 1962 to 2000-#
#------------------------#

if Y6200:

    #-Target Dir-#
    TARGET_DIR = TARGET_DATASET_DIR["nberbaci96"]
    #-NBER-#
    NBER_DIR = TARGET_DATASET_DIR["nber"]
    NBER_STORES = glob.glob(NBER_DIR + "*.h5")
    NBER_DATASETS = [x for x in NBER_STORES if x.split("/")[-1][0:3] != "raw"]
        # NBER_TRADE = [x for x in NBER_DATASETS if re.search("trade", x.split("/")[-1])]
        # NBER_EXPORT = [x for x in NBER_DATASETS if re.search("export", x.split("/")[-1])]
        # NBER_IMPORT = [x for x in NBER_DATASETS if re.search("import", x.split("/")[-1])]
    NBER_RAW = [x for x in NBER_STORES if x.split("/")[-1][0:3] == "raw"]   
    #-BACI-#
    BACI_DIR = TARGET_DATASET_DIR["baci96"]
    BACI_STORES = glob.glob(BACI_DIR + "*.h5")
    BACI_DATASETS = [x for x in BACI_STORES if x.split("/")[-1][0:3] != "raw"]
    BACI_RAW = [x for x in BACI_STORES if x.split("/")[-1][0:3] == "raw"] 


    #-----------------------------------#
    #-Harmonise BACI with NBER DATASETS-#
    #-----------------------------------#

    #-Dataset Options-#
    #-Drop Items Not Applicable for Harmonisation-#
    #-Notes: This Renders, A, B, C and D to be exactly the same. Should we compute them all?-#
    from dataset_construct_nber_options import DATA_OPTIONS
    for dataset in DATA_OPTIONS.keys():
        del DATA_OPTIONS[dataset]['AX']
        del DATA_OPTIONS[dataset]['dropAX'] 
        del DATA_OPTIONS[dataset]['adjust_hk']
        del DATA_OPTIONS[dataset]['sitcr2']
        del DATA_OPTIONS[dataset]['drop_nonsitcr2']
        del DATA_OPTIONS[dataset]['source_institution']

    #-Intertemporal ProductCode Data-#
    from pyeconlab.trade.dataset.NBERWTF.meta import countryname_to_iso3c, iso3c_recodes_for_1962_2000, incomplete_iso3c_for_1962_2000 
    from pyeconlab.trade.dataset.NBERWTF.meta import IntertemporalProducts      #Should Make a Local Static Version
    ICP = IntertemporalProducts().IC6200

    #-Harmonisation Work-#

    for fl in sorted(BACI_DATASETS):
        print "[INFO] Computing Harmonised Equivalents for file: %s" % fl
        fln = fl.split("/")[-1].split(".")[0]
        source, data_type, classlevel, years = fln.split("-")
        product_level = int(classlevel.split("l")[1])
        print "[INFO] Infering Data Type Level = %s" % data_type
        print "[INFO] Infering Product Level = %s" % product_level
        if product_level == 5:
            print "Skipping Level 5 becuase NBER doesn't contain sitc5 values"
            continue
        #-Read Converted SITC BACI Data-#
        baci_data = pd.read_hdf(fl, key="A")              
        #-Setup New Store-#
        fln = fln.split(".")[0] + "-harmonised-nber.h5"
        store = pd.HDFStore(BACI_DIR + "harmonised/" + fln, complevel=9, complib='zlib')
        #-Corresponding NBER File-#
        nber_fl = NBER_DIR + fln.replace("baci", "nber").replace("1998to2012", "1962to2000").replace("-harmonised-nber", "")
        nber_store = pd.HDFStore(nber_fl)
        nber_keys = nber_store.keys()
        for dataset in sorted(nber_keys):
            dataset = dataset.replace("/","")
            print "[INFO] Processing Dataset: %s ..." % dataset
            #-Add Data to Option-#
            if DATA_OPTIONS[dataset]['intertemp_productcode']:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = (True, ICP[product_level])
            else:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = (False, None)
            #-Core
            if type(baci_data.index) == pd.MultiIndex:
                baci_data = baci_data.reset_index()
            baci_adjust = harmonise_data(baci_data.copy(deep=True), data_type, product_level, **DATA_OPTIONS[dataset])
            #-Return Option State-#
            if DATA_OPTIONS[dataset]['intertemp_productcode'][0]:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = True
            else:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = False
            #-Filter Out Countries Not Represented in the NBER Dataset-#
            print "[INFO] Obtaining Countries represented in the equivalent nber dataset ..."
            nber_data = nber_store[dataset]
            if data_type == "export" or data_type == "trade":
                nber_eiso3c = nber_data.eiso3c.unique()
                baci_adjust = baci_adjust.loc[baci_adjust.eiso3c.isin(nber_eiso3c)]
                baci_adjust = baci_adjust.reset_index()
                del baci_adjust["index"]
            if data_type == "import" or data_type == "trade":
                nber_iiso3c = nber_data.iiso3c.unique()
                baci_adjust = baci_adjust.loc[baci_adjust.iiso3c.isin(nber_iiso3c)]
                baci_adjust = baci_adjust.reset_index()
                del baci_adjust["index"]
            #-Write to Store-#
            store.put(dataset, baci_adjust, format='table')
            del nber_data
            del baci_adjust
            gc.collect()
        nber_store.close()
        store.close()
        del baci_data
        gc.collect()

    #-Harmonise NBER BACI with CEPII BACI-#
    #-334 is only concorded at the 3-digit level in the HS-SITC-#
    #-3341, 3342, 3342, 3344 => 334-#
    
    #-Join Datasets-#

    for nberfl in sorted(NBER_DATASETS):
        #-nber-#
        print "[INFO] Merging Harmonised Equivalents for file: %s" % nberfl
        nberfln = nberfl.split("/")[-1].split(".")[0]
        source, data_type, classlevel, years = nberfln.split("-")
        product_level = int(classlevel.split("l")[1])
        print "[INFO] Infering Data Type Level = %s" % data_type
        print "[INFO] Infering Product Level = %s" % product_level
        #-baci-#
        bacifln = "baci"+"-"+data_type+"-"+classlevel+"-"+"1998to2012-harmonised-nber.h5"
        bacifl = BACI_DIR + "harmonised/" + bacifln
        print "[INFO] Pairing with Harmonised BACI data in file: %s" % bacifl
        #-nberbaci-#
        fln = "nberbaci"+"-"+data_type+"-"+classlevel+"-"+"1962to2012-harmonised.h5"
        store = pd.HDFStore(TARGET_DIR + fln, complevel=9, complib='zlib')
        for dataset in sorted(DATA_OPTIONS.keys()):
            print "[INFO] Processing Dataset: %s" % dataset
            #-Check Special Case of intertemp_productcode at Level 1-#
            if DATA_OPTIONS[dataset]['intertemp_productcode'] and product_level == 1:
                print "[INFO] This operation cannot occur at this level of disaggregation ... continuing"
                continue
            nber_data = pd.read_hdf(nberfl, key=dataset)
            #-Special Oil and Petroleum Adjustment-#
            if product_level > 3:
                print "[INFO] Recoding Oil and Petroleum to 334 due to BACI HS96-SITCR2 Concordance"
                sitc_code = "sitc%s"%product_level
                nber_data[sitc_code] = nber_data[sitc_code].apply(lambda x: "334" if x[0:3] == "334" else x)
                idx = set(nber_data.columns)
                idx.remove("value")
                idx= list(idx)
                nber_data = nber_data.groupby(idx).sum().reset_index()
            baci_data = pd.read_hdf(bacifl, key=dataset)
            #-Merge-#
            idx = set(nber_data.columns)
            idx.remove("value")
            idx= list(idx)
            nberbaci = nber_data.merge(baci_data, how='outer', on=idx)
            nberbaci['value'] = nberbaci[['value_x', 'value_y']].apply(lambda row: join_values(row), axis=1)
            del nberbaci["value_x"]
            del nberbaci["value_y"]
            store.put(dataset, nberbaci, format='table')
            del nber_data
            del baci_data
            gc.collect()
        store.close()



#------------------------------------------------------- OTHER YEARS ------------------------------------------------ #


#------------------------#
#-NBER DATA 1974 to 2000-#
#------------------------#

if Y7400:

    print 
    print "[INFO-7400] Merging NBER Y7400 Datasets with BACI ...."

    #-Target Dir-#
    LOCAL_DIR = "Y7400/"
    TARGET_DIR = TARGET_DATASET_DIR["nberbaci96"]+LOCAL_DIR

    #-NBER-#
    NBER_DIR = TARGET_DATASET_DIR["nber"]+LOCAL_DIR
    NBER_STORES = glob.glob(NBER_DIR + "*.h5")
    NBER_DATASETS = [x for x in NBER_STORES if x.split("/")[-1][0:3] != "raw"]

    #-BACI-#
    BACI_DIR = TARGET_DATASET_DIR["baci96"]
    BACI_STORES = glob.glob(BACI_DIR + "*.h5")
    BACI_DATASETS = [x for x in BACI_STORES if x.split("/")[-1][0:3] != "raw"]


    #-----------------------------------#
    #-Harmonise BACI with NBER DATASETS-#
    #-----------------------------------#

    #-Drop Items Not Applicable for Harmonisation-#
    #-Notes: This Renders, A, B, C and D to be exactly the same. Should we compute them all?-#
    from dataset_construct_nber_options import DATA_OPTIONS
    try:
        for dataset in DATA_OPTIONS.keys():
            del DATA_OPTIONS[dataset]['AX']
            del DATA_OPTIONS[dataset]['dropAX'] 
            del DATA_OPTIONS[dataset]['adjust_hk']
            del DATA_OPTIONS[dataset]['sitcr2']
            del DATA_OPTIONS[dataset]['drop_nonsitcr2']
            del DATA_OPTIONS[dataset]['source_institution']
    except:
        print "This has already been done in code above"

    #-Import META DATA-#
    #-Note Country recodes are currently using 1962 to 2000 recodes for the time being-#
    from pyeconlab.trade.dataset.NBERWTF.meta import countryname_to_iso3c, iso3c_recodes_for_1962_2000, incomplete_iso3c_for_1962_2000 
    from pyeconlab.trade.dataset.NBERWTF.meta import IntertemporalProducts      
    ICP = IntertemporalProducts().IC7400

    #--------------------#
    #-Harmonise Datasets-#
    #--------------------#

    for fl in sorted(BACI_DATASETS):
        print "[INFO] Computing Harmonised Equivalents for file: %s" % fl
        fln = fl.split("/")[-1].split(".")[0]
        source, data_type, classlevel, years = fln.split("-")
        product_level = int(classlevel.split("l")[1])
        print "[INFO] Infering Data Type Level = %s" % data_type
        print "[INFO] Infering Product Level = %s" % product_level
        if product_level == 5:
            print "Skipping Level 5 because NBER doesn't contain sitc5 values"
            continue
        #-Read Converted SITC BACI Data-#
        baci_data = pd.read_hdf(fl, key="A")              
        #-Setup New Store-#
        fln = fln.split(".")[0] + "-harmonised-nber.h5"
        store = pd.HDFStore(BACI_DIR + "harmonised/" + LOCAL_DIR + fln, complevel=9, complib='zlib')
        #-Corresponding NBER File-#
        nber_fl = NBER_DIR + fln.replace("baci", "nber").replace("1998to2012", "1974to2000").replace("-harmonised-nber", "")
        nber_store = pd.HDFStore(nber_fl)
        nber_keys = nber_store.keys()
        for dataset in sorted(nber_keys):
            dataset = dataset.replace("/","")
            print "[INFO] Processing Dataset: %s ..." % dataset
            #-Add Data to Option-#
            if DATA_OPTIONS[dataset]['intertemp_productcode']:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = (True, ICP[product_level])
            else:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = (False, None)
            #-Core
            if type(baci_data.index) == pd.MultiIndex:
                baci_data = baci_data.reset_index()
            baci_adjust = harmonise_data(baci_data.copy(deep=True), data_type, product_level, **DATA_OPTIONS[dataset])
            #-Return Option State-#
            if DATA_OPTIONS[dataset]['intertemp_productcode'][0]:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = True
            else:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = False
            #-Filter Out Countries Not Represented in the NBER Dataset-#
            print "[INFO] Obtaining Countries represented in the equivalent nber dataset ..."
            nber_data = nber_store[dataset]
            if data_type == "export" or data_type == "trade":
                nber_eiso3c = nber_data.eiso3c.unique()
                baci_adjust = baci_adjust.loc[baci_adjust.eiso3c.isin(nber_eiso3c)]
                baci_adjust = baci_adjust.reset_index()
                del baci_adjust["index"]
            if data_type == "import" or data_type == "trade":
                nber_iiso3c = nber_data.iiso3c.unique()
                baci_adjust = baci_adjust.loc[baci_adjust.iiso3c.isin(nber_iiso3c)]
                baci_adjust = baci_adjust.reset_index()
                del baci_adjust["index"]
            #-Write to Store-#
            store.put(dataset, baci_adjust, format='table')
            del nber_data
            del baci_adjust
            gc.collect()
        nber_store.close()
        store.close()
        del baci_data
        gc.collect()


    #---------------#
    #-Join Datasets-#
    #---------------#

    for nberfl in sorted(NBER_DATASETS):
        #-nber-#
        print "[INFO] Merging Harmonised Equivalents for file: %s" % nberfl
        nberfln = nberfl.split("/")[-1].split(".")[0]
        source, data_type, classlevel, years = nberfln.split("-")
        product_level = int(classlevel.split("l")[1])
        print "[INFO] Infering Data Type Level = %s" % data_type
        print "[INFO] Infering Product Level = %s" % product_level
        #-baci-#
        bacifln = "baci"+"-"+data_type+"-"+classlevel+"-"+"1998to2012-harmonised-nber.h5"
        bacifl = BACI_DIR + "harmonised/" + LOCAL_DIR + bacifln
        print "[INFO] Pairing with Harmonised BACI data in file: %s" % bacifl
        #-nberbaci-#
        fln = "nberbaci"+"-"+data_type+"-"+classlevel+"-"+"1974to2012-harmonised.h5"
        store = pd.HDFStore(TARGET_DIR + fln, complevel=9, complib='zlib')
        for dataset in sorted(DATA_OPTIONS.keys()):
            print "[INFO] Processing Dataset: %s" % dataset
            #-Check Special Case of intertemp_productcode at Level 1-#
            if DATA_OPTIONS[dataset]['intertemp_productcode'] and product_level == 1:
                print "[INFO] This operation cannot occur at this level of disaggregation ... continuing"
                continue
            nber_data = pd.read_hdf(nberfl, key=dataset)
            #-Special Oil and Petroleum Adjustment-#
            if product_level > 3:
                print "[INFO] Recoding Oil and Petroleum to 334 due to BACI HS96-SITCR2 Concordance"
                sitc_code = "sitc%s"%product_level
                nber_data[sitc_code] = nber_data[sitc_code].apply(lambda x: "334" if x[0:3] == "334" else x)
                idx = set(nber_data.columns)
                idx.remove("value")
                idx= list(idx)
                nber_data = nber_data.groupby(idx).sum().reset_index()
            baci_data = pd.read_hdf(bacifl, key=dataset)
            #-Merge-#
            idx = set(nber_data.columns)
            idx.remove("value")
            idx= list(idx)
            nberbaci = nber_data.merge(baci_data, how='outer', on=idx)
            nberbaci['value'] = nberbaci[['value_x', 'value_y']].apply(lambda row: join_values(row), axis=1)
            del nberbaci["value_x"]
            del nberbaci["value_y"]
            store.put(dataset, nberbaci, format='table')
            del nber_data
            del baci_data
            gc.collect()
        store.close()


##--!!--REQUIRES TESTING BELOW--!!--##

#------------------------#
#-NBER DATA 1984 to 2000-#
#------------------------#

if Y8400:

    print 
    print "[INFO-8400] Merging NBER Y8400 Datasets with BACI ...."

    #-Target Dir-#
    LOCAL_DIR = "Y8400/"
    TARGET_DIR = TARGET_DATASET_DIR["nberbaci96"]+LOCAL_DIR

    #-NBER-#
    NBER_DIR = TARGET_DATASET_DIR["nber"]+LOCAL_DIR
    NBER_STORES = glob.glob(NBER_DIR + "*.h5")
    NBER_DATASETS = [x for x in NBER_STORES if x.split("/")[-1][0:3] != "raw"]

    #-BACI-#
    BACI_DIR = TARGET_DATASET_DIR["baci96"]
    BACI_STORES = glob.glob(BACI_DIR + "*.h5")
    BACI_DATASETS = [x for x in BACI_STORES if x.split("/")[-1][0:3] != "raw"]


    #-----------------------------------#
    #-Harmonise BACI with NBER DATASETS-#
    #-----------------------------------#

    #-Drop Items Not Applicable for Harmonisation-#
    #-Notes: This Renders, A, B, C and D to be exactly the same. Should we compute them all?-#
    from dataset_construct_nber_options import DATA_OPTIONS
    try:
        for dataset in DATA_OPTIONS.keys():
            del DATA_OPTIONS[dataset]['AX']
            del DATA_OPTIONS[dataset]['dropAX'] 
            del DATA_OPTIONS[dataset]['adjust_hk']
            del DATA_OPTIONS[dataset]['sitcr2']
            del DATA_OPTIONS[dataset]['drop_nonsitcr2']
            del DATA_OPTIONS[dataset]['source_institution']
    except:
        print "This has already been done in code above"

    #-Import META DATA-#
    #-Note Country recodes are currently using 1962 to 2000 recodes for the time being-#
    from pyeconlab.trade.dataset.NBERWTF.meta import countryname_to_iso3c, iso3c_recodes_for_1962_2000, incomplete_iso3c_for_1962_2000 
    from pyeconlab.trade.dataset.NBERWTF.meta import IntertemporalProducts      
    ICP = IntertemporalProducts().IC8400

    #--------------------#
    #-Harmonise Datasets-#
    #--------------------#

    for fl in sorted(BACI_DATASETS):
        print "[INFO] Computing Harmonised Equivalents for file: %s" % fl
        fln = fl.split("/")[-1].split(".")[0]
        source, data_type, classlevel, years = fln.split("-")
        product_level = int(classlevel.split("l")[1])
        print "[INFO] Infering Data Type Level = %s" % data_type
        print "[INFO] Infering Product Level = %s" % product_level
        if product_level == 5:
            print "Skipping Level 5 because NBER doesn't contain sitc5 values"
            continue
        #-Read Converted SITC BACI Data-#
        baci_data = pd.read_hdf(fl, key="A")              
        #-Setup New Store-#
        fln = fln.split(".")[0] + "-harmonised-nber.h5"
        store = pd.HDFStore(BACI_DIR + "harmonised/" + LOCAL_DIR + fln, complevel=9, complib='zlib')
        #-Corresponding NBER File-#
        nber_fl = NBER_DIR + fln.replace("baci", "nber").replace("1998to2012", "1984to2000").replace("-harmonised-nber", "")
        nber_store = pd.HDFStore(nber_fl)
        nber_keys = nber_store.keys()
        for dataset in sorted(nber_keys):
            dataset = dataset.replace("/","")
            print "[INFO] Processing Dataset: %s ..." % dataset
            #-Add Data to Option-#
            if DATA_OPTIONS[dataset]['intertemp_productcode']:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = (True, ICP[product_level])
            else:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = (False, None)
            #-Core
            if type(baci_data.index) == pd.MultiIndex:
                baci_data = baci_data.reset_index()
            baci_adjust = harmonise_data(baci_data.copy(deep=True), data_type, product_level, **DATA_OPTIONS[dataset])
            #-Return Option State-#
            if DATA_OPTIONS[dataset]['intertemp_productcode'][0]:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = True
            else:
                DATA_OPTIONS[dataset]['intertemp_productcode'] = False
            #-Filter Out Countries Not Represented in the NBER Dataset-#
            print "[INFO] Obtaining Countries represented in the equivalent nber dataset ..."
            nber_data = nber_store[dataset]
            if data_type == "export" or data_type == "trade":
                nber_eiso3c = nber_data.eiso3c.unique()
                baci_adjust = baci_adjust.loc[baci_adjust.eiso3c.isin(nber_eiso3c)]
                baci_adjust = baci_adjust.reset_index()
                del baci_adjust["index"]
            if data_type == "import" or data_type == "trade":
                nber_iiso3c = nber_data.iiso3c.unique()
                baci_adjust = baci_adjust.loc[baci_adjust.iiso3c.isin(nber_iiso3c)]
                baci_adjust = baci_adjust.reset_index()
                del baci_adjust["index"]
            #-Write to Store-#
            store.put(dataset, baci_adjust, format='table')
            del nber_data
            del baci_adjust
            gc.collect()
        nber_store.close()
        store.close()
        del baci_data
        gc.collect()


    #---------------#
    #-Join Datasets-#
    #---------------#

    for nberfl in sorted(NBER_DATASETS):
        #-nber-#
        print "[INFO] Merging Harmonised Equivalents for file: %s" % nberfl
        nberfln = nberfl.split("/")[-1].split(".")[0]
        source, data_type, classlevel, years = nberfln.split("-")
        product_level = int(classlevel.split("l")[1])
        print "[INFO] Infering Data Type Level = %s" % data_type
        print "[INFO] Infering Product Level = %s" % product_level
        #-baci-#
        bacifln = "baci"+"-"+data_type+"-"+classlevel+"-"+"1998to2012-harmonised-nber.h5"
        bacifl = BACI_DIR + "harmonised/" + LOCAL_DIR + bacifln
        print "[INFO] Pairing with Harmonised BACI data in file: %s" % bacifl
        #-nberbaci-#
        fln = "nberbaci"+"-"+data_type+"-"+classlevel+"-"+"1984to2012-harmonised.h5"
        store = pd.HDFStore(TARGET_DIR + fln, complevel=9, complib='zlib')
        for dataset in sorted(DATA_OPTIONS.keys()):
            print "[INFO] Processing Dataset: %s" % dataset
            #-Check Special Case of intertemp_productcode at Level 1-#
            if DATA_OPTIONS[dataset]['intertemp_productcode'] and product_level == 1:
                print "[INFO] This operation cannot occur at this level of disaggregation ... continuing"
                continue
            nber_data = pd.read_hdf(nberfl, key=dataset)
            #-Special Oil and Petroleum Adjustment-#
            if product_level > 3:
                print "[INFO] Recoding Oil and Petroleum to 334 due to BACI HS96-SITCR2 Concordance"
                sitc_code = "sitc%s"%product_level
                nber_data[sitc_code] = nber_data[sitc_code].apply(lambda x: "334" if x[0:3] == "334" else x)
                idx = set(nber_data.columns)
                idx.remove("value")
                idx= list(idx)
                nber_data = nber_data.groupby(idx).sum().reset_index()
            baci_data = pd.read_hdf(bacifl, key=dataset)
            #-Merge-#
            idx = set(nber_data.columns)
            idx.remove("value")
            idx= list(idx)
            nberbaci = nber_data.merge(baci_data, how='outer', on=idx)
            nberbaci['value'] = nberbaci[['value_x', 'value_y']].apply(lambda row: join_values(row), axis=1)
            del nberbaci["value_x"]
            del nberbaci["value_y"]
            store.put(dataset, nberbaci, format='table')
            del nber_data
            del baci_data
            gc.collect()
        store.close()
