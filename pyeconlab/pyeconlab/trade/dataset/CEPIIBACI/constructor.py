"""
Constructor: CEPII/BACI Data
============================

Files
-----
baciHS_YYYY.rar     where HS=92,96,02 and YYYY=full year

Internal Variables
------------------
    t   : year 
    hs6 : hs6 digit product code 
    i   : exporter 
    j   : importer 
    v   : value, in thousands of US dollars
    q   : quantity, in tons
    a   : start_year (?)

Supplimentary Files
-------------------
HS92
~~~~
1. country_code_baci92.csv
2. product_code_baci92.csv
3. reporter_reliability_92.rar 
4. zero_92.rar

HS96
~~~~
1. country_code_baci96.csv
2. product_code_baci96.csv
3. reporter_reliability_96.rar 
4. zero_96.rar

HS02
~~~~
1. country_code_baci02.csv
2. product_code_baci02.csv
3. reporter_reliability_02.rar 
4. zero_02.rar

Notes
-----
1. BACI has a few different version based on HS system, which may influence the BACI meta data object
"""

#-System Imports-#
import os
import re
import warnings
import string
import pandas as pd
import numpy as np
import cPickle as pickle

#-Package Imports-#
from .base import BACI
from .dataset import BACITradeData, BACIExportData, BACIImportData
from pyeconlab.trade.dataset import CPTradeData, CPExportData, CPImportData
from pyeconlab.country import ISO3166
from pyeconlab.util import check_directory, check_operations, update_operations, from_idxseries_to_pydict, concord_data

class BACIConstructor(BACI):
    """
    BACI Data Constructor

    ..  Inheritance
        -----------
        1. BACI -> Provides Meta Data on CEPII Dataset

    Parameters
    ----------
    source_dir              :   str
                                Specify source directory containing raw csv files
    source_classification   :   str
                                Type of Source Files to Load ['HS92', 'HS96', 'HS02']
    ftype                   :   str, optional(default='hdf')
                                Specify File Type ['rar', 'csv', 'hdf']
    years                   :   list, optional(default=[])
                                Apply a Year Filter [Default: All Years Available in the Data]
    skip_setup              :   bool, optional(default=False)
                                [Testing] This allows you to skip __init__ setup of object to manually load the object with csv data etc. 
                                This is mainly used for loading test data to check attributes and methods etc. 
    reduce_memory           :   bool, optional(default=False)
                                This will delete self.__raw_data after initializing self.dataset with the raw_data
                                [Warning: This will render properties that depend on self.__raw_data inoperable]
                                Useful when building datasets to be more memory efficient as the operations don't require a record of the original raw_data

    Warnings
    --------
    1. Use BACI Country Concordance (see note #2)

    Notes
    -----
    1.  When should standard_names be implimented (at the beginning or at the end?)
        Beginning: update all native imports, but a bit easier to read in the programming
        End: all baci files merge natively untouched. Could write a merge routine to undertake this approach as a check against using standard_names

    2. Country Codes: 

        Not all country codes are the current ISO3166 iso3n values.
        'France' = 251 in Baci; while ISO3166 records 'France' = 250
        Therefore use BACI country concordance to assign iso3c values!

    ..  Questions
        ---------
        1. Should self.raw_data have i,j etc rather than standard_names if it is to be untouched.

    ..  Future Work
        -----------
        1. Should I add a hdf_fn='' option for specifying a h5 file rather than using the internal defaults?
    """

    #-Protected Attributes-#
    __raw_data  = pd.DataFrame
    __source_dir    = str
    
    #-Flexible Attributes-#
    dataset         = pd.DataFrame
    data_type       = str
    name            = str 
    classification  = str 
    revision        = str 
    years           = list
    notes           = str
    complete_dataset = bool
    standard_names  = bool
    operations      = str

    #-Specific Attributes to BACIConstructor-#

    product_datafl_fixed = bool 
    country_datafl_fixed = bool


    def __init__(self, source_dir, source_classification, ftype='hdf', years=[], standard_names=True, skip_setup=False, reduce_memory=False, verbose=True):
        """ 
        Load RAW Data into Object

        Parameters
        ----------
        source_dir              :   str
                                    Specify source directory containing raw csv files
        source_classification   :   str
                                    Type of Source Files to Load ['HS92', 'HS96', 'HS02']
        ftype                   :   str, optional(default='hdf')
                                    Specify File Type ['rar', 'csv', hdf']
        years                   :   list, optional(default=[])
                                    Apply a Year Filter [Default: All Years Available in the Data]
        skip_setup              :   bool, optional(default=True)
                                    [Testing] This allows you to skip __init__ setup of object to manually load the object with csv data etc. 
                                    This is mainly used for loading test data to check attributes and methods etc. 
        reduce_memory           :   bool, optional(default=False)
                                    This will delete self.__raw_data after initializing self.dataset with the raw_data
                                    [Warning: This will render properties that depend on self.__raw_data inoperable]
                                    Useful when building datasets to be more memory efficient as the operations don't require a record of the original raw_data
    
        """
        #-Assign Source Directory-#
        self.__source_dir   = check_directory(source_dir)       #check_directory() performs basic tests on the specified directory

        #-Source Attributes-#
        self.source_revision = self.source_revision[source_classification]  #Overright with Appropriate Revision based on source_classification

        #-Assign Default Attributes-#
        self.name           = u"BACI Dataset"
        self.data_type      = u"trade"
        self.classification = source_classification
        self.revision       = source_classification[-2:]        #Last two digits    
        self.notes          = ""
        self.operations     = u"" 
        self.complete_dataset = False
        self.units_value_str = self.source_units_value_str
        #-Cache Directory-#
        self.__cache_dir = "cache/"

        #-Country, Product Source File Fixed Indicator-#
        self.product_datafl_fixed = False                       #Should this be more sophisticated, this is a constructor so probably not
        self.country_datafl_fixed = False

        #-This is a temporary fix for an inheritance bug when importing new instances of BACIConstructor()
        if re.search("_adjust", self.country_data_fn[self.classification]):
            self.country_data_fn = BACI().country_data_fn
        if re.search("_adjust", self.product_data_fn[self.classification]):
            self.product_data_fn = BACI().product_data_fn

        #-Parse Years-#
        if verbose: print "[INFO] Fetching BACI Data from %s" % source_dir
        if years == []:
            self.complete_dataset = True                        # This forces object to be imported based on the whole dataset
            years = self.source_available_years[self.classification]    
        #-Assign to Attribute-#
        self.years = years      

        #-Parse Skip Setup-#
        if skip_setup == True:
            print "[INFO] Skipping Setup of BACI Constructor!"
            self.__raw_data     = None                                      #Allows to be assigned later on
            return None

        # - Fetch Raw Data for Years - #
        if ftype == 'rar':
            self.load_raw_from_rar(verbose=verbose)
        elif ftype == 'csv':
            self.load_raw_from_csv(standard_names=False, verbose=verbose)
        elif ftype == 'hdf':
            try:
                self.load_raw_from_hdf(years=years, verbose=verbose)
            except:
                print "[INFO] Your source directory: %s does not contain h5 version.\nStarting to compile one now ...." % self.source_dir
                #-Check Cache Folder Exists-#
                if not os.path.exists(self.__source_dir + self.__cache_dir):
                    print "[INFO] Setting up a Cache Directory ..."
                    os.makedirs(self.__source_dir + self.__cache_dir)
                self.load_raw_from_csv(standard_names=False, verbose=verbose)
                self.convert_raw_data_to_hdf(verbose=verbose)               #Compute hdf file for next load
                self.convert_raw_data_to_hdf_yearindex(verbose=verbose)     #Compute Year Index Version Also
        else:
            raise ValueError("ftype must be 'rar', 'csv', or 'hdf'")

        #-Reduce Memory-#
        if reduce_memory:
            self.dataset = self.__raw_data                                  #Saves ~2Gb of RAM (but cannot access raw_data)
            self.__raw_data = None
        else:
            self.dataset = self.__raw_data.copy(deep=True)                  #[Default] pandas.DataFrame.copy(deep=True) is much more efficient than copy.deepcopy()

        #-Standard Names Option-#
        self.standard_names = standard_names
        if self.standard_names:
            self.use_standard_column_names(self.dataset, verbose=verbose)



    def __repr__(self):
        string = "Class: %s\n" % (self.__class__)                           + \
                 "Years: %s\n" % (self.years)                               + \
                 "Complete Dataset: %s\n" % (self.complete_dataset)         + \
                 "# Raw Observations: %s\n" % (self.__raw_data.shape[0])    + \
                 "# Dataset Observations: %s\n" % (self.dataset.shape[0])   + \
                 "Source Last Checked: %s\n" % (self.source_last_checked)
        return string
    
    #-----------------------------------#
    #-Properties (Protected Attributes)-#
    #-----------------------------------#

    @property
    def raw_data(self):
        """ 
        Raw Data Property to Return a Copy of the Private Attribute
        """ 
        try:
            return self.__raw_data.copy(deep=True)                              #Always Return a Copy
        except:                                                                 #Load from h5 file (quickest Load Times)
            self.load_raw_from_hdf(years=self.years, verbose=False)
            return self.__raw_data.copy(deep=True)

    def del_raw_data(self, force=False):
        """ Delete Raw Data """
        if force==True:
            del self.__raw_data

    @property
    def source_dir(self):
        return self.__source_dir


    @property 
    def yearly_world_values(self):
        rdf = self.raw_data.rename(columns={'t' : 'year', 'v' : 'value'})
        rdfy = rdf[['year', 'value']].groupby('year').sum()
        return rdfy

    def reset_dataset(self, verbose=True):
        """
        Reset Dataset to raw_data
        """
        if type(self.__raw_data) != pd.DataFrame:
            raise ValueError("RAW DATA is not a DataFrame! Most likely it has been deleted")
        if verbose: print "[INFO] Reseting Dataset to Raw Data"
        del self.dataset                                                                           #Clean-up old dataset
        self.dataset = self.__raw_data.copy(deep=True)
        self.operations = ''
        self.level = 6

    #----#
    #-IO-#
    #----#

    def load_raw_from_csv(self, standard_names=False, deletions=True, verbose=False):
        """ 
        Load Raw Data from CSV Files [Main Entry Point for Raw Data]

        Parameters
        ----------
        standard_names  :   bool, optional(default=False)
                            Apply standard names [True/False] using interface dictionary
        deletions       :   bool, optional(default=True)
                            Apply the deletions attribute 

        ..  Questions
            ---------
            1. Should this be moved to Generic Constructor Class? 
        """
        if verbose: print "[INFO] Loading RAW [.csv] Files from: %s" % (self.source_dir)
        self.__raw_data = pd.DataFrame()
        for year in self.years:
            fn = self.source_dir + 'baci' + self.classification.strip('HS') + '_' + str(year) + '.csv'
            if verbose: print "[INFO] Loading Year: %s from file: %s" % (year, fn)
            self.__raw_data = self.__raw_data.append(pd.read_csv(fn, dtype={'hs6' : str}))
        self.__raw_data = self.__raw_data.reset_index()                     #Otherwise Each year has repeated obs numbers
        del self.__raw_data['index']
        if deletions:
            for item in self.source_deletions[self.classification]:
                if verbose: print "[DELETING] Column: %s" % item
                del self.__raw_data[item]
        if standard_names:                                                      #Current Default is 'False' to keep raw_data in it's raw state
            self.use_standard_column_names(self.__raw_data)

    def load_raw_from_hdf(self, years=[], verbose=False):
        """
        Load HDF Version of RAW Dataset from a source_directory

        Parameters
        ----------
        years   :   list(int), optional(default=[])
                    Specify a year filter. Default is all years

        Notes
        -----   
        1.  To construct your own hdf version requires to initially load from BACI supplied RAW dta files.
            Then use Constructor method ``convert_source_csv_to_hdf()``

        ..  Questions
            ---------
            1. Should this be moved to Generic Constructor Class?

        """
        self.__raw_data = pd.DataFrame() 
        if years == [] or years == self.source_available_years[self.classification]:
            fn = self.source_dir + self.__cache_dir + self.raw_data_hdf_fn[self.classification]
            if verbose: print "[INFO] Loading RAW DATA from %s" % fn
            self.__raw_data = pd.read_hdf(fn, key='raw_data')
        else:
            fn = self.source_dir + self.__cache_dir + self.raw_data_hdf_yearindex_fn[self.classification] 
            for year in years:
                if verbose: print "[INFO] Loading RAW DATA for year: %s from %s" % (year, fn)
                self.__raw_data = self.__raw_data.append(pd.read_hdf(fn, key='Y'+str(year)))

    def load_country_data(self, fix_source=True, standard_names=True, verbose=True):
        """
        Load Country Classification/Concordance File From Archive
        
        Parameters
        ----------
        fix_source      :   bool, optional(default=True)
                            Apply fix_source methods to standardise the data
        standard_names  :   bool, optional(default=True)
                            Apply standard names adjustment

        Notes
        -----
        1. Could check for _adjust file before re-writing corrected file. LOW PRIORITY

        """
        #-Parse Options-#
        if fix_source and self.country_datafl_fixed == False:
            if re.search("_adjust", self.country_data_fn[self.classification]):                 #Adding these becuase using BACI inheritance causes new instances of BACIConsructor() to have updated state information in BACI() causing issues with filenames
                pass
            else:
                self.fix_country_code_baci(verbose=verbose)
        fn = self.source_dir + self.country_data_fn[self.classification]
        if verbose: print "[INFO] Reading data from file: %s" % fn
        self.country_data = pd.read_csv(fn)
        if standard_names:
            self.country_data.rename(columns={'i' : 'iso3n', 'iso3' : 'iso3c'}, inplace=True)

    def load_product_data(self, fix_source=True, standard_names=True, verbose=False):
        """
        Load Product Code Classification File from Archive

        Parameters
        ----------
        fix_source      :   bool, optional(default=True)
                            Apply fix_source methods to standardise the data
        standard_names  :   bool, optional(default=True)
                            Apply standard names adjustment

        """
        #-Parse Options-#
        if fix_source and self.product_datafl_fixed == False:
            if re.search("_adjust", self.product_data_fn[self.classification]):                 #Adding these becuase using BACI inheritance causes new instances of BACIConsructor() to have updated state information in BACI() causing issues with filenames
                pass
            else:
                if self.classification == "HS92":
                    self.fix_product_code_baci92(verbose=verbose)
                if self.classification == "HS96":
                    self.fix_product_code_baci96(verbose=verbose)
                if self.classification == "HS02":
                    self.fix_product_code_baci02(verbose=verbose)
        if self.product_datafl_fixed == False and self.classification == "HS02":
            print "[WARNING] Has the product_code_baci02 data been adjusted in the source_dir!"         #-Should this be a warn.warnings?-#
        fn = self.source_dir + self.product_data_fn[self.classification]
        self.product_data = pd.read_csv(fn, dtype={'Code' : object})
        if standard_names:
            self.use_standard_column_names(self.product_data)

    def use_standard_column_names(self, df, verbose=True):
        """
        Use interface attribute to Adjust Columns to use Standard Names (inplace=True)

        Parameters
        ----------
        df  :   pd.DataFrame
                Data to be adjusted

        Notes
        -----
        1. This method works in-place

        """
        opstring = u"(use_standard_column_names)"
        if verbose:
            for item in df.columns:
                try: print "[CHANGING] Column: %s to %s" % (item, self.source_interface[item])
                except: pass                                                                            #-Passing Items not Converted by self.source_interface-#
        self.standard_names = True
        df.rename(columns=self.source_interface, inplace=True)
        #-Update Operations Attribute-#
        exception_complete_dataset = self.complete_dataset 
        update_operations(self, opstring)
        if exception_complete_dataset:                          #This Exception is Necessary as the update_operations by default sets complete_dataset to False but this is a harmless operation
            self.complete_dataset = True

    #------------#
    #-Conversion-#
    #------------#

    def convert_raw_data_to_hdf(self, key='raw_data', format='table', hdf_fn='', verbose=True): 
        """ 
        Convert Raw Data to HDF

        Parameters
        ----------
        key     :   str, optional(default='raw_data')
                    Specify a key for the hdf file
        format  :   str, optional(defualt='table') #Fixed or Table?
                    Specify hdf file format
        hdf_fn  :   str, optional(default='')
                    Specify a custom file name, otherwise attribute hdf_fn is used

        Notes 
        -----
        1. this currently just converts whatever is contained in raw_data

        ..  Future Work
            -----------
            1. Add a year filter and name the file accordingly
        """
        if hdf_fn == '':
            hdf_fn = self.source_dir + self.__cache_dir + self.raw_data_hdf_fn[self.classification]    #This default is the entire dataset for any given classification
        if verbose: print "[INFO] Writing raw_data to %s" % hdf_fn
        hdf = pd.HDFStore(hdf_fn, complevel=9, complib='zlib')
        hdf.put(key, self.raw_data, format=format)
        if verbose: print hdf
        hdf.close()
    
    def convert_raw_data_to_hdf_yearindex(self, format='table', hdf_fn='', verbose=True):
        """ 
        Convert Raw Data to HDF File Indexed by Year
        
        Parameters
        ----------
        format  :   str, optional(defualt='table') #Fixed or Table?
                    Specify hdf file format 
        hdf_fn  :   str, optional(default='')
                    Specify a custom file name, otherwise attribute hdf_fn is used

        Notes 
        -----
        1. This compute a list of unique years from self.__raw_data

        """
        if hdf_fn == '': 
            hdf_fn = self.source_dir + self.__cache_dir + self.raw_data_hdf_yearindex_fn[self.classification]  #This default is the entire dataset for any given classification
        if verbose: print "[INFO] Writing raw_data to %s" % hdf_fn
        #-Construct HDF File-#
        hdf = pd.HDFStore(hdf_fn, complevel=9, complib='zlib')
        if self.standard_names == True:
            yid = 'year'
        else:
            yid = 't'
        for year in self.raw_data[yid].unique():
            hdf.put('Y'+str(year), self.raw_data.loc[self.raw_data[yid] == year], format=format)    
        if verbose: print hdf
        hdf.close()

    def convert_csv_to_hdf_yearindex(self, years=[], format='fixed', hdf_fn='', verbose=True):
        """ 
        Convert CSV Files to HDF File Indexed by Year

        Parameters
        ----------
        years   :   list, optional(defualt=[])
                    Apply a year filter. Default to all years
        format  :   str, optional(defualt='fixed')
                    Specify hdf file format
        hdf_fn  :   str, optional(default='')
                    Specify a custom file name, otherwise attribute hdf_fn is used

        """
        if years == []:
            years = self.source_available_years[self.classification]
        #-Setup HDF File-#
        if hdf_fn == '':
            hdf_fn = self.source_dir + self.__cache_dir + self.raw_data_hdf_yearindex_fn[self.classification]
        hdf = pd.HDFStore(hdf_fn, complevel=9, complib='zlib')
        #-Convert Years-#
        for year in years:
            csv_fn = self.source_dir + 'baci' + self.classification.strip('HS') + '_' + str(year) + '.csv'
            if verbose: print "[INFO] Converting file: %s to file: %s" % (csv_fn, hdf_fn)
            hdf.put('Y'+str(year), pd.read_csv(csv_fn, data_type={'hs6' : str}), format=format)
        if verbose: print hdf
        hdf.close()
        return hdf_fn

    #---------#
    #-Pickles-#
    #---------#

    def to_pickle(self, fn, data='dataset', verbose=False):
        """
        Send self.raw_data or self.dataset to a pickle file

        Parameters
        ----------
        fn      :   str
                    Specify a file name
        data    :   str, optional(default='dataset')
                    Select data attribute to save to pickle file (dataset or raw_data)

        """
        fl = open(fn, 'wb') 
        if data == 'dataset':
            pickle.dump(self.dataset, fl)
        elif data == 'raw_data' or data == 'raw':
            pickle.dump(self.raw_data, fl)
        else:
            raise ValueError("data must be either 'dataset' or 'raw_data'")
        fl.close()

    #-------------------#
    #-Fix Source Issues-#
    #-------------------#

    def fix_country_code_baci(self, verbose=True):
        """
        Fix Source Country Code File Issued Based on Classification
        This method selects the appropriate collection of fixes based on source data revision
        """
        opstring = u"(fix_country_code)"
        if check_operations(self, opstring):
            print "[INFO] Process has already been applied to source file"
            return
        if self.classification == "HS92":
            self.fix_country_code_baci9296(verbose=verbose)
        elif self.classification == "HS96":
            self.fix_country_code_baci9296(verbose=verbose)
        elif self.classification == "HS02":
            self.fix_country_code_baci02(verbose=verbose)
        else:
            raise ValueError("Invalid Classification: %s" % self.classification)
        update_operations(self, opstring)

    def fix_country_code_baci9296(self, verbose=True):
        """ 
        Fix issue with country_code_baci92 or country_code_baci96 csv file

        Notes
        -----
        1. baci92 and baci96 share similar issues and have therefore been combined
        """
        warnings.warn("[FORMATER ISSUE] Unicode French Characters are removed in the adjusted file")
        if verbose: print "[INFO] Fixing original %s country data file in source_dir: %s" % (self.classification, self.source_dir)
        if self.classification != "HS92" and self.classification != "HS96":
            raise ValueError("This method only runs on HS92 or HS96 Data")
        fn = self.country_data_fn[self.classification]
        f = open(self.source_dir + fn, 'r')
        #-Adjust Filename-#
        fn,ext = fn.split(".")
        fn = fn + "_adjust" + "." + ext
        nf = open(self.source_dir + fn, 'w')
        #-Core-#
        for idx, line in enumerate(f):
            line = filter(lambda x: x in string.printable, line)
            line = line.replace("?", "")
            nf.write(line)
        #-Close Files-#
        f.close()
        nf.close()
        #-DropDuplicates-#
        df = pd.read_csv(self.source_dir+fn)
        init_shape = df.shape
        df = df.drop_duplicates()
        df.to_csv(self.source_dir+fn, index=False)
        if verbose: print "[INFO] Dropping (%s) Duplicates Found in country codes file: %s" % (init_shape[0] - df.shape[0], self.country_data_fn[self.classification])
        #-Update File List to use adjusted File-#
        if verbose: print "[INFO] Replacing internal reference from: %s to: %s" % (self.country_data_fn[self.classification], fn)
        self.country_data_fn[self.classification] = fn
        self.country_datafl_fixed = True

    def fix_country_code_baci02(self, verbose=True):
        """ 
        Fix issues with country_code_baci02 csv file
        """
        warnings.warn("[FORMATER ISSUE] Unicode French Characters are removed in the adjusted file")
        if verbose: print "[INFO] Fixing original HS02 country data file in source_dir: %s" % self.source_dir
        if self.classification != "HS02":
            raise ValueError("This method only runs on HS02 Data")
        fn = self.country_data_fn["HS02"]
        f = open(self.source_dir + fn, 'r')
        #-Adjust Filename-#
        fn,ext = fn.split(".")
        fn = fn + "_adjust" + "." + ext
        nf = open(self.source_dir + fn, 'w')
        #-Core-#
        for idx, line in enumerate(f):
            line = filter(lambda x: x in string.printable, line)
            line = line.replace("?", "")
            match = re.match("\"(.*)\"", line)
            if match:
                nwl = match.group(1)
                nwl = nwl.replace("\"\"", "\"")
                nwl += "\n"
            else:
                nwl = "%s,%s,\"%s\",\"%s\",%s" % tuple(line.split(","))
            nf.write(nwl)
        #-Close Files-#
        f.close()
        nf.close()
        #-DropDuplicates-#
        df = pd.read_csv(self.source_dir+fn)
        init_shape = df.shape
        df = df.drop_duplicates()
        df.to_csv(self.source_dir+fn, index=False)
        if verbose: print "[INFO] Dropping (%s) Duplicates Found in country codes file: %s" % (init_shape[0] - df.shape[0], self.country_data_fn["HS02"])
        #-Update File List to use adjusted File-#
        if verbose: print "[INFO] Replacing internal reference from: %s to: %s" % (self.country_data_fn["HS02"], fn)
        self.country_data_fn["HS02"] = fn
        self.country_datafl_fixed = True

    #-ProductCode File Fixes-#

    def fix_product_code_baci92(self, verbose=True):
        """
        Fix issues with the product_code_baci92.csv file
        """
        if verbose: print "[INFO] Fixing original HS92 product data file in source_dir: %s" % self.source_dir
        fn = self.product_data_fn["HS92"]
        df = pd.read_csv(self.source_dir + fn)
        df.columns = ["Code", "Description"]
        #-Update FileName-#
        fn,ext = fn.split(".")
        fn = fn + "_adjust" + "." + ext
        df.to_csv(self.source_dir + fn, index=False, quoting=True)
        #-Update File List to use adjusted File-#
        if verbose: print "[INFO] Replacing internal reference from: %s to: %s" % (self.product_data_fn["HS92"], fn)
        self.product_data_fn["HS92"] = fn
        self.product_datafl_fixed = True

    def fix_product_code_baci96(self, verbose=True):
        """
        Fix issues with product_code_baci96.csv file
        """
        if verbose: print "[INFO] Fixing original HS96 product data file in source_dir: %s" % self.source_dir
        if self.classification != "HS96":
            raise ValueError("This method only runs on HS96 Data")
        fn = self.product_data_fn["HS96"]
        f = open(self.source_dir + fn, 'r')
        #-Adjust Filename-#
        fn,ext = fn.split(".")
        fn = fn + "_adjust" + "." + ext
        nf = open(self.source_dir + fn, 'w')
        for idx, line in enumerate(f):
            if idx == 0:
                nf.write("\"Code\",\"Description\"\n")
                continue
            code = re.match("^([0-9]*);?", line).group(1)                                            #look for productcodes
            rest = line.lstrip(code+";")
            rest = rest.replace("\"", "")
            rest = rest.rstrip()
            line = "\"%s\",\"%s\"" % (code, rest)
            nf.write(line + "\n")
        #-Close Files-#
        f.close()
        nf.close()
        #-Update File List to use adjusted File-#
        if verbose: print "[INFO] Replacing internal reference from: %s to: %s" % (self.product_data_fn["HS96"], fn)
        self.product_data_fn["HS96"] = fn
        self.product_datafl_fixed = True

    def fix_product_code_baci02(self, verbose=True):
        """
        Fix issues with product_code_baci02 csv file
        """
        if verbose: print "[INFO] Fixing original HS02 product data file in source_dir: %s" % self.source_dir
        if self.classification != "HS02":
            raise ValueError("This method only runs on HS02 Data")
        fn = self.product_data_fn["HS02"]
        f = open(self.source_dir + fn, 'r')
        #-Adjust Filename-#
        fn,ext = fn.split(".")
        fn = fn + "_adjust" + "." + ext
        nf = open(self.source_dir + fn, 'w')
        for idx, line in enumerate(f):
            line = re.match("^\"(.*)\"$", line).group(1)                    #Unwrap extra "'s
            line = line.replace('\"\"', '\"')                               #Remove Double Quoting
            #-Fix for Code Column-#
            if idx == 0:
                line = line.lstrip('Code')
                line = '\"Code\"' + line
            else:
                try:
                    code = re.search("^([0-9]*),", line).group(1)
                except:
                    continue                                                #Trailing White Spaces
                line = line.lstrip(code)
                line = '\"' + code + '\"' + line
            nf.write(line + "\n")
        #-Close Files-#
        f.close()
        nf.close()
        #-Update File List to use adjusted File-#
        if verbose: print "[INFO] Replacing internal reference from: %s to: %s" % (self.product_data_fn["HS02"], fn)
        self.product_data_fn["HS02"] = fn
        self.product_datafl_fixed = True

    #-----------------------#
    #-Operations on Dataset-#
    #-----------------------#

    def add_country_iso3c(self, remove_iso3n=False, dropna=False, drop_special=(False, ['NTZ']), verbose=True):
        """
        Add CountryNames to ISO3N Codes

        Parameters
        ----------
        remove_iso3n    :   bool, optional(default=False)
                            Remove iso3n columns
        dropna          :   bool, optional(default=False)
                            drop countries in the concordance that are np.nan()     
                            [Default: False. Care must be given when dropping as it may influence exporter, importer collapses]
        drop_special    :   tuple(bool, list(str)), optional(default=(False, ['NTZ']))
                            drop special tuple 3-digit codes such as NTZ = Neutral Zone
        
        Notes
        -----
        1. Check these against the CountryNames Concordance File to Ensure that None of them are Countries (dropna=True)
            [1] EISO3N Codes Dropped: 
                [10, 74, 80, 129, 239, 275, 290, 334, 336, 471, 473, 492, 499, 527, 531, 534, 535, 568, 577, 581,636, 637, 688, 728, 729, 807, 837, 838, 839, 879, 899]
            [2] IISO3N Codes Dropped: 
                [10, 74, 80, 129, 221, 239, 275, 290, 334, 336, 471, 473, 492, 499, 527, 531, 534, 535, 568, 577, 581, 636, 637, 688, 697, 728, 729, 807, 837, 838, 839, 879, 899]
        """
        edrop, idrop, spdrop = 0,0,0
        init_obs = self.dataset.shape[0]
        #-Exporter-#
        self.dataset = self.dataset.merge(self.country_data[['iso3n', 'iso3c']], how='left', left_on=['eiso3n'], right_on=['iso3n'])
        del self.dataset['iso3n']                                                                                                           #Remove Merge Key
        self.dataset.rename(columns={'iso3c' : 'eiso3c'}, inplace=True)
        if dropna:
            print "[INFO] Removing Units with eiso3c == np.nan"
            edrop = len(self.dataset[self.dataset.eiso3c.isnull()])
            print "[Deleting] EISO3N %s observations with codes:" % edrop
            print "%s" % sorted(self.dataset[self.dataset.eiso3c.isnull()].eiso3n.unique())
            self.dataset.dropna(subset=['eiso3c'], inplace=True)
        #-Importer-#
        self.dataset = self.dataset.merge(self.country_data[['iso3n', 'iso3c']], how='left', left_on=['iiso3n'], right_on=['iso3n'])
        del self.dataset['iso3n']                                                                                                           #Remove Merge Key
        self.dataset.rename(columns={'iso3c' : 'iiso3c'}, inplace=True)
        if dropna:
            print "[INFO] Removing Units with iiso3c == np.nan"
            idrop = len(self.dataset[self.dataset.iiso3c.isnull()])
            print "[Deleting] IISO3N %s observations with codes:" % idrop
            print "%s" % sorted(self.dataset[self.dataset.iiso3c.isnull()].iiso3n.unique())
            self.dataset.dropna(subset=['iiso3c'], inplace=True)
            self.dataset = self.dataset.reset_index()
            del self.dataset['index']
        if remove_iso3n:
            del self.dataset['eiso3n']
            del self.dataset['iiso3n']
        drop_special, iso3c_list = drop_special
        if drop_special:
            for iso3c in iso3c_list:
                spdrop = len(self.dataset.loc[(self.dataset.iiso3c == iso3c) | (self.dataset.eiso3c == iso3c)])
                if verbose: print "[Deleting] %s country code with %s observations" % (iso3c, spdrop)
                self.dataset.drop(self.dataset.loc[(self.dataset.iiso3c == iso3c) | (self.dataset.eiso3c == iso3c)].index, inplace=True)
        assert init_obs == self.dataset.shape[0]+edrop+idrop+spdrop, "%s != %s" % (init_obs, self.dataset.shape[0])

    def countries_only(self, cid='iso3n', verbose=True):
        """
        Drop countries specified in attributes country_only_iso3c_deletions, country_only_iso3n_deletions
        
        Parameters
        ----------
        cid     :   str, optional(default='iso3n')
                    Specify what country code type to match on
        Notes 
        -----
        1. this function only uses standard_names (iiso3n, iiso3c etc) 

        ..  Future Work 
            -----------
            1. Write a decorator to print number of observations before and after the function runs
        """
        if cid == 'iso3n':
            for item in self.country_only_iso3n_deletions[self.classification]:
                if verbose: print "[INFO] Deleting iiso3n and eiso3n code: %s" % item
                init_numobs = self.dataset.shape[0]
                self.dataset = self.dataset[self.dataset['eiso3n'] != item]
                self.dataset = self.dataset[self.dataset['iiso3n'] != item]
                if verbose: print "[DELETED] %s observations" % (init_numobs - self.dataset.shape[0])
        elif cid == 'iso3c':
            for item in self.country_only_iso3c_deletions[self.classification]:
                if verbose: print "[INFO] Deleting iiso3c and eiso3c code: %s" % item
                init_numobs = self.dataset.shape[0]
                self.dataset = self.dataset[self.dataset['eiso3c'] != item]
                self.dataset = self.dataset[self.dataset['iiso3c'] != item]
                if verbose: print "[DELETED] %s observations" % (init_numobs - self.dataset.shape[0])
        else:
            raise ValueError("'cid' must be 'iso3n' or 'iso3c'")

    def concord_productcode(self, concordance, new_classification, new_level, verbose=True):
        """ 
        Apply a HS6 concordance to convert dataset to a new trade classification
        
        Parameters
        ----------
        concordance         :   dict('ProductCode' : 'ProductCode')
                                concordance dictionary of HS6 to Something Else
        new_classification  :   str
                                Set new classification attribute
        new_level           :   int 
                                Set new classification level

        Notes
        -----
        1. Eligible for a Generic Constructor

        ..  Future Work 
            -----------
            1. Account for levels in special cases
            2. Consider Implimenting Aggregation for quantity
            3. Automate classification and level encoding

        """
        #-Add Special Cases to the concordance-#
        for k,v in  self.adjust_hs6_to_sitc[self.classification].items():
            concordance[k] = v
        self.dataset[new_classification] = self.dataset['hs6'].apply(lambda x: concord_data(concordance, x, issue_error='.'))
        self.dataset = self.dataset[['year', 'eiso3n', 'iiso3n', 'value']+[new_classification]].groupby(['year', 'eiso3n', 'iiso3n']+[new_classification]).sum().reset_index()
        #-Reset Attributes-#
        self.classification = new_classification
        self.level = new_level      


    def merge_all_sourcefiles(self, rename_newvars=True, verbose=True):
        """
        Merge all baciXX_YYYY.csv, country_code_baciXX.csv, product_code_baciXX.csv files using native column names
        
        Status: UNDER-REVIEW

        Notes
        -----
        1. Is this still useful? => Propose Deletion
        2. This can be used as a test against using the converted standard_names
        3. Should I rename incoming data? I think harmonised internal reader friendly columns names are the best solution
        """
        warnings.warn("[WARNING] This method will reconstruct raw_data and dataset attributes!", UserWarning)
        #-Ensure raw_data is source data-#
        self.load_raw_from_csv(standard_names=False, deletions=False, verbose=verbose)
        self.load_country_data(standard_names=False, verbose=verbose)
        self.load_product_data(fix_source=True, standard_names=False, verbose=verbose)
        #-Merge Datasets-#
        self.dataset = self.raw_data
        #-Countries-#
        #-Exporters-#
        self.dataset = self.dataset.merge(self.country_data[['iso3', 'name_english', 'i']], how="left", left_on='i', right_on=['i'])
        if rename_newvars: 
            self.dataset = self.dataset.rename(columns={'iso3' : 'eiso3c', 'name_english' : 'ename'})
        #-Importers-#
        self.dataset = self.dataset.merge(self.country_data[['iso3', 'name_english', 'i']], how="left", left_on=['j'], right_on=['i'])
        if rename_newvars: 
            self.dataset = self.dataset.rename(columns={'iso3' : 'iiso3c', 'name_english' : 'iname'})
        #-Products-#
        self.dataset = self.dataset.merge(self.product_data[['Code', 'ShortDescription']], how="left", left_on=['hs6'], right_on=['Code'])

    #-----------#
    #-META DATA-#
    #-----------#

    def iso3n_to_countryname_pyfile(self, target_dir='meta/', force=False, verbose=True):
        """ 
        Write Dictionary of iso3n to countryname pyfile from BACI Country Concordance File
        
        Parameters
        ----------
        target_dir  :   str, optional(default='meta/')
                        Specify target directory
        force       :   bool, optional(default=False)
                        Apply force (i.e. useful if data is incomplete). Useful for debugging

        """
        if not self.complete_dataset:
            if not force: raise ValueError("This is not a complete dataset!")
        if not self.country_datafl_fixed:
            self.fix_country_code_baci()
        #-Set Filename-#
        fl = '%s_iso3n_to_name.py' % (self.classification.lower())
        docstring = "ISO3N to CountryName Dictionary for Classification: %s" % self.classification
        data = pd.read_csv(self.source_dir + self.country_data_fn[self.classification])
        data = data[['i', 'name_english']].set_index('i')
        data = data['name_english']
        data.name = u"iso3n_to_name"
        from_idxseries_to_pydict(data, target_dir=target_dir, fl=fl, docstring=docstring, verbose=verbose)

    def iso3n_to_iso3c_pyfile(self, target_dir='meta/', force=False, verbose=True):
        """ 
        Write Dictionary of iso3n to iso3c pyfile from BACI Country Concordance File

        Parameters
        ----------
        target_dir  :   str, optional(default='meta/')
                        Specify target directory
        force       :   bool, optional(default=False)
                        Apply force (i.e. useful if data is incomplete). Useful for debugging

        """
        if not self.complete_dataset:
            if not force: raise ValueError("This is not a complete dataset!")
        if not self.country_datafl_fixed:
            self.fix_country_code_baci()
        #-Set Filename-#
        fl = '%s_iso3n_to_iso3c.py' % (self.classification.lower())
        docstring = "ISO3N to CountryName Dictionary for Classification: %s" % self.classification
        docstring += "Source: BACI Country Concordance File"
        data = pd.read_csv(self.source_dir + self.country_data_fn[self.classification])
        data = data[['i', 'iso3']].set_index('i').dropna()
        data = data['iso3']
        data.name = u"iso3n_to_iso3c"
        from_idxseries_to_pydict(data, target_dir=target_dir, fl=fl, docstring=docstring, verbose=verbose)

    #--------------------#
    #-Country Codes Meta-#
    #--------------------#

    def intertemporal_countrycodes(self, cid='iso3c', dataset=False, force=False, wmeta=False, verbose=True):
        """
        Wrapper for Generating intertemporal_countrycodes from 'raw_data' or 'dataset'

        Parameters
        ----------
        cid         :   str, optional(default='iso3c')
                        Country Identifier
        dataset     :   bool, optional(default=False)
                        Specify wether to use the dataset attribute (defaults to raw_data)
        force       :   bool, optional(defaults=False)
                        Apply force (i.e. useful if data is incomplete). Useful for debugging
        wmeta       :   bool, optional(defaults=False)
                        Include Meta Data (i.e. CountryName and CountryISO3C)

        Notes
        -----
        1. Is this really necessary?

        """
        if dataset:
            if verbose: print "Constructing Intertemporal Country Code Tables from Dataset ..."
            table_iiso3n, table_eiso3n = self.intertemporal_countrycodes_dataset(cid=cid, force=force, meta=meta, verbose=verbose)
            return table_iiso3n, table_eiso3n
        else:
            if verbose: print "Constructing Intertemporal Country Code Tables from RAW DATA ..."
            table_iiso3n, table_eiso3n = self.intertemporal_countrycodes_raw_data(force=force, meta=meta, verbose=verbose)
            return table_iiso3n, table_eiso3n

    def intertemporal_countrycodes_raw_data(self, force=False, wmeta=False, verbose=False):
        """
        Construct a table of importer and exporter country codes by year from RAW DATA
        Intertemporal Country Code Tables can also be computed from dataset using .intertemporal_countrycodes_dataset()
        which includes iso3c etc.

        Parameters
        ----------
        force       :   bool, optional(default=False)
                        Apply force (i.e. useful if data is incomplete). Useful for debugging
        wmeta       :   bool, optional(default=False)
                        Include Meta Data (i.e. CountryNames, CountryISO3C)

        Returns
        -------
            table_iiso3n, table_eiso3n

        Notes
        -----
        1.  The raw data file interface is specified as ::
                
                Interface
                ---------
                't'     : year 
                'hs6'   : HS 6 Digits
                'i'     : Exporter
                'j'     : Importer 
                'v'     : Value
                'q'     : Quantity

        """
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("[ERROR] Not a Complete Dataset!")
        #-Get Raw Data -#
        data = self.__raw_data        
        #-Core-#
        #-Importers-#
        table_iiso3n = data[['t', 'j']]
        table_iiso3n['code'] = table_iiso3n['j']                    #keep a 'j' in the index
        table_iiso3n = table_iiso3n.drop_duplicates().set_index(['j', 't'])
        table_iiso3n = table_iiso3n.unstack(level='t')
        table_iiso3n.columns = table_iiso3n.columns.droplevel()     #Removes Unnecessary 'code' label
        if wmeta:
            from .meta import iso3n_to_iso3c, iso3n_to_name
            iso3n_to_iso3c = pd.Series({int(k):v for k,v in iso3n_to_iso3c[self.classification].iteritems()}).to_frame(name="iiso3c")
            iso3n_to_name = pd.Series({int(k):v for k,v in iso3n_to_name[self.classification].iteritems()}).to_frame(name="importer")                 #Just include shorter iso3c version
            table_iiso3n = table_iiso3n.merge(iso3n_to_iso3c, left_index=True, right_index=True).reset_index().set_index(keys=['index'])
            table_iiso3n = table_iiso3n.merge(iso3n_to_name, left_index=True, right_index=True).reset_index().set_index(keys=['index', 'iiso3c', 'importer'])
            table_iiso3n.index.set_names(names=['j', 'iiso3c', 'importer'], inplace=True)
        #-Exporters-#
        table_eiso3n = data[['t', 'i']]
        table_eiso3n['code'] = table_eiso3n['i']                    #keep a 'j' in the index
        table_eiso3n = table_eiso3n.drop_duplicates().set_index(['i', 't'])
        table_eiso3n = table_eiso3n.unstack(level='t')
        table_eiso3n.columns = table_eiso3n.columns.droplevel()     #Removes Unnecessary 'code' label
        if wmeta:
            from .meta import iso3n_to_iso3c, iso3n_to_name
            iso3n_to_iso3c = pd.Series({int(k):v for k,v in iso3n_to_iso3c[self.classification].iteritems()}).to_frame(name="eiso3c")
            iso3n_to_name = pd.Series({int(k):v for k,v in iso3n_to_name[self.classification].iteritems()}).to_frame(name="exporter")                 #Just include shorter iso3c version
            table_eiso3n = table_eiso3n.merge(iso3n_to_iso3c, left_index=True, right_index=True).reset_index().set_index(keys=['index'])
            table_eiso3n = table_eiso3n.merge(iso3n_to_name, left_index=True, right_index=True).reset_index().set_index(keys=['index', 'eiso3c', 'exporter'])
            table_eiso3n.index.set_names(names=['i', 'eiso3c', 'exporter'], inplace=True)
        return table_iiso3n, table_eiso3n

    def intertemporal_countrycodes_dataset(self, cid='iso3n', force=False, wmeta=False, verbose=False):
        """
        Construct a table of importer and exporter country codes by year from DATASET
        This includes iso3c and is useful when using .countries_only() etc.
        
        Parameters
        ----------
        cid         :   str, optional(default='iso3n')
                        Specify country code identifier ('iso3n', 'iso3c')
        force       :   bool, optional(default=False)
                        Apply force (i.e. useful if data is incomplete). Useful for debugging
        wmeta       :   bool, optional(default=False)
                        Include Meta Data (i.e. CountryNames, CountryISO3C)

        Returns
        -------
            table_iiso3, table_eiso3

        Notes
        -----
        1. This method requires standard_names

        """
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("[ERROR] Not a Complete Dataset!")
        #-Get Dataset-#
        data = self.dataset 
        #-Check Standard Names-#
        if not check_operations(self, u"(use_standard_column_names)"):
            self.use_standard_column_names(data, verbose=verbose) 
        #-Importers-#
        table_iiso3 = data[['year', 'i'+cid]]
        table_iiso3['code'] = table_iiso3['i'+cid]              #keep a 'j' in the index
        table_iiso3 = table_iiso3.drop_duplicates().set_index(['i'+cid, 'year'])
        table_iiso3 = table_iiso3.unstack(level='year')
        table_iiso3.columns = table_iiso3.columns.droplevel()   #Removes Unnecessary 'code' label
        if wmeta:
            from .meta import iso3n_to_iso3c, iso3n_to_name
            iso3n_to_iso3c = pd.Series({int(k):v for k,v in iso3n_to_iso3c[self.classification].iteritems()}).to_frame(name="iiso3c")
            # iso3n_to_name = pd.Series({int(k):v for k,v in iso3n_to_name[self.classification].iteritems()}).to_frame(name="importer")                 #Just include shorter iso3c version
            table_iiso3n = table_iiso3n.merge(iso3n_to_iso3c, left_index=True, right_index=True).reset_index().set_index(keys=['index', 'iiso3c', ])
            table_iiso3n.index.set_names(names=['j', 'iiso3c'], inplace=True)
        #-Exporters-#
        table_eiso3 = data[['year', 'e'+cid]]
        table_eiso3['code'] = table_eiso3['e'+cid]                  #keep a 'j' in the index
        table_eiso3 = table_eiso3.drop_duplicates().set_index(['e'+cid, 'year'])
        table_eiso3 = table_eiso3.unstack(level='year')
        table_eiso3.columns = table_eiso3.columns.droplevel()   #Removes Unnecessary 'code' label
        if wmeta:
            from .meta import iso3n_to_iso3c, iso3n_to_name
            iso3n_to_iso3c = pd.Series({int(k):v for k,v in iso3n_to_iso3c[self.classification].iteritems()}).to_frame(name="eiso3c")
            # iso3n_to_name = pd.Series({int(k):v for k,v in iso3n_to_name[self.classification].iteritems()}).to_frame(name="exporter")                 #Just include shorter iso3c version
            table_eiso3n = table_eiso3n.merge(iso3n_to_iso3c, left_index=True, right_index=True).reset_index().set_index(keys=['index', 'eiso3c'])
            table_eiso3n.index.set_names(names=['i', 'eiso3c'], inplace=True)
        return table_iiso3, table_eiso3


    # - Product Codes Meta - #

    def intertemporal_productcodes(self, dataset=False, index="p", force=False, wmeta=True, verbose=False):
        """
        Construct a table of productcodes by year
        
        Parameters
        ----------
        dataset     :   bool, optional(default=False)
                        Build from RAW Data or Dataset Attribute
        force       :   bool, optional(default=False)
                        Force method to be performed on incomplete data. Useful for testing. 

        Future Work
        -----------
        1.  Implement a Table returned in a different classification or level
            classification="HS96", level=6,
        """
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("[ERROR] Not a Complete Dataset!")
        #-Parse Data Source-#
        if dataset:
            data = self.dataset
        else:
            self.reset_dataset(verbose=verbose)             #Ensure RAW DATA Starting Point
            self.load_country_data(verbose=verbose)
            self.use_standard_column_names(self.dataset)    #Use Standard Column Names
            self.add_country_iso3c(verbose=verbose)         #Add ISO3C
            self.load_product_data(verbose=verbose)         #Add Product Data
            data = self.dataset
        #-Core-#
        if index == "cp" or index == "pc":
            #-Exporter-Product Index-#
            table_eiso3n_hs6 = data[['year', 'hs6', 'eiso3n']].drop_duplicates()
            table_eiso3n_hs6['code'] = 1
            table_eiso3n_hs6 = table_eiso3n_hs6.set_index(['eiso3n', 'hs6', 'year']).sort_index()
            table_eiso3n_hs6 = table_eiso3n_hs6.unstack(level='year')
            table_eiso3n_hs6.columns = table_eiso3n_hs6.columns.droplevel()   #Removes Unnecessary 'code' label
            if wmeta:
                from .meta import iso3n_to_iso3c, iso3n_to_name
                iso3n_to_iso3c = pd.Series({int(k):v for k,v in iso3n_to_iso3c[self.classification].iteritems()}).to_frame(name="eiso3c")
                iso3n_to_name = pd.Series({int(k):v for k,v in iso3n_to_name[self.classification].iteritems()}).to_frame(name="exporter")                 #Just include shorter iso3c version
                table_eiso3n_hs6 = table_eiso3n_hs6.reset_index().set_index(['eiso3n']).merge(iso3n_to_iso3c, left_index=True, right_index=True)
                table_eiso3n_hs6 = table_eiso3n_hs6.merge(iso3n_to_name, left_index=True, right_index=True).reset_index().set_index(keys=['index', 'eiso3c', 'exporter', 'hs6'])
                table_eiso3n_hs6.index.set_names(names=['eiso3n', 'eiso3c', 'exporter', 'hs6'], inplace=True)  
            #-Importer-Product Index-#
            table_iiso3n_hs6 = data[['year', 'hs6', 'iiso3n']].drop_duplicates()
            table_iiso3n_hs6['code'] = 1
            table_iiso3n_hs6 = table_iiso3n_hs6.set_index(['iiso3n', 'hs6', 'year']).sort_index()
            table_iiso3n_hs6 = table_iiso3n_hs6.unstack(level='year')
            table_iiso3n_hs6.columns = table_iiso3n_hs6.columns.droplevel()   #Removes Unnecessary 'code' label  
            if wmeta:
                from .meta import iso3n_to_iso3c, iso3n_to_name
                iso3n_to_iso3c = pd.Series({int(k):v for k,v in iso3n_to_iso3c[self.classification].iteritems()}).to_frame(name="iiso3c")
                iso3n_to_name = pd.Series({int(k):v for k,v in iso3n_to_name[self.classification].iteritems()}).to_frame(name="importer")                 #Just include shorter iso3c version
                table_iiso3n_hs6 = table_iiso3n_hs6.reset_index().set_index(['iiso3n']).merge(iso3n_to_iso3c, left_index=True, right_index=True)
                table_iiso3n_hs6 = table_iiso3n_hs6.merge(iso3n_to_name, left_index=True, right_index=True).reset_index().set_index(keys=['index', 'iiso3c', 'importer', 'hs6'])
                table_iiso3n_hs6.index.set_names(names=['iiso3n', 'iiso3c', 'importer', 'hs6'], inplace=True)      
            if index == "pc":
                if wmeta:
                    table_eiso3n_hs6 = table_eiso3n_hs6.reorder_levels([3,0,1,2]).sort_index()
                    table_iiso3n_hs6 = table_iiso3n_hs6.reorder_levels([3,0,1,2]).sort_index()
                else:
                    table_eiso3n_hs6 = table_eiso3n_hs6.reorder_levels([1,0]).sort_index()
                    table_iiso3n_hs6 = table_iiso3n_hs6.reorder_levels([1,0]).sort_index()
            return table_eiso3n_hs6, table_iiso3n_hs6
        else:
            #-Simple Product Index-#
            table_hs6 = data[['year', 'hs6']].drop_duplicates()
            table_hs6['code'] = 1
            table_hs6 = table_hs6.set_index(['hs6', 'year']).unstack(level='year')
            #-Drop TopLevel Name in Columns MultiIndex-#
            table_hs6.columns = table_hs6.columns.droplevel()   #Removes Unnecessary 'code' label
            if wmeta:
                #-Add Product Name-#
                pdata = self.product_data.rename(columns={"Code" : "hs6"}).set_index(keys=["hs6"])
                table_hs6 = table_hs6.merge(pdata, left_index=True, right_index=True)
                table_hs6 = table_hs6.reset_index().set_index(keys=['hs6', 'Description'])
            return table_hs6
    

    #--------------#
    #---Datasets---#
    #--------------#


    def construct_sitc_dataset(self, data_type, dataset, product_level, sitc_revision=2, report=True, dataset_object=False, force=False, verbose=True):
        """
        Constructor of Predefined SITC Datasets

        Status: IN-WORK

        Parameters
        ----------
        data_type       :   str
                            Specify type of data ('trade', 'export', 'import')
        dataset         :   str
                            Specify Predefined set of Parameters ('A', 'B' etc.)
        product_level   :   int
                            Specify a Product Level for Final Dataset (1, 2, 3, or 4)
        sitc_revision   :   int, optional(default=2)
                            Specify SITC Revision
        dataset_object  :   bool, optional(default=False)
                            Specify if the method should return an nberwtf object

        ..  Notes
            -----
            1. Other options defined by construct_sitc() function can be set using constructor_dataset options dict. 

        ..  Future Work
            -----------
            1. If dataset == dict then use it as the dataset parameters for compilation

        """
        #-Dataset Definitions-#
        from .constructor_dataset import SITC_DATASET_DESCRIPTION, SITC_DATASET_OPTIONS
        #-Checks-#
        if self.complete_dataset == False:
            if force:
                print "[WARNING] Building SITC Dataset with Subset of the full Dataset"
            else:
                raise ValueError("This Method requires a complete RAW dataset")
        #-Parse Options-#
        if type(dataset) == dict:
            print "[INFO] Using Passed Arguments as Dataset Construction Options"
            OPTIONS = dataset
            #-Note: self.notes with description etc needs to be set using this option-#
        elif dataset not in SITC_DATASET_OPTIONS.keys():
            raise ValueError("Specified Dataset (%s) is not found in the SITC_DATASET_OPTIONS property")
        else:
            #-Use Predefined Dictionaries as defined in constructor_dataset.py-#
            DESCRIPTION = SITC_DATASET_DESCRIPTION[dataset]
            OPTIONS = SITC_DATASET_OPTIONS[dataset]
            #-OpString-#
            str_kwargs = [", %s=%s" % (key, SITC_DATASET_OPTIONS[dataset][key]) for key in sorted(SITC_DATASET_OPTIONS[dataset].keys())]
            op_string = u"(construct_sitc_dataset(data_type=%s, dataset=%s, product_level=%s, sitc_revision=%s, report=%s, dataset_object=%s, verbose=%s%s))" % (data_type, dataset, product_level, sitc_revision, report, dataset_object, verbose, "".join(str_kwargs))
            self.notes = op_string #-Save Settings-#
        #-MAIN WORK-#
        from .constructor_dataset_sitc import construct_sitc as construct_dataset
        self.dataset = construct_dataset(self.dataset, data_classification=self.classification, data_type=data_type, level=product_level, revision=sitc_revision, multiindex=False, verbose=verbose, **OPTIONS)
        self.dataset_name = "SITCR%s-%s"%(sitc_revision, str(dataset))
        #-Construct Report-#
        if report:
            rdf = self.__raw_data                                                     #Note: This produces a copy!
            #-Year Values-#
            rdfy = rdf.rename(columns={"t" : "year", "v" : "value"}).groupby(['year']).sum()['value'].reset_index()
            dfy = self.dataset.groupby(['year']).sum()['value'].reset_index()
            y = rdfy.merge(dfy, how="outer", on=['year']).set_index(['year'])
            y['%'] = y['value_y'] / y['value_x'] * 100
            report =    "Report %s\n"%(op_string) + \
                        "---------------------------------------\n"
            for year in self.years:
                report += "This dataset in year: %s captures %s percent of Total Values\n" % (year, int(y.ix[year]['%']))
            print report
        #-Update Attributes-#
        self.classification = 'SITC'
        self.revision = sitc_revision
        self.level = product_level
        self.data_type = data_type
        #-OpString-#
        update_operations(self, op_string)
        #-Dataset Option-#
        if dataset_object:
            return self.to_dataset()

    # -------------------------------------------------------------------------------------- #
    # -- NOTICE: With the Addition of construct_sitc_dataset() this section is deprecated -- #
    # -------------------------------------------------------------------------------------- #

    # ------------------------------------ START REMOVAL ----------------------------------- #

    #-Self Contained Datasets-#

    def construct_dataset_SC_CP_SITCR2L3_Y1998to2012(self, data_type, dataset_object=True, source_institution='un', report=True, verbose=True):
        """
        Construct a Self Contained (SC) Direct Action Dataset at the Country x Product Level (SITC Level 3)
        **Note:** Self Contained methods reduce the Need to Debug other routines and methods. 
        The other methods are however useful to diagnose issues and to understand properties of the dataset

        STATUS: DEPRECATED

        Dataset:
            
            Source Classification -> HS96

        Parameters
        ----------
        data_type       :   str
                            Specify data type: 'trade', 'export', 'import'
                            'export' will include values from a country to any region (including NES, and Nuetral Zone etc.)
                            'import' will include values to a country from any region (including NES, and Nuetral Zone etc.)
        dataset_object  :   bool, optional(default=True)
                            Return data as an object. False returns a dataframe
        source_institution  :   str, optional(default='un')
                                Specify source institution
        report          :   bool, optional(default=True)
                            Compile and Issue a Report

        Note
        ----
        1. NBER Adjustment will happen when joining the two datasets together
        2. Method has useful comments throughtout for further information
        """

        warnings.warn("[DEPRECATED] in prefernence for .construct_sitc_dataset() separate method")

        #-Helper Functions-#
        def merge_iso3c_and_replace_iso3n(data, cntry_data, column):
            " Merge ISO3C and Replace match on column (i.e. eiso3n)"
            data = data.merge(cntry_data, how='left', left_on=[column], right_on=['iso3n'])
            del data['iso3n']
            del data[column]
            data.rename(columns={'iso3c' : column[0:-1]+'c'}, inplace=True)
            return data

        def dropna_iso3c(data, column):
            " Drop iiso3c or eiso3c isnull() values "
            if column == 'iiso3c':
                data.drop(data.loc[(data.iiso3c.isnull())].index, inplace=True)
            elif column == 'eiso3c':
                data.drop(data.loc[(data.eiso3c.isnull())].index, inplace=True)
            return data

        #-Start from RawData-#
        #--------------------#
        data = self.raw_data 
        #-Obtain Key Index Variables-#
        data.rename(columns={'t' : 'year', 'i' : 'eiso3n', 'j' : 'iiso3n', 'v' : 'value', 'q': 'quantity'}, inplace=True)   #'hs6' is unchanged
        #-Exclude Quantity-#
        del data['quantity']
        #-Import Country Codes to ISO3C-#
        #-------------------------------#
        self.load_country_data(fix_source=True, standard_names=True, verbose=True)  #Using this due to fix required on source files and it's data is attached to self.country_data
        cntry_data = self.country_data[['iso3n', 'iso3c']]
        #-Import Product Concordance-#
        #----------------------------#
        if self.classification == "HS02": 
            from pyeconlab.trade.concordance import HS2002_To_SITCR2       
            concordance = HS2002_To_SITCR2(sitc_level=3).concordance
        elif self.classification == "HS96":
            from pyeconlab.trade.concordance import HS1996_To_SITCR2        
            concordance = HS1996_To_SITCR2(sitc_level=3).concordance
        elif self.classification == "HS92":
            from pyeconlab.trade.concordance import HS1992_To_SITCR2        
            concordance = HS1992_To_SITCR2(sitc_level=3).concordance
        else:
            raise ValueError("No Appropriate Concordance was found for classification (%s)" % self.classification)
        #-Add Special Cases to the concordance-#
        for k,v in  self.adjust_hs6_to_sitc[self.classification].items():
            concordance[k] = v
        #-Change Value Units-#
        #--------------------#
        # data['value'] = data['value']*1000                            #currently keeping value units in 1000's (similar to nber)
        # self.units_value_str = "$'s"
        #-Collapse Trade Data based on data option-#
        #------------------------------------------#
        if data_type == "trade":
            #-Merge in ISO3C-#
            #----------------#
            data = merge_iso3c_and_replace_iso3n(data, cntry_data, column='eiso3n')
            data = merge_iso3c_and_replace_iso3n(data, cntry_data, column='iiso3n')
            print "[WARNING] Dropping Countries where iso3c has null() values will remove country export/import from NES, and other regions!"
            data = dropna_iso3c(data, column='eiso3c')
            data = dropna_iso3c(data, column='iiso3c')
            #-Merge in SITCR2 Level 3-#
            #-------------------------#
            data['sitc3'] = data['hs6'].apply(lambda x: concord_data(concordance, x, issue_error=np.nan))
            del data['hs6']
            data = data.groupby(['year', 'eiso3c', 'iiso3c', 'sitc3']).sum()
            self.classification = 'SITC'                                                                        #duplication could be reduced here using a function
            self.revision = 2
            self.level = 3
            print "[Returning] BACI HS96 Source => TRADE data for SITCR2 Level 3 with ISO3C Countries"
        elif data_type == "export" or data_type == "exports":
            #-Export Level-#
            #--------------#
            del data['iiso3n']
            data = data.groupby(['year', 'eiso3n', 'hs6']).sum().reset_index()
            #-Merge in ISO3C-#
            #----------------#
            data = merge_iso3c_and_replace_iso3n(data, cntry_data, column='eiso3n')
            data = dropna_iso3c(data, column='eiso3c')
            #-Merge in SITCR2 Level 3-#
            #-------------------------#
            data['sitc3'] = data['hs6'].apply(lambda x: concord_data(concordance, x, issue_error=np.nan))
            del data['hs6']
            data = data.groupby(['year', 'eiso3c', 'sitc3']).sum()
            self.classification = 'SITC'                                                                        #duplication could be reduced here using a function
            self.revision = 2
            self.level = 3
            print "[Returning] BACI HS96 Source => EXPORT data for SITCR2 Level 3 with ISO3C Countries"
        elif data_type == "import" or data_type == "imports":
            #-Import Level-#
            #--------------#
            del data['eiso3n']
            data = data.groupby(['year', 'iiso3n', 'hs6']).sum().reset_index()
            #-Merge in ISO3C-#
            #----------------#
            data = merge_iso3c_and_replace_iso3n(data, cntry_data, column='iiso3n')
            data = dropna_iso3c(data, column='iiso3c')
            #-Merge in SITCR2 Level 3-#
            #-------------------------#
            data['sitc3'] = data['hs6'].apply(lambda x: concord_data(concordance, x, issue_error=np.nan))
            del data['hs6']
            data = data.groupby(['year', 'iiso3c', 'sitc3']).sum()
            self.classification = 'SITC'                                                                        #duplication could be reduced here using a function
            self.revision = 2
            self.level = 3
            print "[Returning] BACI HS96 Source => IMPORT data for SITCR2 Level 3 with ISO3C Countries"
        else:
            raise ValueError("'data' must be 'trade', 'export', or 'import'")
        #-Replace Dataset-#
        self.dataset = data
        self.data_type = data_type
        self.notes = u"HS96L6 to SITCR2L3 => Computed with options: dataset_object=%s, source_institution=%s" % (dataset_object, source_institution)
        #-Report-#
        if report:
            rdf = self.raw_data.rename(columns={'t' : 'year', 'v' : 'value'})
            #-Year Values-#
            rdfy = rdf.groupby(['year']).sum()['value'].reset_index()
            dfy = data.reset_index().groupby(['year']).sum()['value'].reset_index()
            y = rdfy.merge(dfy, how="outer", on=['year']).set_index(['year'])
            y['%'] = y['value_y'] / y['value_x'] * 100
            report =    "Report construct_default_simple_sitc3(options)\n" + \
                        "---------------------------------------\n"
            for year in self.years:
                report += "This dataset in year: %s captures %s percent of Total 'World' Values\n" % (year, int(y.ix[year]['%']))
            print report
        #-Return Dataset Object-#
        if dataset_object:
            return self.to_dataset()
    
    # ------------------------------------ END REMOVAL ----------------------------------- #

    def attach_attributes_to_dataset(self, df):
        """ 
        Attach Attributes to the Dataset DataFrame for Transfer when building new objects
        """
        #-Attach Attributes to dataset object for transfer-#
        df.txf_name             = self.name
        df.txf_data_type        = self.data_type
        df.txf_classification   = self.classification
        df.txf_revision         = self.revision
        df.txf_complete_dataset = self.complete_dataset
        df.txf_notes            = self.notes
        df.txf_source_revision  = self.source_revision
        df.txf_units_value_str  = self.units_value_str
        return df

    def to_dataset(self, generic=False):
        """
        Convert to a Dataset Object
        """
        #-Prepare Data for Object Standard Input-#
        data = self.dataset
        if type(data.index) == pd.MultiIndex:
            data = data.reset_index()
        data = data.rename_axis({'sitc%s'%self.level : 'productcode'}, axis=1)
        data = self.attach_attributes_to_dataset(data)                          #Alternatively we could create the object and then attach names directly!
        if self.data_type == "trade":
            if generic:
                return CPTradeData(data)
            return BACITradeData(data)
        elif self.data_type == "export":
            if generic:
                return CPExportData(data)
            return BACIExportData(data)
        elif self.data_type == "import":
            if generic:
                return CPImportData(data)
            return BACIImportData(data)
        else:
            raise ValueError("data_type (%s) is not 'trade', 'export' or 'import'" % self.data_type)    


    #---------------------#
    #---COMPARISON WORK---#
    #---------------------#

    def compare_dataset_with_raw(self, verbose=True):
        """ 
        Compare Yearly Values with Initial Raw Data

        STATUS: NOT IMPLEMENTED
        """
        raise NotImplementedError

    #-----------------#
    #---FUTURE WORK---#
    #-----------------#


    def load_raw_from_rar(self, verbose=False):
        """ 
        Load Raw Data from RAR Files 
        
        Status: ON-HOLD

        Note
        ----
        [1] this will require the installation of unrar!?
        """
        raise NotImplementedError("Currenlty considering the use of unrar!")
        data = pd.DataFrame()
        for year in self.years:
            fn = self.source_dir + 'baci' + self.classification.strip('HS') + '_' + str(year) + '.rar'
            print "[INFO] Opening: %s" % fn
            rar = rarfile.RarFile(fn)
            #- Incomplete [currently debating if want to build dependancy on unrar -#