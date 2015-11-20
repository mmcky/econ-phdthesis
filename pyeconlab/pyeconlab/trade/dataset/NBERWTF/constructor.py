# -*- coding: utf-8 -*-
"""
NBERWTF Constructor
===================

Compile NBERFeenstra RAW data Files and Perform Data Preparation Tasks

It is the CORE responsibility of this module to clean, prepare and investigate the data.
The NATURE of the data shouldn't be changed in this class. 
For example, routines for collapsing the bilateral flows to exports should be contained in NBERWTF 

Conventions
-----------
__raw_data  : Should be an exact copy of the imported files and protected. 
dataset     : Contains the Modified Dataset

Notes
-----
1. Load Times ::
    
    [1] a = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR, ftype='hdf') [~41s]      From a complevel=9 file (Filesize: 148Mb)
    [2] a = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR, ftype='hdf') [~34.5s]    From a complevel=0 file (FileSize: 2.9Gb)
    [3] a = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR, ftype='dta') [~14min 23s]

2. Convert Times (from a = NBERWTFConstructor(source_dir=SOURCE_DATA_DIR, ftype='dta')) ::

    [1] a.convert_raw_data_to_hdf(complevel=0) [1min 21seconds]
    [2] a.convert_raw_data_to_hdf(complevel=9) [2min 54seconds]

    Outcome: Default complevel=9 (Doubles the conversion time but load time isn't significantly deteriorated)

Future Work
-----------
1. A More memory efficient for Export and Import Data Only would be to collapse prior to adjustments?
2. Clarify operators on raw_data vs. dataset
3. Simplify this!
4. Propose to Leave this as a Data Exploration Class and REMOVE dataset construction as their requirements can be very specific. 
"""

#-Future-#
from __future__ import division
#-System Imports-#
import os
import copy
import re
import gc
import pandas as pd
from pandas.util.testing import assert_frame_equal
import numpy as np
import countrycode as cc
import cPickle as pickle
import warnings
warnings.simplefilter("always")
#-Package Imports-#
from .base import NBERWTF
from .dataset import NBERWTFTradeData, NBERWTFExportData, NBERWTFImportData 
from pyeconlab.util import  from_series_to_pyfile, check_directory, recode_index, merge_columns, check_operations, update_operations, from_idxseries_to_pydict, \
                            countryname_concordance, concord_data, random_sample, find_row, assert_merged_series_items_equal
from pyeconlab.trade.classification import SITC

#-Debug and Testing-#
# from memory_profiler import profile

# - Data in data/ - #
this_dir, this_filename = os.path.split(__file__)
#DATA_PATH = check_directory(os.path.join(this_dir, "data"))
META_PATH = check_directory(os.path.join(this_dir, "meta"))

#-Concordances-#
from pyeconlab.country import iso3n_to_iso3c, iso3n_to_name             #Why does this import prevent nosetests from running?               

class NBERWTFConstructor(NBERWTF):
    """
    Data Constructor / Compilation Object for Feenstra NBER World Trade Data
    
    Parameters
    ----------
    see __init__()

    Notes
    -----
    1. Source Data Attributes ::
        
        Years:  1962 to 2000
        Classification: SITC R2 L4 (Not Entirely Standard)
        Notes:  Pre-1974 and Pre-1984 care is required for constructing intertemporally or dynamically consistent data (Check .dynamic_dataset())

    2. Source File Interface ::
    
        Source Directory:   Specified by source_dir
        Filename Format:    wtf##.dta where ## is 62-00     [03/07/2014]
                            Note: Files currently need to be updated to the latest Stata .dta format MANUALLY

        Variables
        ---------
        DOT         :   Direction of trade (1=Data from importer, 2=Data from exporter) [DOT=1 => CIF; DOT=2 => FOB]
        SITC        :   Standard International Trade Classification Revision 2
        ICode       :   Importer country code
        ECode       :   Exporter country code
        Importer    :   Importer country name
        Exporter    :   Exporter country name
        Unit        :   Units or measurement (see below)
        Year        :   4-digit year
        Quantity    :   Quantity (only for years 1984 – 2000)
        Value       :   Nominal Thousands of US dollars


    3. Summary of Important Documentation ::

        Values
        ------
            years   :   1962-2000
            units   :   Thousands of US dollars

        Quantity
        --------
            years   :   1984-2000
            units   :   A Area (1,000 square meters) 
                        H Energy (1,000 kilowatt hours)
                        K Weight (kilograms)
                        L Length (1,000 meters)
                        N Units (number of items)
                        P Pairs (number of pairs)
                        V Volume (cubic meters)
                        W Weight (metric tons)

        ProductCodes
        -------------

            SITC Rev 1
            ----------
                years   :   1962-1983 [Converted to SITC R2 [Section 2 of Documentation PDF]]
                                      [Table #3: SITC Rev1 and SITC Rev2 Concordance]
                            Note: This produces some dynamic inconsistencies if working over time. 

            SITC Rev 2
            ----------
                years   :   1984-2000

             Additional Notes
             ----------------
            
            1.  A and X codes for 1984-2000 (Not all Countries)

            2.  Codes Ending in 0: "4-digit SITC codes ending in zero were introduced into the data because we substituted the U.S.
                values of exports and imports in place of the UN values, whenever the U.S. was a partner. In the
                U.S. values, an SITC Rev. 2 code ending in zero has the same meaning as a code ending in A or
                X; that is, it represents trade within that 3-digit code that could not be accurately assigned to a 4-
                digit code. For example, trade within SITC 0220 really means trade within one of the SITC
                industries 0222, 0223, or 0224." [FAQ]

            3.  I am not currently sure what these SITC codes are. 
                I have requested further information from Robert Feenstra
                They only occur in 1962 - 1965 and have a combined value of: $6,683,000
                0021    [Associated only with Malta]
                0023    [Various Eastern European Countries and Austria]
                0024
                0025
                0031
                0035
                0039
                2829    [Assume: 282 NES. The MIT MediaLabs has this as “Waste and scrap metal of iron or steel” ]
                Note: Currently these items will be dropped. See delete_productcode_issues_with_raw_data():

            4. There are currently 28 observations with no SITC4 codes. See issues_with_raw_data(),  See delete_issues_with_raw_data()

        CountryCodes
        ------------
            1. icode & ecode are structured: XXYYYZ => UN-REGION [2] + ISO3N [3] + Modifier [1]
            2. Default Dataset should Match on ISO3N and merge in ISO3C from pyeconlab.country.concordance (iso3n_to_iso3c)

    4.  Types of Operations
        -------------------
        1. Reduction/Collapse  :   This collapses data and applies a function like ADD to lines with the same idx 
                                    These need to happen BEFORE adjust methods
        2. Merge               :   Merge methods that add data (such as Hong Kong adjusted data etc.)
        3. Adjust              :   Adjust Methods alter the data but don't change it's length (spliting codes etc.)

        Order of Operations:    Reduction/Collapse -> Merge -> Adjust

    5.  Organisation of Class
        ----------------------
        1. Attributes
        2. Internal Methods (__init__())
        3. Properties
        4. Supplementary Data (Loading Data)
        5. Operations on Dataset (Adjusting self._dataset, cleaning tasks etc)
        6. Construct Datasets (NBERWTF)*
        7. Supporting Functions
        8. Generate Meta Data Files For Inclusion into Project Package (meta/)
        9. Converters (hd5 Files etc.)
        10. Generate Data to support construction of tests
        11. Construct Case Study Data
    *Note: Predefined Datasets are only considered here. Access to the dataset functions is possible through the constructor_dataset_* family of functions. 

    6.  Additional Notes
        ----------------
        1. There should only be ONE assignment in __init__ to the __raw_data attribute [Is there a way to enforce this?]
            Any modification prior to returning an NBERWTF object should be carried out on "._dataset"
        2. All Methods in this Class should operate on **NON** Indexed Data
        3. This Dataset Requires ~25GB of RAM

    ..  Future Work:
        -----------
        1. Update Pandas Stata to read older .dta files (then get wget directly from the website)
        2. When constructing meta/ data for inclusion in the package, it might be better to import from .dta files directly the required information
            For example. CountryCodes only needs to bring in a global panel of countrynames and then that can be converted to countrycodes
            [Update: This is currently not possible due to *.dta files being binary]
        3. Move op_string work and turn it into a decorator function
        4. Construct a BASE Constructor Class that contains generic methods (like IO)
      **5. Construct more formalised structure to ensure raw_data isn't operated on except split_countrycodes (OR should this always work on dataset?)
        6. Simply this class rather than keeping it so general
    """

    # - Attributes - #

    _exporters          = None                      #These are defined due to property check to None. But these could be removed if property just try/except
    _importers          = None 
    _country_list       = None
    _dataset            = None                                  

    # - Dataset Attributes - #

    _name               = u'NBERWTF'
    classification      = u'SITC'
    revision            = 2
    source_web          = u"http://cid.econ.ucdavis.edu/nberus.html"
    source_last_checked = np.datetime64('2014-07-04')
    complete_dataset    = False                                         #Make this harder to set with self.__?
    years               = []
    level               = 4
    operations          = ''
    _available_years    = xrange(1962,2000+1,1)
    _fn_prefix          = u'wtf'
    _fn_postfix         = u'.dta'
    _source_dir         = None
    _units_value        = 1000
    _units_value_str    = "$1000's"
    _file_interface     = [u'year', u'icode', u'importer', u'ecode', u'exporter', u'sitc4', u'unit', u'dot', u'value', u'quantity']
    notes               = ""

    # - Other Data in NBER Feenstra WTF -#
    _supp_data          = dict
    
    # - Dataset Reference - #
    __raw_data_hdf_fn   = u'wtf62-00_raw.h5'
    __raw_data_hdf_yearindex_fn = u'wtf62-00_yearindex.h5'
    __cache_dir = u"cache/"

    def __init__(self, source_dir, years=[], ftype='hdf', standardise=False, apply_fixes=True, skip_setup=False, force=False, reduce_memory=False, verbose=True):
        """ 
        Load RAW Data into Object

        Parameters
        ----------
        source_dir      :   str
                            Folder that contains the raw stata files (*.dta)
        years           :   list, optional(default=[])
                            Apply a Year Filter [Default: ALL]
        ftype           :   str, optional(default='hdf')
                            File Type ['dta', 'hdf'] [Default 'hdf' -> however it will generate one if not found from 'dta']
        standardise     :   bool, optional(default=False)
                            Include Standardised Codes (Countries: ISO3C etc.)
        apply_fixes     :   bool, optional(default=True)
                            Apply Fixes to NBER Raw data in line with NBER FAQ
        skip_setup      :   bool, optional(default=False)
                            [Testing] This allows you to skip __init__ setup of object to manually load the object with csv data etc. 
                            This is mainly used for loading test data to check attributes and methods etc. 
        force           :   bool, optional(default=False)
                            If not working with the full dataset you may enter force=True to run standardisation routines etc.
                            [Warning: This will not return Intertemporally Consistent Concordances]
        reduce_memory   :   bool, optional(default=False)
                            This will delete self.__raw_data after initializing self._dataset with the raw_data
                            [Warning: This will render properties that depend on self.__raw_data inoperable]
                            Usage: Useful when building datasets to be more memory efficient as the operations don't require a record of the original raw_data
                            [Default: False] Only Saves ~2GB of RAM
        
        """
        #-Assign Source Directory-#
        self._source_dir    = check_directory(source_dir)   # check_directory() performs basic tests on the specified directory
        self.data_type      = u"trade"
        self._apply_fixes   = apply_fixes
        #-Parse Skip Setup-#
        if skip_setup == True:
            print "[INFO] Skipping Setup of NBERWTFConstructor!"
            self.__raw_data     = None                                              #Allows to be assigned later on
            return None
        #-Setup Object-#
        if verbose: print "Fetching NBER-Feenstra Data from %s" % source_dir
        if years == []:
            self.complete_dataset = True    # This forces object to be imported based on the whole dataset
            years = self._available_years   # Default Years
        #-Assign to Attribute-#
        self.years  = years
        # - Fetch Raw Data for Years - #
        if ftype == 'dta':
            self.load_raw_from_dta(verbose=verbose)
        elif ftype == 'hdf':
            try:
                self.load_raw_from_hdf(years=years, verbose=verbose)
            except:
                print "[INFO] Your source_directory: %s does not contain h5 version in cache folder.\n Starting to compile one now ...."
                #-Check Cache Folder Exists-#
                if not os.path.exists(self._source_dir + self.__cache_dir):
                    print "[INFO] Setting up a Cache Directory ..."
                    os.makedirs(self._source_dir + self.__cache_dir)
                self.load_raw_from_dta(verbose=verbose)
                self.convert_raw_data_to_hdf(verbose=verbose)           #Compute hdf file for next load
                self.convert_stata_to_hdf_yearindex(verbose=verbose)    #Compute Year Index Version Also
        else:
            raise ValueError("ftype must be dta or hdf")  

        #-Reduce Memory-#
        if reduce_memory:
            self._dataset = self.__raw_data                                     #Saves ~2Gb of RAM (but cannot access raw_data)
            self.__raw_data = None
        else:
            self._dataset = self.__raw_data.copy(deep=True)                     #[Default] pandas.DataFrame.copy(deep=True) is much more efficient than copy.deepcopy()

        #-Apply Fixes-#
        if apply_fixes:
            self.fix_raw_data(verbose=verbose) 

        #-Simple Standardization-#
        if standardise == True: 
            if verbose: print "[INFO] Running Interface Standardisation ..."
            self.standardise_data(force=force, verbose=verbose)
        gc.collect()
        


    def __repr__(self):
        string = "Class: %s\n" % (self.__class__)                           + \
                 "Years: %s\n" % (self.years)                               + \
                 "Complete Dataset: %s\n" % (self.complete_dataset)         + \
                 "Source Last Checked: %s\n" % (self.source_last_checked)
        #-Parse TestData Indicator-#
        try:
            if self.test_data == True:
                string += "\n[WARNING] TEST DATA LOADED IN OBJECT\n"
        except:
            pass
        return string
        
    # - Object Properties - #

    def set_fn_prefix(self, prefix):
        self._fn_prefix = prefix

    def set_fn_postfix(self, postfix):
        self._fn_postfix = postfix

    # - Raw Data Properties - #

    @property
    def raw_data(self):
        """ 
        Raw Data Property (returns new data object)
        
        Notes
        -----
        1. This isn't very memory efficient
        2. Should this be complicated by loading raw from an HDF File of the Dataset?
        """ 
        try:
            return self.__raw_data.copy(deep=True)                              #Always Return a Copy
        except:                                                                 #Load from h5 file
            self.load_raw_from_hdf(years=self.years, verbose=False)
            return self.__raw_data

    def del_raw_data(self, force):
        """ 
        Delete Raw Data
        """
        if force == True:
            self.__raw_data = None

    @property
    def raw_data_operations(self):
        """
        Check RAW data operations. 

        Notes 
        -----
        1. This should be 'None' now that ALL operations have moved to dataset. Consider DEPRECATED?
        """  
        try: 
            return self.__raw_data.operations
        except: 
            return None                         #No Operations Applied

    def set_raw_data(self, df, force=False):
        """
        Force Set raw_data (used for testing)
        """
        if type(self.__raw_data) == pd.DataFrame:
            if force == False:
                print "[WARNING] To force the replacement of raw_data use 'force'=True"
                return None
        self.__raw_data =  df   #Should this make a copy?

    @property
    def exporters(self):
        """ 
        Returns List of Exporters (from Raw Data)
        """
        if self._exporters == None:
            self._exporters = list(self.__raw_data['exporter'].unique())
            self._exporters.sort()
        return self._exporters  

    def global_exporter_list(self):
        """
        Return Global Sorted Unique List of Exporters
        Useful as Input to Concordances such as NBERFeenstraExporterToISO3C

        ..  Future Work
            -----------
            1. Should I write an Error Decorator? 
        """
        if self.complete_dataset == True:
            return self.exporters
        else:
            raise ValueError("Raw Dataset must be complete - currently %s years have been loaded" % self.years)

    @property 
    def exporters_iso3c(self):
        if not check_operations(self, "(add_iso3c)"):                           #Check if Operation has been conducted
                self.add_iso3c(verbose=True)
        return self.dataset.eiso3c.unique()

    @property
    def importers(self):
        """
        Returns List of Importers (from Raw Data)
        """
        if self._importers == None:
            self._importers = list(self.__raw_data['importer'].unique())
            self._importers.sort()
        return self._importers
    
    def global_importer_list(self):
        """
        Return Global Sorted Unique List of Importers
        Useful as Input to Concordances such as NBERFeenstraImporterToISO3C
        """
        if self.complete_dataset == True:
            return self.importers
        else:
            raise ValueError("Raw Dataset must be complete - currently %s years have been loaded" % self.years)

    @property
    def country_list(self):
        """
        Returns a Country List (Union of Exporters and Importers)
        """
        if self._country_list == None:
            self._country_list = list(set(self.exporters).union(set(self.importers)))
            self._country_list.sort()
        return self._country_list   

    def supp_data(self, item):
        """
        Return an Item from the Supplementary Data Dictionary
        """
        return self._supp_data[item]

    @property 
    def supp_data_items(self):
        """
        Return a List of Items Available in Supplementary Data
        """
        items = []
        for key in self._supp_data.keys():
            if type(self._supp_data[key]) == pd.DataFrame:
                items.append(key)
        return sorted(items)

    @property 
    def dataset(self):
        """
        Dataset contains the Exportable Result to NBERWTF
        """
        try:
            return self._dataset 
        except:                                             #-Raw Data Not Yet Copied-#
            self._dataset = self.__raw_data.copy(deep=True)
            return self._dataset

    def reset_dataset(self, verbose=True):
        """
        Reset Dataset to raw_data
        """
        if type(self.__raw_data) != pd.DataFrame:
            raise ValueError("RAW DATA is not a DataFrame! Most likely it has been deleted")
        if verbose: print "[INFO] Reseting Dataset to Raw Data"
        del self._dataset                                                                           #Clean-up old dataset
        self._dataset = self.__raw_data.copy(deep=True)
        if self._apply_fixes:
            self.fix_raw_data(verbose=verbose)
        self.operations = ''
        self.level = 4

    def set_dataset(self, df, force=False, reset_operations=True):
        """ 
        Check if Dataset Exists Prior to Assignment
        
        Notes 
        -----
        1. Is this ever going to be used? Consider DEPRECATED?
        """
        if type(self._dataset) == pd.DataFrame:
            if force == False:
                print "[WARNING] The dataset attribute has previously been set. To force the replacement use 'force'=True"
                return None
        self._dataset =  df                             #Should this make a copy?
        if reset_operations:
            print "[INFO] Reseting operations attribute"
            self.operations = ''

    @property
    def verify_raw_data(self):
        """
        Simple verification of raw data files

        Notes
        -----
        1. This should be moved to tests/
        """
        if len(self.raw_data) != 27573764:
            return False
        if self.raw_data['value'].sum() != 337094011179.25201:
            return False 
        if self.raw_data['quantity'].sum() != 10302685608925.896:
            return False
        return True

    @property 
    def yearly_world_values(self):
        """
        Construct yearly world values
        """
        rdf = self.raw_data
        rdf = rdf.loc[(rdf.importer=="World") & (rdf.exporter == "World")]
        #-Year Values-#
        rdfy = rdf[['year', 'value']].groupby(['year']).sum()
        return rdfy
    
    def yearly_product_world_values(self, level=4):
        """
        Construct yearly product world values (sitc4)

        Parameters
        ----------
        level   :   int, optional(default=4)
                    Specify SITC Level to Return
        """
        rdf = self.raw_data
        rdf = rdf.loc[(rdf.importer=="World") & (rdf.exporter == "World")]
        #-Year Product Values-#
        rdfpy = rdf[['year', 'sitc4', 'value']].groupby(['year', 'sitc4']).sum()
        #-Simple Adjust Level-#
        if level < 4:
            rdfpy.reset_index(inplace=True)
            rdfpy['sitc%s'%level] = rdfpy['sitc4'].apply(lambda x: x[:level])
            rdfpy = rdfpy[['year', 'sitc%s'%level, 'value']].groupby(['year', 'sitc%s'%level]).sum()
        return rdfpy

    @property
    def yearly_country_values(self):
        """ 
        Construct yearly country values (from raw data)
        """
        if self.operations != "":
            raise ValueError("This operation needs to be conducted on a fresh dataset with no operations. You can reset the data with .reset_dataset()")
        self.add_iso3c()
        rdf = self.dataset.copy(deep=True) 
        rdf = rdf.loc[(rdf.importer=="World") & (rdf.exporter!="World")]
        #-Year Country Values-#
        rdfpy = rdf[['year', 'eiso3c', 'value']].groupby(['year', 'eiso3c']).sum()
        return rdfpy

    def stats(self, dataset=True, basic=False, extended=False, dlimit=10):
        """
        Print Some Basic Summary Statistics about the Dataset or RAW DATA
        """
        if dataset:
            df = self.dataset
            msg = "Dataset (%s) Statistics\n-------------------------------\n" % self._name
        else:
            df = self.raw_data
            msg = "Raw Data (%s) Statistics\n-------------------------------\n" % self._name
        msg += "Years: %s\n" % self.years
        msg += "Observations: %s; Variables: %s\n" % (df.shape[0], df.shape[1])
        if basic:
            print msg
            return None
        for col in df.columns:
            if col in ['value', 'quantity']:
                continue
            uniq = list(df[col].unique())
            msg += "Column: '%s' has %s Unique Entries\n" % (col, len(uniq))
            if extended:
                    if len(uniq) >= dlimit:
                        msg += "Items => %s ... %s\n" % (uniq[:int(dlimit/2)], uniq[-int(dlimit/2):])
                    else:
                        msg += "Items => %s\n" % uniq
        print msg 

    def exporter_total_values(self, data, key='exporter', year=False):
        """ 
        Return Exporter Total Values DataFrame (Optional: by Year)

        Parameters
        ----------
        dataset :   pd.DataFrame(Class Property)
                    property (a.raw_data or a.dataset)
        key     :   str, optional(default='exporter')
                    Specify 'exporter' or 'ecode'
        year    :   bool, optional(default=False)
                    Return results by year [True/False]

        """
        if year:
            return data.groupby(['year', key]).sum()['value']
        return data.groupby([key]).sum()['value']

    def importer_total_values(self, data, key='importer', year=False):
        """ 
        Return Importer Total Values DataFrame (Optional: by Year)

        Parameters
        ----------
        dataset :   pd.DataFrame(Class Property)
                    property (a.raw_data or a.dataset)
        key     :   str, optional(default='importer')
                    Specify 'importer' or 'icode'
        year    :   bool, optional(default=False)
                    Retrun results by year [True/False]

        """
        if year:
            return data.groupby(by=['year', key]).sum()['value']
        return data.groupby(by=[key]).sum()['value']

    def to_exports(self, dataset=False, verbose=True):
        """ 
        Collapse Data to Exporters

        Notes
        -----
        1. Care must be taken with idx construction

        """
        if verbose: print "[INFO] Collapsing to Exports"
        if dataset:
            data = self.dataset.copy(deep=True)
        else:
            data = self.raw_data.copy(deep=True)
        subidx = set(data.columns)
        for ritem in ['importer', 'icode', 'quantity', 'unit', 'iiso3c', 'iiso3n', 'iregion', 'imod', 'dot']:           #Add and Removal Lists to Dataset Object?
            try:
                subidx.remove(ritem)
            except:
                pass
        gidx = subidx.copy()
        gidx.remove('value')
        data = data[list(subidx)].groupby(list(gidx)).sum()         #Aggregate 'value'
        return data

    # ------ #
    # - IO - #
    # ------ #

    def load_raw_from_dta(self, verbose=True):
        """
        Load RAW ``*.dta`` files from a source_directory
        
        Notes
        -----
        1. Move to Generic Class of DatasetConstructors?
        2. This should try and load from a raw_data file first rather than raw_data_year
        """
        if verbose: print "[INFO]: Loading RAW [.dta] Files from: %s" % (self._source_dir)
        self.__raw_data     = pd.DataFrame()                                    
        for year in self.years:
            fn = self._source_dir + self._fn_prefix + str(year)[-2:] + self._fn_postfix
            if verbose: print "Loading Year: %s from file: %s" % (year, fn)
            self.__raw_data = self.__raw_data.append(pd.read_stata(fn))
        self.__raw_data = self.__raw_data.reset_index()                                     #Otherwise Each year has repeated obs numbers
        del self.__raw_data['index']                                                        #Remove Old Index
        gc.collect()

    def load_raw_from_hdf(self, years=[], use_raw_years_fl=False, gc_collect=True, verbose=True):
        """
        Load HDF Version of RAW Dataset from a source_directory
        
        Parameters
        ----------
        years               :   list, optional(default=[])
                                Specify Years to Load from HDF 
        use_raw_years_fl    :   bool, optional(default=False)
                                Use raw_years HDF file. 
        gc_collect          :   bool, optional(default=True)
                                Garbage Collection Objects to Ensure Memory is released. 

        Note   
        -----        
        1. To construct your own hdf version requires to initially load from NBER supplied RAW dta files. Then use Constructor method ``convert_source_dta_to_hdf()``
        2. This currently accomodates two types of HDF files. Adopt a single specification with Years to reduce complexity

        ..  Questions
            ---------
            1. Move to a Generic Class of DatasetConstructors?

        """
        #-Complete Raw Data File-#
        if years == [] or years == self._available_years and not use_raw_years_fl:                  
            fn = self._source_dir + self.__cache_dir + self.__raw_data_hdf_fn
            if verbose: print "[INFO] Loading RAW DATA from %s" % fn
            self.__raw_data = pd.read_hdf(fn, key='raw_data')
            if gc_collect:
                gc.collect()
        #-Year Indexed File-#
        else:
            self.__raw_data     = pd.DataFrame() 
            fn = self._source_dir + self.__cache_dir + self.__raw_data_hdf_yearindex_fn 
            for year in years:
                if verbose: print "[INFO] Loading RAW DATA for year: %s from %s" % (year, fn)
                self.__raw_data = self.__raw_data.append(pd.read_hdf(fn, key='Y'+str(year)))
                if gc_collect:
                    gc.collect()

    def check_cache(self, check="year", verbose=True):
        """
        Check cache files match dta
        
        Parameters
        ----------
        check       :       str, optional(default="both")
                            Check "raw", "year" or "both" types of cache files

        Notes
        -----
        1. Best to use skip_setup=True when generating initial object to reduce the memory footprint in checking files
        2. This currently FAILS for "raw" becuase when tables get appended together this forces the 'values' to be floats in all years and it is only found in some year specific files

        """
        #-Check RAW File-#
        years = self.source_years
        if check == "raw" or check == "both":   
            print "[INFO] Checking Raw Data Cache File"
            fn = self._source_dir + self.__cache_dir + self.__raw_data_hdf_fn
            if verbose: print "[INFO] Loading RAW DATA from %s" % fn
            raw_data = pd.read_hdf(fn, key='raw_data')
            #-Check Against Year Files-#                                   
            for year in years:
                raw_year = raw_data.ix[raw_data.year==year].reset_index()                       #Reset the Index as object view retains old index line numbers
                del raw_year['index']                                                           #Remove old index reference for comparison to dta_year
                fn = self._source_dir + self._fn_prefix + str(year)[-2:] + self._fn_postfix
                if verbose: print "Loading Year: %s from file: %s" % (year, fn)
                dta_year = pd.read_stata(fn)
                dta_year['value'] = dta_year['value'].astype(float)                             #Note all dta_year items are imported as floats
                try:
                    # assert_merged_series_items_equal(raw_year['value'], dta_year['value'])    #Should I use this merge utility or assert_frames?
                    assert_frame_equal(raw_year, dta_year)
                except:
                    print "Not an EXACT Match ... trying approximately"
                    try:
                        assert_frame_equal(raw_year, dta_year, check_less_precise=True)
                    except:
                        return raw_year, dta_year  #debug
            if check == "raw":
                return True, None
        #-Check RAW Year File-#
        if check == "year" or check == "both":
            #-Check Against Year Files-#                                   
            for year in years:
                print "[INFO] Checking Year: %s" % year
                #-Cache Data-#
                fn = self._source_dir + self.__cache_dir + self.__raw_data_hdf_yearindex_fn 
                if verbose: print "Loading Year: %s from file: %s" % (year, fn)
                raw_year = pd.read_hdf(fn, key='Y'+str(year))
                #-Raw DTA Files-#
                fn = self._source_dir + self._fn_prefix + str(year)[-2:] + self._fn_postfix
                if verbose: print "Loading Year: %s from file: %s" % (year, fn)
                dta_year = pd.read_stata(fn)
                assert_merged_series_items_equal(raw_year['value'], dta_year['value'])              #This is done via an Inner Merge so order isn't as important
            if check == "year":
                return True, None
        return True, True


    def dataset_to_hdf(self, flname='default', key='default', format='table', verbose=True):
        """
        Save a dataset to HDF File
        """
        if flname == 'default':
            flname = self._name+'_%s-%s'%(self.years[0], self.years[-1])+'_SITC-L%s'%(self.level)+'_dataset.h5'
        if key == 'default':
            key = self._name
        if verbose: print "[INFO] Saving dataset to: %s" % flname
        self.dataset.to_hdf(flname, key='dataset', mode='w', complevel=9, complib='zlib', format=format)
        gc.collect()



    # ---------------------- #
    # - Supplementary Data - #
    # ---------------------- #

    def load_china_hongkongdata(self, years=[], return_dataset=False, verbose=True):
        """
        Load China Kong Kong Adjustment Data into Supplementary Data with key ['chn_hk_adjust']
        
        Parameters
        ----------
        years           :   list(int), optional(default=[])
                            Apply a year filter. Default behaviour is ALL years
        return_dataset  :   bool, optional(default=False)
                            Returns a reference to the data in supp_data dictionary

        Notes
        -----
            File Pattern: CHINA_HK??.dta (?? = 88,89,...,00)

        Returns
        -------
        self._supp_data['chn_hk_adjust']    :   pd.DataFrame    

        ..  Future Work:
            -----------
            1.  Currently this method uses the source_dir that is defined when the object is initialised. 
                Could add in the option to specify a different source_dir but this probably won't get used. Not Wasting Time Now
            2.  Modify this so that only the intersection between available years and years. 
        """
        op_string = u"(load_china_hongkongdata(years=%s, return_dataset=%s))" % (years, return_dataset)
        # - Attributes of China Hong-Kong Adjustment - #
        fn_prefix   = u'china_hk'
        fn_postfix  = u'.dta'
        available_years = xrange(1988, 2000 + 1)
        key         = u'chn_hk_adjust'
        #- Parse Year Filter - #
        if years == []:
            years = list(set([int(x) for x in self.years]).intersection(set(available_years)))      #This will load the intersection of self.years and years available for adjustment
        else:
            years = list(set(years).intersection(set(available_years)))     # Make sure years is supported by available data
        # - Import Data - #
        try: 
            self._supp_data[key]
            print "[NOTICE] It appears China Hong Kong Data has Already Been Imported!"
            return self._supp_data[key]
        except:
            data = pd.DataFrame()
            for year in years:
                if verbose: print "[INFO] Using source_dir: %s" % self._source_dir
                fn = self._source_dir + fn_prefix + str(year)[-2:] + fn_postfix
                if verbose: print "[INFO] Loading Year: %s from file: %s" % (year, fn)
                data = data.append(pd.read_stata(fn))
            # - Add Notes to the DataFrame - #
            data.notes =    u'Files: ' + fn_prefix + u'??' + fn_postfix + '\n'  +\
                            u'Years: ' + str(years) + '\n'                      +\
                            u'Source: ' + self._source_dir 
            # - Assign Data to Supp_Data with Key - #
            self._supp_data = {key : data}
        #-Update op_string-#
        update_operations(self, op_string)
        # - Option to Return Dataset - #
        if return_dataset:
            return self._supp_data[key]
            

    def bilateral_flows(self, verbose=False):
        """
        Load NBERFeenstra Bilateral Trade Flows (summed across SITC commodities)

        Notes
        -----
        1. File: WTF_BILAT.dta ::

            Variables
            ---------
            ICode       :   Importer country code
            ECode       :   Exporter country code
            Importers   :   Importer country name
            Exporter    :   Exporter country name
            Value??     :   Thousands of US dollars where ?? = 62, 63,...,00
        
            DataShape: Wide

        2. Can use this Supplementary Data to check Aggregations and how different they are etc.
        3. This corresponds to a CountryLevelExportSystem()
        
        """
        fn = u'WTF_BILAT.dta'
        key = u'bilateral_flows'
        try:
            self._supp_data[key]
            print "[NOTICE] It appears Bilateral Flows Data has Already Been Imported!"
            return self._supp_data[key]
        except:
            if verbose: print "[INFO] Using source_dir: %s" % self._source_dir
            fn = self._source_dir + fn
            data = pd.read_stata(fn)
            # - Add Notes to the DataFrame - #
            data.notes =    u'Source: ' + fn + '\n' + \
                            u'Shape: Wide(bilateral pairs x years)' + '\n'
            # - Assign Data to supp_data - #
            self._supp_data[key] = data
            return self._supp_data[key]


    # ---------------------------------- #
    # - General Operations on Dataset  - #
    # ---------------------------------- #

    def reduce_to(self, to=['year', 'iiso3c', 'eiso3c', 'sitc4', 'value'], rtrn=False, verbose=False):
        """
        Reduce a dataset to a specified list of columns

        Parameters
        ----------
        to      :   list, optional(default=['year', 'iiso3c', 'eiso3c', 'sitc4', 'value'])
                    Supply a list to reduce the dataset to the specified columns
        rtrn    :   bool, optional(default=False)
                    Return reference to the internal dataset

        Note
        ----
        1. This is an ideal candidate for a constructor superclass so that it is inherited
        """
        opstring = u"(reduce_to(to=%s))" % to
        if verbose: print "[INFO] Reducing Dataset from %s to %s" % (list(self.dataset.columns), to)
        self._dataset = self.dataset[to]
        self.operations += opstring
        if rtrn:
            return self.dataset

    def adjust_china_hongkongdata(self, verbose=False):
        """
        Replace/Adjust China and Hong Kong Data to account for China Shipping via Hong Kong
        This will merge in the Hong Kong / China Adjustments provided with the dataset for the years 1988 to 2000.

        Notes
        -----
        1. This method requires no operations to have been done previously on the dataset.  

        """
        op_string = u'(adjust_raw_china_hongkongdata)'
        if check_operations(self, op_string):                           #Check if Operation has been conducted
            return None
        #-Merge Settings-#
        if not check_operations(self, "load_china_hongkongdata"):
            self.load_china_hongkongdata()
        if self.operations != '':
            print "[WARNING] Operations already conducted on data: %s" % self.operations
        # if self.operations != '':
        #     raise ValueError("This method requires no previous operations to have been performed on the dataset!")
        # else:
        #     on          =   [u'year', u'icode', u'importer', u'ecode', u'exporter', u'sitc4', u'unit', u'dot']              #Merge on the Full complement of Items in the Original Dataset
        on = [u'year', u'icode', u'importer', u'ecode', u'exporter', u'sitc4', u'unit', u'dot']
        #-Note: Current merge_columns utility merges one column set at a time-#
        #-Values-#
        raw_value = self.dataset[on+['value']].rename(columns={'value' : 'value_raw'})
        try:
            supp_value = self._supp_data[u'chn_hk_adjust'][on+['value_adj']]
        except:
            raise ValueError("[ERROR] China/Hong Kong Data has not been loaded!")
        value = merge_columns(raw_value, supp_value, on, collapse_columns=('value_raw', 'value_adj', 'value'), dominant='right', output='final', verbose=verbose)

        # Note: Imposing the requirement that this be merged on complete matching information (i.e. no previous operations on the dataset, this section could be removed from try,except)
        #'Quantity' May not be available if collapse_to_valuesonly has been done etc.
        try:                                                             
            #-Quantity-#
            raw_quantity = self._dataset[on+['quantity']]
            supp_quantity = self._supp_data[u'chn_hk_adjust'][on+['quantity']]
            quantity = merge_columns(raw_quantity, supp_quantity, on, collapse_columns=('quantity_x', 'quantity_y', 'quantity'), dominant='right', output='final', verbose=verbose)
            #-Join Values and Quantity-#
            if verbose: 
                print "[INFO] Merging Value Adjusted and Quantity Adjusted Series"
                print "[INFO] Value Adjusted Number of Observations: \t%s (Variables: %s)" % (value.shape[0], value.shape[1])
                print "[INFO] Quantity Adjusted Number of Observations: %s (Variables: %s)" % (quantity.shape[0], quantity.shape[1])
            updated_raw_values = value.merge(quantity, how='outer', on=on)
            if verbose:
                print "[INFO] Merged Number of Observations: \t\t%s (Variables: %s)" % (updated_raw_values.shape[0], updated_raw_values.shape[1])
        except:
            if verbose: print "[INFO] Quantity Information is not available in the current dataset\n"
            updated_raw_values = value  #-Quantity Not Available-#

        report =    u"[INFO] # of Observations in Original Dataset: %s\n" % (len(self._dataset)) +\
                    u"[INFO] # of Observations in Updated Dataset: \t%s\n" % (len(updated_raw_values))
        if verbose: print report

        #-Cleanup of Temporary Objects-#
        del raw_value, supp_value, value
        try:
            del raw_quantity, supp_quantity, quantity
        except:
            pass    #-Quantity Merge Hasn't Taken Place-#

        #- Add Notes -#
        update_operations(self, op_string)

        #-Set Dataset to the Update Values-#
        updated_raw_values["year"] = updated_raw_values["year"].apply(lambda x: int(x))      #merge_columns is causing year to change from int to float?
        self._dataset = updated_raw_values

        

    def standardise_data(self, force=False, verbose=False):
        """
        Run Appropriate Set of Standardisation over the Dataset

        The actions conducted in the method are:
        1. Trade Values in $'s 
        2. Add ISO3C Codes and Well Formatted CountryNames ('exportername', 'importername')
        3. Marker for Standard SITC Revision 2 Codes

        Notes
        -----
        1. Raw Dataset has Non-Standard SITC rev2 Codes so adding a marker to identify 'official' codes

        ..  Future Work
            -----------
            1. Remove this and use subfunctions in __init__ to be more explicit?
            2. Is this needed? Delete?

        """
        op_string = u"(standardise_data)"
        #-Check if Operation has been conducted-#
        if check_operations(self, op_string): return None
        #-Core-#
        self.change_value_units(verbose=verbose)            #Change Units to $'s
        self.add_iso3c(verbose=verbose)
        self.add_isocountrynames(verbose=verbose)
        self.add_sitcr2_official_marker(verbose=verbose)    #Build SITCR2 Marker
        #- Add Operation to df attribute -#
        update_operations(self, op_string)

    def fix_raw_data(self, verbose=False):
        """
        Apply Fixes to NBER Data for ZWE, MWI
        
        ZWE - Drop Data in 1963,1964
        MWI - Drop Data in 1963,1964

        """
        if verbose: print "[INFO] Adjustments to RAW DATA based on NBER FAQ ..."
        data = self._dataset
        for yr in xrange(1963,1964+1,1):
            for country in ["Malawi", "Zimbabwe"]:
                if verbose:
                    print
                    print "[INFO] Removing %s trade values in year %s ..."%(country, yr)
                drop = data.loc[(data.year == yr)&((data.exporter == country)|(data.importer == country))]
                if verbose:
                    print "...... Dropping"
                    print drop.to_string()
                    print "[INFO] Number of Observations = %s"%data.shape[0]
                    print "[INFO] Dropping ... %s observations"%len(drop)
                data = data.drop(drop.index)
                if verbose: print "[INFO] Number of Observations after drop = %s"%data.shape[0]
                gc.collect()
        self._dataset = data    #Set Property

    # ------------------------------- #
    # - Operations on Country Codes - #
    # ------------------------------- #

    from .meta import fix_exporter_to_iso3n, fix_ecode_to_iso3n, fix_exporter_to_iso3c, fix_importer_to_iso3n, fix_icode_to_iso3n, fix_importer_to_iso3c    #-Move to Top of File-#?

    @property 
    def fix_countryname_to_iso3n(self):
        """ 
        Compute a joint dictionary of exporter and importer adjustments
        
        Notes
        -----
        1. A joint dictionary can reduce errors by ensuring both exporters and importers are encoded the same way

        """
        try:
            return self.__fix_countryname_to_iso3n
        except:
            #-Load Data-#
            fix_exporter_to_iso3n = self.fix_exporter_to_iso3n
            fix_importer_to_iso3n = self.fix_importer_to_iso3n
            #-Core-#
            countryname_to_iso3n = dict()
            for key in fix_exporter_to_iso3n.keys():
                countryname_to_iso3n[key] = fix_exporter_to_iso3n[key]
            for key in fix_importer_to_iso3n.keys():
                try:
                    if countryname_to_iso3n[key] == fix_importer_to_iso3n[key]:
                        pass
                    else:
                        raise ValueError("The index: %s conflicts between exporter and importer fix dictionaries\nExporter: %s != Importer: %s") % (key, fix_exporter_to_iso3n[key], fix_importer_to_iso3n[key])
                except:
                    countryname_to_iso3n[key] = fix_importer_to_iso3n[key]
            self.__fix_countryname_to_iso3n = countryname_to_iso3n
            return countryname_to_iso3n

    def split_countrycodes(self, dataset=True, apply_fixes=True, iso3n_only=False, force=False, verbose=True):
        """
        Split CountryCodes into components ('icode', 'ecode')
        Code structure ::   

            XXYYYZ => UN-REGION [2] + ISO3N [3] + Modifier [1]

        Parameters
        ----------
        dataset         :   bool, optional(default=True)
                            Use the dataset attribute (otherwise use raw_data)
        apply_fixes     :   bool, optional(default=True)
                            A djusts iso3n numbers to match updated codes. 
        iso3n_only      :   Removes iregion and imod
        force           :   force this to run regardless of op_string

        Notes
        -----
        1.  Should this be done more efficiently? (i.e. over a single pass of the data) 
            Current timeit result: 975ms per loop for 1 year
        """
        #-Set Data from Dataset OR Raw Data-#
        if dataset:
            data = self.dataset
        else:
            data = self.__raw_data
        #-Check if Operation has been conducted-#
        op_string = u"(split_countrycodes)"
        if check_operations(self, op_string, verbose=verbose): 
            if force:   
                pass
            else:
                return None
        # - Importers - #
        if verbose: print "Spliting icode into (iregion, iiso3n, imod)"
        data['iregion'] = data['icode'].apply(lambda x: int(x[:2]))
        data['iiso3n']  = data['icode'].apply(lambda x: int(x[2:5]))
        data['imod']    = data['icode'].apply(lambda x: int(x[-1]))
        # - Exporters - #
        if verbose: print "Spliting ecode into (eregion, eiso3n, emod)"
        data['eregion'] = data['ecode'].apply(lambda x: int(x[:2]))
        data['eiso3n']  = data['ecode'].apply(lambda x: int(x[2:5]))
        data['emod']    = data['ecode'].apply(lambda x: int(x[-1]))
        #- Add Operation to df attribute -#
        update_operations(self, op_string)
        if not dataset:
            return None
        #-Apply Custom Fixes and ISO3N Option-#
        if iso3n_only:
            del data['iregion']
            del data['imod']
            del data['eregion']
            del data['emod']
        if apply_fixes:
            self.apply_iso3n_custom_fixes(match_on='countrycode', verbose=verbose)
        

    def apply_iso3n_custom_fixes(self, match_on='countrycode', verbose=True):
        """ 
        Apply Custom Fixes for ISO3N Numbers
        
        Parameters
        ----------
        match_on    :   str, optional(default='countrycode')   
                        allows matching on 'countryname' (i.e. exporter, importer) or 'countrycode' (i.e. ecode, icode)

        Notes
        -----
        1. Currently uses attribute fix_countryname_to_iso3n, fix_icode_to_iso3n, fix_ecode_to_iso3n (will move to meta)

        ..  Future Work 
            -----------
            1. Write Tests

        """
        #-Op String-#
        op_string = u"(apply_iso3n_custom_fixes)"
        if check_operations(self, op_string): 
            return None
        if not check_operations(self, u"(split_countrycodes)", verbose=verbose):        #ensure iiso3n, eiso3n are constructed
            if verbose: print "[INFO] Calling split_countrycodes() method"
            self.split_countrycodes(apply_fixes=False, iso3n_only=True, verbose=verbose)
        #-Core-#
        if match_on == 'countryname':
            fix_countryname_to_iso3n = self.fix_countryname_to_iso3n
            for key in sorted(fix_countryname_to_iso3n.keys()):
                if verbose: print "For countryname %s updating iiso3n and eiso3n codes to %s" % (key, fix_countryname_to_iso3n[key])
                df = self._dataset
                df.loc[df.importer == key, 'iiso3n'] = fix_countryname_to_iso3n[key]
                df.loc[df.exporter == key, 'eiso3n'] = fix_countryname_to_iso3n[key]
        elif match_on == 'countrycode':
            fix_ecode_to_iso3n = self.fix_ecode_to_iso3n                                                        #Will be moved to Meta
            for key in sorted(fix_ecode_to_iso3n.keys()):
                if verbose: print "For ecode %s updating eiso3n codes to %s" % (key, fix_ecode_to_iso3n[key])
                df = self._dataset
                df.loc[df.ecode == key, 'eiso3n'] = fix_ecode_to_iso3n[key]
            fix_icode_to_iso3n = self.fix_icode_to_iso3n                                                        #Will be moved to Meta
            for key in sorted(fix_icode_to_iso3n.keys()):
                if verbose: print "For icode %s updating iiso3n codes to %s" % (key, fix_icode_to_iso3n[key])
                df = self._dataset
                df.loc[df.icode == key, 'iiso3n'] = fix_icode_to_iso3n[key]
        else:
            raise ValueError("match_on must be either 'countryname' or 'countrycode'")
        #- Add Operation to class attribute -#
        update_operations(self, op_string)

    def add_iso3c(self, verbose=False):
        """ 
        Add ISO3C codes to dataset

        This method uses the iso3n codes embedded in icode and ecode to add in iso3c codes
        I find this to be the most reliable matching method. 
        However there are other ways by matching on countrynames etc.
        Some of these concordances can be found in './meta'

        Notes 
        -----
        1. See also: build_countrynameconcord_add_iso3ciso3n()
        2. Requires :: 

            1. split_countrycodes()
            2. iso3n_to_iso3c (#Check if Manual Adjustments are Required: nberfeenstrawtf(iso3n)_to_iso3c_adjust)
        
        3.  This matches all UN iso3n codes which aren't just a collection of countries. 
            For example, this concordance includes items such as 'WLD' for World
        
        ..  Questions
            ---------
            1.  Should cleanup of iso3n and iregion imod occur here? Should this collapse (sum)?

        """
        #-OpString-#
        op_string = u"(add_iso3c)"
        if check_operations(self, op_string): return None
        #-Core-#
        if not check_operations(self, u"(split_countrycodes)"):         #Requires iiso3n, eiso3n
            if verbose: print "[INFO] Calling split_countrycodes() method"
            self.split_countrycodes(apply_fixes=True, iso3n_only=True, verbose=verbose)
        un_iso3n_to_iso3c = iso3n_to_iso3c(source_institution='un')
        #-Concord and Add a Column-#
        self._dataset['iiso3c'] = self._dataset['iiso3n'].apply(lambda x: concord_data(un_iso3n_to_iso3c, x, issue_error='.'))
        self._dataset['eiso3c'] = self._dataset['eiso3n'].apply(lambda x: concord_data(un_iso3n_to_iso3c, x, issue_error='.'))
        #- Add Operation to cls attribute -#
        update_operations(self, op_string)

    def add_isocountrynames(self, source_institution='un', verbose=False):
        """
        Add Standard Country Names

        Parameters
        ----------
        source_institution  :   str, optional(default='un')
                                Allows to specify which institution data to use in the match between iso3n and countryname

        Notes
        -----
        1. Requires ::
       
            [1] split_countrycodes()
            [2] iso3n_to_iso3c (#Check if Manual Adjustments are Required: nberfeenstrawtf(iso3n)_to_iso3c_adjust)

        2.  This matches all UN iso3n codes which aren't all countries. 
            These include items such as 'WLD' for World
        """
        #-OpString-#
        op_string = u"(add_isocountrynames)"
        if check_operations(self, op_string): return None
        #-Checks-#
        if not check_operations(self, u"(split_countrycodes"):      #Requires iiso3n, eiso3n
            self.split_countrycodes(apply_fixes=True, iso3n_only=True, verbose=verbose)
        #-Core-#
        un_iso3n_to_un_name = iso3n_to_name(source_institution=source_institution) 
        #-Concord and Add a Column-#
        self._dataset['icountryname'] = self._dataset['iiso3n'].apply(lambda x: concord_data(un_iso3n_to_un_name, x, issue_error='.'))
        self._dataset['ecountryname'] = self._dataset['eiso3n'].apply(lambda x: concord_data(un_iso3n_to_un_name, x, issue_error='.'))

        #-OpString-#
        update_operations(self, op_string)

    def countries_only(self, error_code='.', rtrn=False, verbose=True):
        """
        Filter Dataset for Countries Only AND returns a reference to dataset attribute

        Notes
        -----
        1. Requires ::

            add_iso3c()

        2. This uses the iso3c codes to filter on countries only
        3. This leaves in old countries that may no longer currently exist!

        Future Work
        -----------
        1. Rewrite these to use .loc method?
        2. Write Tests to check the sum of a countries exports and compare to Corresponding World Export Line
        3. Build a Report for Dropped countrycodes

        """
        #-OpString-#
        op_string = u"(countries_only)"
        if check_operations(self, op_string): return self.dataset           #Already been computed
        #-Checks-#
        if not check_operations(self, u"(add_iso3c)"):          
            if verbose: print "[INFO] Calling add_iso3c method"
            self.add_iso3c(verbose=verbose)
        #-Drop NES and Unmatched Countries-#
        self._dataset = self.dataset[self.dataset.iiso3c != error_code]     #Keep iiso3n Countries
        self._dataset = self.dataset[self.dataset.eiso3c != error_code]     #Keep eiso3n Countries
        #-Drop WLD-#
        self._dataset = self.dataset[self.dataset.iiso3c != 'WLD']
        self._dataset = self.dataset[self.dataset.eiso3c != 'WLD']
        #-Drop '.'-#
        self._dataset = self.dataset[self.dataset.iiso3c != '.']
        self._dataset = self.dataset[self.dataset.eiso3c != '.']
        #-ResetIndex-#
        self._dataset = self._dataset.reset_index()                         
        #-OpString-#
        update_operations(self, op_string)
        if rtrn:                                                            #If return dataset, return with old observation numbers
            return self.dataset
        del self._dataset['index']                                          #This Removes old index number.
        

    def drop_world_observations(self, verbose=True):
        """
        Drop Observations that contain 'World' in either exporter or importer
        """
        #-OpString-#
        op_string = u"(drop_world_observations)"
        if check_operations(self, op_string): 
            return None             #Already been computed
        #-Core-#
        if verbose: print "[INFO] Dropping Observations that include `World` in importer or exporter attribute"
        df = self.dataset
        df = df.loc[(df.importer != "World") & (df.exporter != "World")]
        self._dataset = df


    def world_only(self, error_code='.', rtrn=False, verbose=True):
        """
        Filter Dataset for World Exports Only and returns a reference to the dataset attribute

        Parameters
        ----------
        error_code  :   str, optional(default='.')
                        Specify an error code
        rtrn        :   bool, optional(default=False)
                        Return the dataset 

        Notes
        -----
        1. Requires ::
            
            add_iso3c()

        ..  Future Work
            -----------
            1. Build a Report
            2. Add inplace option to return a dataframe rather than right to dataset?

        """
        #-OpString-#
        op_string = u"(world_only)"
        if check_operations(self, op_string): return self.dataset   #Already been computed
        #-Checks-#
        if not check_operations(self, u"(add_iso3c)"):          #Requires iiso3n, eiso3n
            self.add_iso3c(verbose=verbose)
        #-Core-#
        self._dataset = self._dataset.loc[(self._dataset.iiso3c == 'WLD') | (self._dataset.eiso3c == 'WLD')]            #Take WLD either in iiso3c or eiso3c (| = or)
        #-OpString-#    
        update_operations(self, op_string)
        if rtrn:
            return self.dataset


    def adjust_countrycodes_intertemporal(self, force=False, dropvars=['icode', 'importer', 'ecode', 'exporter', 'iiso3n', 'eiso3n'], verbose=True):
        """
        Adjust Country Codes to be Inter-temporally Consistent

        Parameters
        ----------
        force   :       bool, optional(default=False)
                        Force operations over an incomplete dataset
        dropvars    :   list, optional(default=['icode', 'importer', 'ecode', 'exporter', 'iiso3n', 'eiso3n'])
                        Specify which variables to drop

        ..  Future Work
            -----------
            1. Write Tests
            2. Make the Reporting More Informative

        """
        from pyeconlab.trade.dataset.NBERWTF.meta import iso3c_recodes_for_1962_2000
        from pyeconlab.util import concord_data

        #-Parse Complete Dataset Check-#
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("This is not a complete Dataset! ... use force=True if you want to proceed.")
        #-OpString-#
        op_string = u"(adjust_countrycodes_intertemporal)"
        if check_operations(self, op_string): 
            return None
        #-Checks-#
        if not check_operations(self, u"(countries_only)"):         #Adds iso3c 
            if verbose: print "[INFO] Calling countries_only method to remove NES and World aggregations from the dataset"
            self.countries_only(verbose=verbose)
        #-Adjust Codes-#
        if verbose: print "[INFO] Adjusting Codes for Intertemporal Consistency from meta subpackage (iso3c_recodes_for_1962_2000)"
        self._dataset['iiso3c'] = self.dataset['iiso3c'].apply(lambda x: concord_data(iso3c_recodes_for_1962_2000, x, issue_error=False))   #issue_error = false returns x if no match
        self._dataset['eiso3c'] = self.dataset['eiso3c'].apply(lambda x: concord_data(iso3c_recodes_for_1962_2000, x, issue_error=False))   #issue_error = false returns x if no match
        #-Drop Removals-#
        if verbose: print "[INFO] Deleting Recodes to '.'"
        self._dataset = self.dataset[self.dataset['iiso3c'] != '.']
        self._dataset = self.dataset[self.dataset['eiso3c'] != '.']
        #-Collapse Constructed Duplicates-#
        subidx = set(self.dataset.columns)
        subidx.remove('value')
        for item in ['quantity', 'unit', 'dot'] + dropvars:
            try:
                subidx.remove(item)
            except:
                pass
        if verbose: print "[INFO] Collapsing Dataset to SUM duplicate entries on %s" % list(subidx)
        self._dataset = self.dataset[list(subidx) + ['value']].groupby(list(subidx)).sum()
            #self._dataset = self.dataset.groupby(list(['year', 'iiso3c', 'eiso3c', 'sitc%s' % self.level])).sum()
            #self._dataset = self.dataset.sort(columns=['year', 'iiso3c', 'eiso3c', 'sitc%s' % self.level])
        self._dataset = self.dataset.reset_index()                                                  #Return Flat File                                                           
        #-OpString-#    
        update_operations(self, op_string)


    def countryname_concordance_using_cc(self, concord_vars=('countryname', 'iso3c'), target_dir=None, force=False, verbose=False):
        """
        Compute a Country Name Concordance using package: pycountrycode

        It is better to use ISO3N Codes that are built into icode and ecode. Therefore STOP work on this approach.
        Current Concordances can be found in "./meta"

        Warnings
        --------
        1. pycountrycode is no longer supported!

        Returns:
        --------
        pd.DataFrame(countryname,iso3c,iso3n) and/or writes a file

        Notes
        -----
        1. Dependencies ::

            PyCountryCode [https://github.com/vincentarelbundock/pycountrycode]
            Note: pycountrycode has an issue with converting iso3n to iso3c so currently use country names
            vincentarelbundock/pycountrycode Issue #24

        ..  Future Work:
            ------------
            1. Build my own version of pycountrycode so that this work is internalised to this package and doesn't depend on PyCountryCode
                I would like Time Varying Definitions of Country Codes, In addition to Time Varying Aggregates like LDC, MDC, etc.
            2. Add Option to Write Concordance to a py file or csv etc
            3. Write Some Error Checking tests
            4. Turn this Routine into a utility function and remove code duplication

        """
        #-Parse Complete Dataset Check-#
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("This is not a complete Dataset!\n If you want to build a concordance for a given year use force=True")
        iso3c = cc.countrycode(codes=self.country_list, origin='country_name', target='iso3c')
        #-Reject Non-String Responses-#
        for idx,code in enumerate(iso3c):
            if type(code) != str:
                iso3c[idx] = '.'                                                                    #encode as '.' missing
        #-Check Same Length-#                                                                       #This is probably redundant as zip will complain?
        if len(iso3c) != len(self.country_list):
            raise ValueError("Results != Length of Original Country List")
        concord = pd.DataFrame(zip(self.country_list, iso3c), columns=['countryname', 'iso3c'])
        concord['iso3c'] = concord['iso3c'].apply(lambda x: '.' if len(x)!=3 else x)
        concord['iso3n'] = cc.countrycode(codes=concord.iso3c, origin='iso3c', target='iso3n')
        concord.name = 'Concordance for %s : %s' % (self._name, concord.columns)
        self.country_concordance = concord
        #-Parse File Option-#
        if type(target_dir) == str:
            target_dir = check_directory(target_dir)
            fl = "%s_to_%s.py" % (concord_vars[0], concord_vars[1])                     #Convention from_to_to
            if verbose: print "[INFO] Writing concordance to: %s" % (target_dir + fl)
            concord_series = concord[list(concord_vars)].set_index(concord_vars[0])[concord_vars[1]]    #Get Indexed Series Index : Value#
            concord_series.name = u"%s to %s" % (concord_vars[0], concord_vars[1])
            docstring   =   u"Concordance for %s to %s\n" % (concord_vars)          + \
                            u"%s" % self
            from_idxseries_to_pydict(concord_series, target_dir=target_dir, fl=fl, docstring=docstring, verbose=False)
        return self.country_concordance


    # ------------------------------- # 
    # - Operations on Product Codes - #
    # ------------------------------- # 

    def collapse_to_valuesonly(self, subidx=None, return_duplicates=False, verbose=False):
        """
        Adjust Dataset For Export Values that are defined multiple times due to Quantity Unit Codes ('unit')
       
        Parameters
        ----------
        subidx  :   str, optional(default=None)
                    Specify sub index, otherwise method will construct appropriate subindex 
        return_duplicates   :   bool, optional(default=False)
                                Return dubplicates for debugging

        Notes:
        ------
        1. This will remove 'quantity', 'unit' ('dot'?)

        ..  Questions
            ---------
            1. Does this need to be performed before adjust_china_hongkongdata (as this might match multiple times!)?
            2. Write Tests

        """
        #-Find Appropriate idx-#
        if type(subidx) != list:
            subidx = set(self.dataset.columns)
            for item in ['quantity', 'unit', 'dot']:    #Cannot Aggregate These Items
                try:
                    subidx.remove(item)
                except:
                    pass
            subidx.remove('value')                      #Remove to Aggregate in groupby
            subidx = list(subidx)                       
        #-Check if Operation has been conducted-#
        op_string = u"(collapse_to_valuesonly[%s])" % subidx
        if check_operations(self, op_string): return None
        # - Conduct Duplicate Analysis - #
        dup = self._dataset.duplicated(subset=subidx)  
        if verbose:
            print "[INFO] Current Dataset Length: %s" % self._dataset.shape[0]
            print "[INFO] Current Number of Duplicate Entry's: %s" % len(dup[dup==True])
            print "[INFO] Deleted 'quantity', 'unit' as cannot aggregate quantity data in different units and 'dot' due to the existence of np.nan"
        if return_duplicates:           #Return Duplicate Rows
            dup = self.dataset[dup]
        #-Collapse/Sum Duplicates-#
        self._dataset = self.dataset[subidx+['value']].groupby(by=subidx).sum()
        self._dataset = self.dataset.reset_index()                                  #Remove IDX For Later Data Operations
        if verbose:
            print "[INFO] New Dataset Length: %s" % self._dataset.shape[0]
        #- Add Operation to df attribute -#
        update_operations(self, op_string)
        if return_duplicates:
            return dup

    def change_value_units(self, verbose=False):
        """ 
        Updates Values to $'s instead of 1000's of $'s' in Dataset
        """
        #-OpString-#
        op_string = u"(change_value_units)"
        if check_operations(self, op_string): return None
        #-Core-#
        if verbose: print "[INFO] Setting Values to be in $'s not %s$'s" % (self._units_value)
        self._dataset['value'] = self.dataset['value'] * self._units_value
        #-OpString-#
        update_operations(self, op_string)

    def add_sitcr2_official_marker(self, level=4, source_institution='un', verbose=False):
        """ 
        Add an Official SITCR2 Marker to Dataset

        Parameters
        ----------
        level               :   int, optional(default=4)
                                Specify SITC Revision 2 Level
        source_institution  :   str, optional(default='un')
                                allows to specify where SITC() retrieves data [Default: 'un']

        """
        #-OpString-#
        op_string = u"(add_sitcr2_official_marker)"
        if check_operations(self, op_string): return None
        #-Core-#
        if verbose: print "[INFO] Adding SITC Revision 2 (Source='un') marker variable 'SITCR2'"
        sitc = SITC(revision=2, source_institution=source_institution)
        codes = sitc.get_codes(level=level)
        sitcl = 'sitc%s' % level
        self._dataset['SITCR2'] = self.dataset[sitcl].apply(lambda x: 1 if x in codes else 0)
        #-OpString-#
        update_operations(self, op_string)

    def add_productcode_levels(self, verbose=False):
        """
        Add SITC L1, L2, and L3 Codes derived from 'sitc4'
        """
        for level in [1,2,3]:
            self.add_productcode_level(level, verbose)

    def add_productcode_level(self, level, verbose=False):
        """
        Add a Product Code for a specified level between 1 and 3 for 'sitc4'

        Parameters
        ----------
        level   :   int
                    Specify Productcode Level

        Notes
        -----
        1. This could be simplified by using sitcl = 'sitc%s' % level string 

        """
        if level == 1:
            op_string = u"(add_productcode_level1)"
            if check_operations(self, op_string): return None
        elif level == 2:
            op_string = u"(add_productcode_level2)"
            if check_operations(self, op_string): return None
        elif level == 3:
            op_string = u"(add_productcode_level3)"
            if check_operations(self, op_string): return None
        else:
            raise ValueError("SITC4 Can Only Be Split into Levels 1,2, or 3")
        #-Core-#
        if verbose: print "[INFO] Adding Product Code Level: SITC L%s" % level
        if level == 1:
            self._dataset['SITCL1'] = self._dataset['sitc4'].apply(lambda x: x[0:1])
        if level == 2:
            self._dataset['SITCL2'] = self._dataset['sitc4'].apply(lambda x: x[0:2])
        if level == 3:
            self._dataset['SITCL3'] = self._dataset['sitc4'].apply(lambda x: x[0:3])
        #-OpString-#
        update_operations(self, op_string)

    def collapse_to_productcode_level(self, level=3, subidx='default', verbose=False):
        """
        Collapse the Dataset to a Higher Level of Aggregation 

        Parameters
        ----------
        level   :   int, optional(default=3)
                    Specify SITC Level (1 to 3)
        subidx  :   str, optional(default='default')
                    Specify a Column Filter
                    [Default: Builds a SubIDX from self.dataset.columns]
                    Alternatives:   ['year', 'icode', 'importer', 'ecode', 'exporter', 'sitc4', 'dot', 'value']
                                    ['year', 'iiso3c', 'eiso3c', 'sitc4', 'value']

        Notes
        -----
        1. This is a good candidate to be in a superclass
        2. Why restrict to the default subidx?

        ..  Future Work
            -----------
            1. Infer the level and then aggregate based on that inference rather than relying on sitc4 data as the baseline. 
        """
        if verbose: print "[INFO] Cannot Aggregate Quantity due to units. Discarding 'quantity'"
        op_string = u"(collapse_to_productcode_level%s)" % level
        if check_operations(self, op_string): 
            return None
        if level not in [1,2,3]:
            raise ValueError("Level must be 1,2, or 3 for SITC4 Data")
        if subidx == 'default':
            cols = set(self.dataset.columns)
            for item in ['quantity', 'unit', 'dot']:
                try:
                    cols.remove(item)
                except:
                    pass                                                                #Item not in dataset, report?
            #-Ensure 'value' is last-#
            cols.remove('value')
            subidx = list(cols) + ['value']                                         #Is this really necessary? just remove value?
        if verbose: print "[INFO] Collapsing Data to SITC Level #%s" % level
        colcode = 'sitc%s' % level
        self._dataset[colcode] = self.dataset['sitc4'].apply(lambda x: x[0:level])
        #-Aggregate-#
        for idx,item in enumerate(subidx):
            if item == 'sitc4': subidx[idx] = colcode                               # Remove sitc4 and add in the lower level of aggregation sitc3 etc.
        if verbose: print "[INFO] Aggregating on: %s" % subidx[:-1]
        self._dataset = self.dataset[subidx].groupby(by=subidx[:-1]).sum()      # Exclude 'value', as want to sum over it. It needs to be last in the list!
        self._dataset = self.dataset.reset_index()
        self.level = level
        #-OpString-#
        update_operations(self, op_string)

    def delete_sitc4_issues_with_raw_data(self, verbose=False):
        """
        This method deletes any known issues with the raw_data associated with productcodes

        Notes
        -----
        1. Requires ::

            self.level == 4

        2. SITC4 Deletions is Documented in meta subpackage

        """
        from .meta import sitc4_deletions
        self.sitc4_deletions = sitc4_deletions                                  #Add as an Attribute
        #-Parse Requirement-#
        if self.level != 4:
            raise ValueError("The Dataset must be using SITC level 4 Data")
        #-Core-#
        sobs = self.dataset.shape[0]
        if verbose: print "[INFO] Starting Dataset has %s observations" % sobs
        for item in self.sitc4_deletions + ['','.']:                                                            #Delete Error Items '.' and empty items ''
            sgobs = self.dataset[self.dataset['sitc4'] == item].shape[0]
            if verbose: print "[INFO] Deleting sitc4 productcode: %s [Dropping %s observations]" % (item, sgobs)
            self._dataset = self.dataset[self.dataset['sitc4'] != item]
        if verbose: print "[INFO] Dataset now has %s observations [%s Deleted]" % (self.dataset.shape[0], sobs - self.dataset.shape[0])

    def identify_alpha_productcodes(self, verbose=False):
        """
        Identify Productcodes that contain an alpha code 'A', 'X'

        'X' :   Adjustments for aggregation mismatch between Levels 
                4441 + 4442 = $150
                444         = $200
                444X        = $50

        'A' :   No disaggregated details reported, but there are values at higher levels of aggregation 
                444     = $200 but no 4441 or 4442 defined then 
                444A    = $200 

        Notes
        -----
        1.  The A and X codes are not used for the adjusted SITC codes of the 35 countries for
            which specific corrections and adjustments were made, as described in Section 5.
        2.  To obtain comparable inter-temporal codes requires adjustments to the data. A concordance is required
            to bring the specially constructed product codes to data in future years.

        """
        op_string = u"(add_productcode_alpha_indicator)"
        if check_operations(self, op_string): return None
        #-Core-#
        if verbose: print "[INFO] Identifying SITC Codes with A and X"
        self._dataset['SITCA'] = self._dataset['sitc%s' % self.level].apply(lambda x: 1 if re.search("[aA]",x) else 0)
        self._dataset['SITCX'] = self._dataset['sitc%s' % self.level].apply(lambda x: 1 if re.search("[xX]",x) else 0)
        #-OpString-#
        update_operations(self, op_string)

    def drop_alpha_productcodes(self, cleanup=True, verbose=False):
        """
        Drop Productcodes that contain an alpha code 'A', 'X'

        Parameters
        ----------
        cleanup     :   bool, optional(default=True)
                        Clean up SITCA and SITCX identifiers that were added to the dataset

        Notes
        -----
        1.  The A and X codes are not used for the adjusted SITC codes of the 35 countries for
            which specific corrections and adjustments were made, as described in Section 5.
        2.  To obtain comparable inter-temporal codes requires adjustments to the data. A concordance is required
            to bring the specially constructed product codes to data in future years.
        """
        op_string = u"(drop_alpha_productcodes)"
        if check_operations(self, op_string): return None
        pre_value = self.dataset["value"].sum()
        #-Core-#
        if not check_operations(self, u"(identify_alpha_productcodes)"):
            self.identify_alpha_productcodes(verbose=verbose)
        if verbose: print "[INFO] Dropping SITC Codes with A and X"
        obs = self.dataset.shape[0]
        df = self.dataset
        df = df.loc[(df.SITCA != 1) & (df.SITCX != 1)]
        self._dataset = df
        post_value = self.dataset["value"].sum()
        if verbose: print "[INFO] Dropped %s Observations (%s percent of Value)" % (obs - self.dataset.shape[0], ((post_value - pre_value)/pre_value) *100)
        #-OpString-#
        update_operations(self, op_string)
        if cleanup:
            if verbose: print "[INFO] Cleaning up SITC A and X Identifiers"
            del self._dataset['SITCA']
            del self._dataset['SITCX']

    def compute_valAX_sitclevel(self, level=3):
        """ 
        Compute the Value of Codes that Contain AX and the percentage of that Groups Value based on an aggregated level

        Parameters
        ----------
        level   :   int, optional(default=3)
                    Specify a higher level of aggregation [0,1,2,3] 

        Notes
        -----
        1. Required Columns ::

            'SITC#' #=[1,2,3]; 'year', 'value'

        2.  At the SITCL3 the maximum value contained in AX codes is 4% (mean: 0.2%)
            There isn't a huge loss of data by deleting them. These values should be included in the SITCL3 Dataset

        ..  Questions
            ---------
            1.  Should I unstack 'year'?

        """
        #-Required Items-#
        self.add_iso3c()                                                            # ISO3C
        self.add_productcode_levels()                                               # SITCL3
        self.identify_alpha_productcodes()                                          # SITCA, SITCX
        #-Data-#
        sitcl = 'SITCL%s' % level
        data = self.dataset.copy(deep=True)
        data['SITC_AX'] = data['SITCA'] + data['SITCX']
        data['SITC_AX'] = data['SITC_AX'].apply(lambda x: 1 if x >= 1 else 0)
        if sitcl == 'SITCL0':
            subidx = ['year', 'SITC_AX', 'value']
            data = data[subidx].groupby(['year', 'SITC_AX']).sum()
        else:
            subidx = ['year', sitcl, 'SITC_AX', 'value']
            data = data[subidx].groupby(['year', sitcl, 'SITC_AX']).sum()
        data = data.unstack(level='SITC_AX')
        data.columns = data.columns.droplevel()
        data.columns = pd.Index(['NOAX', 'AX'])
        data['%Tot'] = data['AX'].div(data['NOAX'] + data['AX']) * 100
        return data

    def compute_val_unofficialcodes_sitclevel(self, level=3, includeAX=False):
        """ 
        Compute the Value of Codes that are Unofficial and the percentage of that Groups Value based on an aggregated level

        Parameters
        ----------
        level   :   int, optional(default=3)
                    specify a higher level of aggregation [0,1,2,3]
        includeAX   :   bool, optional(default=False)
                        Include AX Values

        ..  Future Work
            -----------
            1. Implement the case level = 0
                # if sitcl == 'SITCL0':
                #   subidx = ['year', 'AX', 'value']
                #   data = data[subidx].groupby(['year', 'MARKER']).sum()
                # else:
        """
        #-RequiredItems-#
        #self.add_iso3c()
        self.add_productcode_levels()                                               # SITCL3
        self.add_sitcr2_official_marker(source_institution='un')                    # SITCR2
        if includeAX:
            self.identify_alpha_productcodes()                                      # SITCA, SITCX
        #-Data-#
        sitcl = 'SITCL%s' % level
        data = self.dataset.copy(deep=True)
        if includeAX:
            data['AX'] = data['SITCA'] + data['SITCX']                              #A or X
            data['AX'] = data['AX'].apply(lambda x: 1 if x >= 1 else 0)
            subidx = ['year', sitcl, 'AX', 'SITCR2', 'value']
            data = data[subidx].groupby(['year', sitcl, 'AX', 'SITCR2']).sum()
            data = data.unstack(level=['AX', 'SITCR2'])
            data.columns = data.columns.droplevel()
            midx = pd.MultiIndex.from_tuples([(0,1)], names=['AX', 'SITCR2'])                   
            s1  = data[[(0,1)]].div(pd.DataFrame(data.sum(axis=1), columns=midx)) * 100
            s1.columns = ['%Official']
            midx = pd.MultiIndex.from_tuples([(1,0)], names=['AX', 'SITCR2'])
            s2  = data[[(1,0)]].div(pd.DataFrame(data.sum(axis=1), columns=midx)) * 100
            s2.columns = ['%AX']                                    
            s3  = pd.DataFrame(data[[(0,0), (1,0)]].sum(axis=1).div(data.sum(axis=1)) * 100, columns=['%NotOffandAX'])
            for item in [s1,s2,s3]:
                data = data.merge(item, left_index=True, right_index=True)
            data.columns = ['NotAX-NotSITCR2', 'NotAX-SITCR2', 'AX-NotSITCR2'] + list(data.columns[3:])
            data = data.stack().unstack(level='year')
            return data
        else:
            subidx = ['year', sitcl, 'SITCR2', 'value']
            data = data[subidx].groupby(['year', sitcl, 'SITCR2']).sum()
            data = data.unstack(level='SITCR2')
            data.columns = data.columns.droplevel()
            data.columns = pd.Index(['SITCR2', 'NOT-SITCR2'])
            data['%Tot'] = data['NOT-SITCR2'].div(data['SITCR2'] + data['NOT-SITCR2']) * 100
            data = data.stack.unstack(level='year')
        return data


    # ------------------------------------------- #
    # - Construct Predefined Datasets Wrappers  - #
    # ------------------------------------------- #
    
    def construct_sitc_dataset(self, data_type, dataset, product_level, sitc_revision=2, report=True, dataset_object=False, special_years="", verbose=True):
        """
        Constructor of Predefined SITC Datasets

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
        special_years   :   str, optional(default="")
                            Specify Special Year Case for Intertemporal Productcodes Option

        Future Work
        -----------
        1. Is there a better way to add in Hong Kong Data?
        2. If dataset == dict then use it as the dataset parameters for compilation

        """
        #-Dataset Definitions-#
        from .constructor_dataset import SITC_DATASET_DESCRIPTION, SITC_DATASET_OPTIONS
        #-Checks-#
        if self.operations != "":
            raise ValueError("This Method requires a complete RAW dataset")
        if sum(self.years) != 77259:                                                                        #IS there a better way than this hack!
            raise ValueError("This Dataset must contain the full range of years to be constructed")
        #-Parse Input-#
        if sitc_revision == 2:
            from .constructor_dataset_sitcr2 import construct_sitcr2 as construct_dataset
        else:
            raise ValueError("SITC Revision 2 is currently the only implimented revision")
        if dataset not in SITC_DATASET_OPTIONS.keys():
            raise ValueError("Specified Dataset (%s) is not found in the SITC_DATASET_OPTIONS property")
        #-OpString-#
        str_kwargs = [", %s=%s" % (key, SITC_DATASET_OPTIONS[dataset][key]) for key in sorted(SITC_DATASET_OPTIONS[dataset].keys())]
        op_string = u"(construct_sitc_dataset(data_type=%s, dataset=%s, product_level=%s, sitc_revision=%s, report=%s, dataset_object=%s, verbose=%s%s))" % (data_type, dataset, product_level, sitc_revision, report, dataset_object, verbose, "".join(str_kwargs))
        self.notes = op_string #-Save Settings-#
        if check_operations(self, op_string): 
            return None
        #-Main Work-#
        DESCRIPTION = SITC_DATASET_DESCRIPTION[dataset]
        OPTIONS = SITC_DATASET_OPTIONS[dataset]
        #-Check and add Supplementary Data-#
        if OPTIONS['adjust_hk'] == True:
            if not check_operations(self, "load_china_hongkongdata"):
                self.load_china_hongkongdata(verbose=verbose) #-Load Data with Default Attributes-#
            OPTIONS['adjust_hk'] = (True, self.supp_data(item='chn_hk_adjust'))
        else:
            OPTIONS['adjust_hk'] = (False, None)
        #-Intertemporal Product Codes-#
        if OPTIONS['intertemp_productcode']:
            from .meta import IntertemporalProducts
            if special_years == "":
                ICP = IntertemporalProducts().IC6200[product_level]
            elif special_years == "7400":
                ICP = IntertemporalProducts().IC7400[product_level]
            elif special_years == "8400":
                ICP = IntertemporalProducts().IC8400[product_level]
            OPTIONS['intertemp_productcode'] = (True, ICP)
        else:
            OPTIONS['intertemp_productcode'] = (False, None)
        #-Compute Dataset-#
        self._dataset = construct_dataset(self.dataset, data_type=data_type, level=product_level, verbose=verbose, **OPTIONS)
        self.dataset_name = "SITCR2-%s" % dataset
        #-Restore Original Option-#
        if type(OPTIONS['adjust_hk']) == tuple:
            OPTIONS['adjust_hk'] = OPTIONS['adjust_hk'][0]
        if type(OPTIONS['intertemp_productcode']) == tuple:
            OPTIONS['intertemp_productcode'] = OPTIONS['intertemp_productcode'][0]
        #-Construct Report-#
        if report:
            rdf = self.raw_data                                                     #Note: This produces a copy!
            rdf = rdf.loc[(rdf.importer=="World") & (rdf.exporter == "World")]
            #-Year Values-#
            rdfy = rdf.groupby(['year']).sum()['value'].reset_index()
            dfy = self._dataset.groupby(['year']).sum()['value'].reset_index()
            y = rdfy.merge(dfy, how="outer", on=['year']).set_index(['year'])
            y['%'] = y['value_y'] / y['value_x'] * 100
            report =    "Report %s\n"%(op_string) + \
                        "---------------------------------------\n"
            for year in self.years:
                report += "This dataset in year: %s captures %s percent of Total 'World' Values\n" % (year, int(y.ix[year]['%']))
            print report
        #-Update Attributes-#
        self.revision = sitc_revision
        self.level = product_level
        #-OpString-#
        update_operations(self, op_string)
        #-Return a Dataset Object-#
        if dataset_object: 
            obj = self.to_nberwtf(data_type=data_type)
            return obj

    # -------------------------------------------------------------------------------------- #
    # -- NOTICE: With the Addition of construct_sitc_dataset() this section is deprecated -- #
    # -------------------------------------------------------------------------------------- #

    # ------------------------------------ START REMOVAL ----------------------------------- #

    def construct_dataset_SC_CNTRY_SR2L3_Y62to00(self, data_type, dropAX=True, sitcr2=True, drop_nonsitcr2=True, intertemp_cntrycode=False, drop_incp_cntrycode=False, adjust_units=False, report=True, source_institution='un', verbose=True):
        """
        Construct a Self Contained (SC) Direct Action Dataset for Countries at the SITC Level 3
        **Note**: Self Contained Compilation Reduces the Need to Debug many other routines for the time being. 
        The other methods are however useful to diagnose issues and to understand properties of the dataset

        STATUS: tests/test_constructor_dataset_sitcr2l3.py

        See 

        Parameters
        ----------
        data_type           :   str
                                Specify what type of data 'trade', 'export', 'import'
        dropAX              :   bool, optional(default=True)
                                Drop AX Codes 
        sitcr2              :   bool, optional(default=True)
                                Add SITCR2 Indicator
        drop_nonsitcr2      :   bool, optional(default=True)
                                Drop non-standard SITC2 Codes
        intertemp_cntrycode :   bool, optional(default=False)
                                Generate Intertemporal Consistent Country Units (from meta)
        drop_incp_cntrycode :   bool, optional(default=False)
                                Drop Incomplete Country Codes (from meta)
        adjust_units        :   bool, optional(default=False)
                                Adjust units by a factor of 1000 to specify in $'s
        report              :   bool, optional(default=True)
                                Print Report
        source_institution  :   str, optional(default='un')
                                which institutions SITC classification to use

        Notes
        -----
        1. Operations ::

            [1] Drop SITC4 to SITC3 Level (for greater intertemporal consistency)
            [2] Import ISO3C Codes as Country Codes
            [3] Drop Errors in SITC3 codes ["" Codes]
            Optional:
            ---------
            [A] Drop sitc3 codes that contain 'A' and 'X' codes [Default: True]
            [B] Drop Non-Standard SITC3 Codes [i.e. Aren't in the Classification]
            [C] Adjust iiso3c, eiso3c country codes to be intertemporally consistent
            [D] Drop countries with incomplete data across 1962 to 2000 (strict measure)

        2. Datasets ::

            [A] dropAX=False, sitcr2=False, drop_nonsitcr2=False, adjust_hk=False, intertemp_cntrycode=False, drop_incp_cntrycode=False
            [B] dropAX=False, sitcr2=False, drop_nonsitcr2=False, adjust_hk=True, intertemp_cntrycode=False, drop_incp_cntrycode=False
            [C] dropAX=True, sitcr2=True, drop_nonsitcr2=True, adjust_hk=True, intertemp_cntrycode=False, drop_incp_cntrycode=False
            [D] dropAX=True, sitcr2=True, drop_nonsitcr2=True, adjust_hk=True, intertemp_cntrycode=True, drop_incp_cntrycode=False 
            [E] dropAX=True, sitcr2=True, drop_nonsitcr2=True, adjust_hk=True, intertemp_cntrycode=True, drop_incp_cntrycode=True  

        3. This makes use of countryname_to_iso3c in the meta data subpackage
        4. This method can be tested using /do/basic_sic3_country_data.do
        5. DropAX + Drop NonStandard SITC Rev 2 Codes still contains ~94-96% of the data found in the raw data

        ..  Future Work
            -----------
            1. Check SITC Revision 2 Official Codes
            2. Should this be split into a header function with specific trade, export, and import methods?
            3. What should I do about the duplicate information contained in this docstring and the actual dataset constructor function (which is externally available)
        """
        warnings.warn("Deprecated in preference for construct_sitc_dataset method. This method doesn't include HK-CHINA Adjustments", PendingDeprecationWarning)
        from .meta import countryname_to_iso3c
        self.dataset_name = 'CNTRY_SR2L3_Y62to00_A'
        #-Checks-#
        if self.operations != "":
            raise ValueError("This Method requires a complete RAW dataset")
        if sum(self.years) != 77259:                                                                        #IS there a better way than this hack!
            raise ValueError("This Dataset must contain the full range of years to be constructed")
        #-Construct Dataset-#
        from .constructor_dataset_sitcr2l3 import construct_sitcr2l3
        df = construct_sitcr2l3(self.dataset, data_type=data_type, dropAX=dropAX, sitcr2=sitcr2, drop_nonsitcr2=drop_nonsitcr2, adjust_hk=False, 
                                intertemp_cntrycode=intertemp_cntrycode, drop_incp_cntrycode=drop_incp_cntrycode,           
                                adjust_units=adjust_units, source_institution='un', verbose=True)
        #-Report-#
        if report:
            rdf = self.raw_data
            rdf = rdf.loc[(rdf.importer=="World") & (rdf.exporter == "World")]
            #-Year Values-#
            rdfy = rdf.groupby(['year']).sum()['value'].reset_index()
            dfy = df.groupby(['year']).sum()['value'].reset_index()
            y = rdfy.merge(dfy, how="outer", on=['year']).set_index(['year'])
            y['%'] = y['value_y'] / y['value_x'] * 100
            report =    "Report construct_dataset_SC_CNTRY_SR2L3_Y62to00(options)\n" + \
                        "---------------------------------------\n"
            for year in self.years:
                report += "This dataset in year: %s captures %s percent of Total 'World' Values\n" % (year, int(y.ix[year]['%']))
            print report
        #-Set Attributes-#
        self.level = 3
        self.data_type = data_type
        if adjust_units:
            self._units_value = 1
            self._units_value_str = "$'s"
        #-Set Dataset to Object-#
        self._dataset = df


    def construct_dataset_SC_CNTRY_SR2L3_Y62to00_A(self, data_type, dataset_object=True, verbose=True):
        """
        Complete Dataset Constructor for Dataset A
       
        Settings: dropAX=False, sitcr2=False, drop_nonsitcr2=False, intertemp_cntrycode=False, drop_incp_cntrycode=False

        Parameters
        -----------
        data_type       :   str
                            Specify data type 'trade', 'export', 'import'
        dataset_object  :   bool, optional(default=True)
                            Return a dataset object

        Notes
        ----- 
        1. For Export/Import Data should use construct_dataset_SC_CNTRY_SR2L3_Y62to00(data_type='export'/'import') 
            as can aggregate with NES on the importer partner side which is dropped in the cleaned trade database

        """
        warnings.warn("Deprecated in preference for construct_sitc_dataset method. This method doesn't include HK-CHINA Adjustments", PendingDeprecationWarning)
        self.construct_dataset_SC_CNTRY_SR2L3_Y62to00(data_type=data_type, dropAX=False, sitcr2=False, drop_nonsitcr2=False, intertemp_cntrycode=False, drop_incp_cntrycode=False, report=verbose, verbose=verbose)
        if dataset_object:
            self.notes = "Computed with options dropAX=False, sitcr2=False, drop_nonsitcr2=False, intertemp_cntrycode=False, drop_incp_cntrycode=False"
            obj = self.to_nberwtf(data_type=data_type)
            return obj

    def construct_dataset_SC_CNTRY_SR2L3_Y62to00_B(self, data_type, dataset_object=True, verbose=True):
        """
        Dataset Constructor for Dataset B
        
        Settings: dropAX=True, sitcr2=True, drop_nonsitcr2=True, intertemp_cntrycode=False, drop_incp_cntrycode=False

        Parameters
        -----------
        data_type       :   str
                            Specify data type 'trade', 'export', 'import'
        dataset_object  :   bool, optional(default=True)
                            Return a dataset object

        Notes
        ------ 
        1. For Export/Import Data should use construct_dataset_SC_CNTRY_SR2L3_Y62to00(data_type='export'/'import') 
            as can aggregate with NES on the importer partner side which is dropped in the cleaned trade database
        """
        warnings.warn("Deprecated in preference for construct_sitc_dataset method. This method doesn't include HK-CHINA Adjustments", PendingDeprecationWarning)
        self.construct_dataset_SC_CNTRY_SR2L3_Y62to00(data_type=data_type, dropAX=True, sitcr2=True, drop_nonsitcr2=True, intertemp_cntrycode=False, drop_incp_cntrycode=False, report=verbose, verbose=verbose)
        if dataset_object:
            self.notes = "Computed with options dropAX=True, sitcr2=True, drop_nonsitcr2=True, intertemp_cntrycode=False, drop_incp_cntrycode=False"
            obj = self.to_nberwtf(data_type=data_type)
            return obj

    def construct_dataset_SC_CNTRY_SR2L3_Y62to00_C(self, data_type, dataset_object=True, verbose=True):
        """
        Dataset Constructor for Dataset C
        
        Settings: dropAX=True, sitcr2=True, drop_nonsitcr2=True, intertemp_cntrycode=True, drop_incp_cntrycode=False

        Parameters
        -----------
        data_type       :   str
                            Specify data type 'trade', 'export', 'import'
        dataset_object  :   bool, optional(default=True)
                            Return a dataset object

        Notes
        ----- 
        1. For Export/Import Data should use construct_dataset_SC_CNTRY_SR2L3_Y62to00(data_type='export'/'import') 
            as can aggregate with NES on the importer partner side which is dropped in the cleaned trade database
        """
        warnings.warn("Deprecated in preference for construct_sitc_dataset method. This method doesn't include HK-CHINA Adjustments", PendingDeprecationWarning)
        self.construct_dataset_SC_CNTRY_SR2L3_Y62to00(data_type=data_type, dropAX=True, sitcr2=True, drop_nonsitcr2=True, intertemp_cntrycode=True, drop_incp_cntrycode=False, report=verbose, verbose=verbose)
        if dataset_object:
            self.notes = "Computed with options dropAX=True, sitcr2=True, drop_nonsitcr2=True, intertemp_cntrycode=True, drop_incp_cntrycode=False"
            obj = self.to_nberwtf(data_type=data_type)
            return obj

    def construct_dataset_SC_CNTRY_SR2L3_Y62to00_D(self, data_type, dataset_object=True, verbose=True):
        """
        Dataset Constructor for Dataset D
        
        Settings: dropAX=True, sitcr2=True, drop_nonsitcr2=True, intertemp_cntrycode=True, drop_incp_cntrycode=True

        Parameters
        -----------
        data_type       :   str
                            Specify data type 'trade', 'export', 'import'
        dataset_object  :   bool, optional(default=True)
                            Return a dataset object

        Notes
        ----- 
        1.  For Export/Import Data should use construct_dataset_SC_CNTRY_SR2L3_Y62to00(data_type='export'/'import') 
            as can aggregate with NES on the importer partner side which is dropped in the cleaned trade database
        """
        warnings.warn("Deprecated in preference for construct_sitc_dataset method. This method doesn't include HK-CHINA Adjustments", PendingDeprecationWarning)
        self.construct_dataset_SC_CNTRY_SR2L3_Y62to00(data_type=data_type, dropAX=True, sitcr2=True, drop_nonsitcr2=True, intertemp_cntrycode=True, drop_incp_cntrycode=True, report=verbose, verbose=verbose)
        if dataset_object:
            self.notes = "Computed with options dropAX=True, sitcr2=True, drop_nonsitcr2=True, intertemp_cntrycode=True, drop_incp_cntrycode=True"
            obj = self.to_nberwtf(data_type=data_type)
            return obj

    # ------------------------------------ END REMOVAL ----------------------------------- #

    #-Dataset Construction Using Internal Methods-#

    def construct_dynamically_consistent_dataset(self, no_index=True, verbose=True):
        """
        Constructs DEFAULT Dynamically Consistent Dataset for ProductCodes and CountryCodes
        Note: This can make debugging more difficult, and may wish to use an _SC_ dataset method (Self Contained)

        STATUS: **IN WORK** {Requires Testing}

        Notes
        -----
        1. Operations - Option A ::

                [1] Merge in Data at Raw Data Stage (China/HK Adjustments) & Delete sitc4 code issues
                [2] Collapse to Values Only (Keeping only ['year', 'icode', 'ecode', 'sitc4']) removes the Quantity Disaggregation
                [1] Alter Country Codes to be Intertemporally Consistent Units of Analysis (i.e. SUN = Soviet Union)
                [2] Collapse Values to SITCL3 Data
                [3] Remove Problematic Codes


        ..  Future Work
            -----------
            1. Work through these steps to ensure operations are on dataset and they flow from one to another
            2. Write Tests for these methods as a priority
                [A] adjust_china_hongkongdata
                [B] collapse_to_valuesonly

        """
        if self.complete_dataset != True:
            raise ValueError("Dataset must be a complete dataset!") 
        #--Merges at RAW DATA Phase--#
        self.delete_sitc4_issues_with_raw_data(verbose=verbose)
        self.load_china_hongkongdata(years=self.years, verbose=verbose)                                     #Bring in China/HongKong Adjustments to supp_data
        self.adjust_china_hongkongdata(verbose=verbose)
        #-Reduction/Collapse-#
        self.collapse_to_valuesonly(subidx=['year', 'icode', 'ecode', 'sitc4'], verbose=verbose)            #This will remove unit, quantity, dot, exporter and importer
        #--Collapse to SITCL3--#
        self.collapse_to_productcode_level(level=3, verbose=verbose)        #Collapse to SITCL3 Level
        self.drop_alpha_productcodes(verbose=verbose)                       #Drop 1984 to 2000 alpha codes 
        #--Intertemporal CountryCode Adjustments--# 
        self.adjust_countrycodes_intertemporal(verbose=verbose)     #Adds in iiso3c, eiso3c etc
        #--Addition/Corrections to Dataset--#
        self.change_value_units(verbose=verbose)                            #Change Units to $'s
        self.add_sitcr2_official_marker(level=3, verbose=verbose)           #Build SITCR2 Marker    
        self._dataset = self.dataset.set_index(keys=['year', 'eiso3c', 'iiso3c', 'sitc3'])
        if verbose:
            df = self.dataset
            print "Dataset Summary:"
            print "----------------"
            print "Number of Observations: %s" % df.shape[0]
            print "Number of Years: %s" % len(df.index.levels[0])
            print "Number of Exporters: %s" % len(df.index.levels[1])
            print "Number of Importers: %s" % len(df.index.levels[2])
            print "Number of Products: %s" % len(df.index.levels[3])
        if no_index:
            self._dataset = self.dataset.reset_index()
        return self.dataset

    #---------------------#
    # -- END MIGRATION -- #
    #---------------------#



    #-----------------------------#
    #-PyEconLab Object Interfaces-#
    #-----------------------------#

    def attach_attributes_to_dataset(self):
        #-Attach Transfer Attributes-#
        self._dataset.txf_name              = self._name
        self._dataset.txf_data_type         = self.data_type
        self._dataset.txf_classification    = self.classification
        self._dataset.txf_revision          = self.revision 
        self._dataset.txf_complete_dataset  = self.complete_dataset
        self._dataset.txf_notes             = self.notes
        self._dataset.txf_source_revision   = self.source_revision
        self._dataset.txf_units_value_str   = self._units_value_str
        return self._dataset

    def to_nberwtf(self, data_type, values_only=True, generic=False, verbose=True):
        """
        Construct NBERWTF Object with Common Core Object Names
        Note: This is constructed from the ._dataset attribute

        This will export the cleaned bilateral data to the NBERWTF object. 

        Parameters
        ----------
        data_type   :   str
                        Specify type of data 'trade', 'export', 'import'
        generic     :   bool, optional(default=False)
                        Return a generic data class (i.e. CPImportData)

        Notes
        -----
        1. Object Interface's ::

            'trade'     :   ['year', iiso3c', 'eiso3c', 'sitc[1-4]', 'value']   (Optional: 'quantity'?)
            'export'    :   ['year', 'eiso3c', 'sitc[1-4]', 'value']
            'import'    :   ['year', 'iiso3c', 'sitc[1-4]', 'value']

        ..  Future Work 
            -----------
            1. Turn data_type into a self.data_type attribute!
        """

        self.data_type = data_type

        sitcl = 'sitc%s' % self.level
        self._dataset = self.dataset.rename_axis({sitcl : 'productcode'}, axis=1)
        self.attach_attributes_to_dataset()

        if values_only:
            valid = set(['year', 'iiso3c', 'eiso3c', 'productcode', 'value'])
            for item in self._dataset.columns:
                if item not in valid:
                    if verbose: print "[INFO] Dropping column: %s (due to values_only option)" % item
                    del self._dataset[item]

        if data_type == 'trade':
            if generic:
                return CPTradeData(self.dataset)
            return NBERWTFTradeData(self.dataset)
        elif data_type == 'export' or data_type == 'exports':
            if generic:
                return CPExportData(self.dataset)
            return NBERWTFExportData(self.dataset)
        elif data_type == 'import' or data_type == 'imports':
            if generic:
                return CPImportData(self.dataset)
            return NBERWTFImportData(self.dataset)
        else:
            raise ValueError("data_type must be either 'trade', 'export(s)', or 'import(s)'")

    def to_dynamic_productleveltradesystem(self, verbose=True):
        """
        Method to construct a ProductLevelTradeSystem from the dataset
        
        STATUS: NOT IMPLEMENTED
        
        Notes
        -----
        1. Shouldn't this be taken care of by the dataset object to reduce duplication.

        """
        raise NotImplementedError

    def to_dynamic_productlevelexportsystem(self, verbose=True):
        """
        Method to construct a Product Level Export System from the dataset
        Warning: This assumes the dataset contains the intended data
        Note: This requires 'export' data

        Notes
        -----
        1. Shouldn't this be taken care of by the dataset object to reduce duplication. 

        """
        print "[WARNING] This method assumes the data in .dataset is the intended data"
        #-Prepare Names-#
        self._dataset.rename(columns={'eiso3c' : 'country', 'sitc3' : 'productcode', 'value' : 'export'}, inplace=True)
        self._dataset.set_index(['year'], inplace=True)
        #-Construct Object-#
        from pyeconlab.trade.systems import DynamicProductLevelExportSystem
        system = DynamicProductLevelExportSystem()
        system.from_df(df=self.dataset)
        return system

    def to_dynamic_productlevelimportsystem(self, verbose=True):
        """
        Method to construct a Product Level Import System from the dataset
        Note: This requires 'import' data

        STATUS: NOT IMPLEMENTED

        Notes
        -----
        1. Shouldn't this be taken care of by the dataset object to reduce duplication. 

        """
        raise NotImplementedError
    

    # --------------------------------------------------------------- #
    # - META DATA FUNCTIONS                                         - #
    #                                                                 #
    # - Note: These are largely for internal package construction   - #
    # - And for intertemporal diagnosis tables for sitc4 etc        - #
    # --------------------------------------------------------------- #

    def generate_global_info(self, target_dir, out_type='py', verbose=False):
        """
        Construct Global Information About the Dataset 
        
        Automatically import ALL data and Construct Unique ::
        
            [1] Country List    [Unique List of Countries in the Dataset]
            [2] Exporter List   [Unique List of Exporters in the Dataset]
            [3] Importer List   [Unique List of Exporters in the Dataset]
            [4] CountryName to ISO3C (REGEX) {py file only} [Requires Manual Adjustment]
            [5] CountryName to ISO3N (REGEX) {py file only} [Requires Manual Adjustment]

        Parameters:
        -----------
        target_dir  :   str
                        target directory where files are to be written. Should specify REPO Location if updating REPO Files OR DATA_PATH if replace in Installed Package
        out_type    :   str, optional(default='py')
                        file type for results files 'csv', 'py' 

        Notes
        -----
        1. Usage ::

            Useful if NBER Feenstra's Dataset get's updated etc. OR for constructing manual country concordances etc.

        2. The CountryName Concordances are automatically generated (currently using pycountrycode). These files should be checked for accuracy
        

        ..  Future Work:
            ------------
            1.  Maybe this sort of information should be compiled by parsing the source files and importing only 
                the required information to save on time and memory! 
                (Update: Current Implementation of read.stata() doesn't allow selective import due to being a binary file)
        """
        # - Check if Dataset is Complete for Global Info Property - #
        if self.complete_dataset != True:
            raise ValueError("Dataset must be complete. Try running the constructor without defining years=[]")
        # - Summary - #
        if verbose: 
            print "[INFO] Writing Exporter, Importer, and CountryLists to %s files in location: %s" % (out_type, target_dir)
        #-CSV-#
        if out_type == 'csv':
            # - Exporters - #
            pd.DataFrame(self.exporters, columns=['exporter']).to_csv(target_dir + 'exporters_list.csv', index=False)
            # - Importers - #
            pd.DataFrame(self.importers, columns=['importer']).to_csv(target_dir + 'importers_list.csv', index=False)
            # - Country List - #
            pd.DataFrame(self.country_list, columns=['country_list']).to_csv(target_dir + 'countryname_list.csv', index=False)
        #-PY-#
        elif out_type == 'py':
            # - Exporters - #
            s = pd.Series(self.exporters)
            s.name = 'exporters'
            from_series_to_pyfile(s, target_dir=target_dir, fl='exporters.py', docstring=self._name+': exporters'+'\n'+self.source_web)
            # - Importers - #
            s = pd.Series(self.importers)
            s.name = 'importers'
            from_series_to_pyfile(s, target_dir=target_dir, fl='importers.py', docstring=self._name+': importers'+'\n'+self.source_web)
            # - Country List - #
            s = pd.Series(self.country_list)
            s.name = 'countries'
            from_series_to_pyfile(s, target_dir=target_dir, fl='countrynames.py', docstring=self._name+': country list'+'\n'+self.source_web)
            # - CountryName to ISO3C, ISO3N Concordance - #
            self.countryname_concordance_using_cc(target_dir=target_dir, concord_vars=('countryname', 'iso3c'), verbose=verbose)    #-This Obfuscates file construction and should probably return a dict with a matching from_dict_to_pyfile etc
            self.countryname_concordance_using_cc(target_dir=target_dir, concord_vars=('countryname', 'iso3n'), verbose=verbose)
        else:
            raise TypeError("out_type: Must be of type 'csv' or 'py'")
        
    def write_metadata(self, target_dir='./meta', verbose=True):
        """
        Write Basic Files for ./meta in Excel Format for inclusion into the REPO

        Notes
        -----
        1. Files ::

            [1] intertemporal_iiso3n.xlsx
            [2] intertemporal_eiso3n.xlsx
            [3] intertemporal_sitc4.xlsx

        2. Excel is currently used as an easy way to inspect the data
        3. Not using local package location as mostly want to check output prior to entry through repo.

        """
        #-Parse Directory-#
        target_dir = check_directory(target_dir)
        if verbose: print "Writing Meta Data Files to: %s" % target_dir
        #-Compute Intertemporal Tables of iiso3n and eiso3n-#
        table_iiso3n, table_eiso3n = self.intertemporal_countrycodes(verbose=verbose)
        table_iiso3n.to_excel(target_dir + 'intertemporal_iiso3n.xlsx')
        table_eiso3n.to_excel(target_dir + 'intertemporal_eiso3n.xlsx')
        #-Compute Intertemporal ProductCodes-#
        table_sitc4 = self.intertemporal_productcodes(verbose=verbose)
        table_sitc4.to_excel(target_dir + 'intertemporal_sitc4.xlsx')


    ### ------------------------ ###
    ### - Intertemporal Tables - ###
    ### ------------------------ ###


    def intertemporal_tables(self, idx=['eiso3c'], value='value', force=False, verbose=False):
        """
        Construct an Intertemporal Table (Wide Table of Data)
        
        Parameters
        ----------
        idx         :   list, optional(default=['eiso3c'])
                        Specify Index Element for Table
        value       :   str, optional(default='value')
                        Specify name for value variable
        force       :   bool, optional(default=False)
                        Force operation over an incomplete dataset. Useful for Testing etc.

        Returns
        -------
        dataframe (intertemporal table)

        """
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("[ERROR] Not a Complete Dataset!")
        #-Get Dataset-#
        df = self.dataset.copy(deep=True) 
        df = df[idx + ['year'] + [value]].groupby(idx +['year']).sum().unstack(level='year')
        return df

    # - Country Codes Meta - #

    def intertemporal_countrycodes(self, dataset=False, force=False, verbose=False):
        """
        Wrapper for Generating intertemporal_countrycodes FROM 'raw_data' or 'dataset'

        Parameters
        ----------
        dataset     :   bool, optional(default=False)
                        Work on dataset (if False then raw_data)
        force       :   bool, optional(default=False)
                        Force method to be performed on incomplete data. Useful for testing. 

        """
        if dataset:
            if verbose: print "Constructing Intertemporal Country Code Tables from Dataset ..."
            table_iiso3n, table_eiso3n = self.intertemporal_countrycodes_dataset(force=force, verbose=verbose)
            return table_iiso3n, table_eiso3n
        else:
            if verbose: print "Constructing Intertemporal Country Code Tables from RAW DATA ..."
            table_iiso3n, table_eiso3n = self.intertemporal_countrycodes_raw_data(force=force, verbose=verbose)
            return table_iiso3n, table_eiso3n

    def intertemporal_countrycodes_raw_data(self, force=False, verbose=False):
        """
        Construct a table of importer and exporter country codes by year from RAW DATA
        Intertemporal Country Code Tables can also be computed from dataset using .intertemporal_countrycodes_dataset()
        which includes iso3c etc.

        Parameters
        ----------
        force       :   bool, optional(default=False)
                        Force method to be performed on incomplete data. Useful for testing. 

        Returns
        -------
        table_iiso3n, table_eiso3n

        """
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("[ERROR] Not a Complete Dataset!")
        #-Get Raw Data -#
        data = self.__raw_data        
        #-Split Codes-#
        if not check_operations(self, u"(split_countrycodes)"):                          #Requires iiso3n, eiso3n
            if verbose: print "[INFO] Running .split_countrycodes() as is required ..."
            self.split_countrycodes(dataset=False, verbose=verbose)
        #-Core-#
        #-Importers-#
        table_iiso3n = data[['year', 'importer', 'icode', 'iiso3n']].drop_duplicates().set_index(['importer', 'icode', 'year'])
        table_iiso3n = table_iiso3n.unstack(level='year')
        table_iiso3n.columns = table_iiso3n.columns.droplevel()     #Removes Unnecessary 'iiso3n' label
        #-Exporters-#
        table_eiso3n = data[['year', 'exporter', 'ecode', 'eiso3n']].drop_duplicates().set_index(['exporter', 'ecode', 'year'])
        table_eiso3n = table_eiso3n.unstack(level='year')
        table_eiso3n.columns = table_eiso3n.columns.droplevel()     #Removes Unnecessary 'eiso3n' label
        return table_iiso3n, table_eiso3n

    def intertemporal_countrycodes_dataset(self, cid='default', force=False, verbose=False):
        """
        Construct a table of importer and exporter country codes by year from DATASET
        This includes iso3c and is useful when using .countries_only() etc.
        
        Parameters
        ----------
        cid         :   str, optional(default='default')
                        Specify Country Indicator. 
        force       :   bool, optional(default=False)
                        Force method to be performed on incomplete data. Useful for testing. 

        Returns
        -------
        table_iiso3n, table_eiso3n

        """
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("[ERROR] Not a Complete Dataset!")
        #-Get Dataset-#
        data = self.dataset 
        #-Split Codes-#
        if 'eiso3n' not in data.columns or 'iiso3n' not in data.columns:
            if verbose: print "Running .split_countrycodes(force=True) as is required ..."
            self.split_countrycodes(iso3n_only=True, force=True, verbose=verbose)
        if not check_operations(self, u"add_iso3c"):                    
            if verbose: print "Running .add_iso3c() as is required ..."
            self.add_iso3c(verbose=verbose)
        #-Core-#
        cid_options = set(['importer', 'icode', 'iiso3n', 'iiso3c', 'exporter', 'ecode', 'eiso3n', 'eiso3c'])   #Should this be a class attribute?
        if cid == 'default':
            cid = set(data.columns)
            cid = cid.intersection(cid_options)
        else:
            cid = set(cid)
            cid = cid.intersection(cid_options)
        if verbose: print "[INFO] Using country id code types: %s" % cid
        #-Compute icid, ecid-#
        icid, ecid = set(['iiso3n']), set(['eiso3n'])           # Enforce ISO3N
        for item in cid: 
            if item[0] == 'i':
                icid.add(item)
            elif item[0] == 'e':
                ecid.add(item)
        #-Importers-#
        if verbose: print "[INFO] icid: %s" % list(icid)
        table_iiso3n = data[['year'] + list(icid)].drop_duplicates()
        icid.remove('iiso3n')                                       #Fill Table with iiso3n data
        idx = list(icid) + ['year']
        table_iiso3n = table_iiso3n.set_index(idx)
        table_iiso3n = table_iiso3n.unstack(level='year')
        table_iiso3n.columns = table_iiso3n.columns.droplevel()     #Removes Unnecessary 'iiso3n' label
        table_iiso3n.index = table_iiso3n.index.reorder_levels(order=['iiso3c', 'importer', 'icode'])
        #-Exporters-#
        if verbose: print "[INFO] ecid: %s" % list(ecid)
        table_eiso3n = data[['year'] + list(ecid)].drop_duplicates()
        ecid.remove('eiso3n')
        idx = list(ecid) + ['year']
        table_eiso3n = table_eiso3n.set_index(idx)
        table_eiso3n = table_eiso3n.unstack(level='year')
        table_eiso3n.columns = table_eiso3n.columns.droplevel()     #Removes Unnecessary 'eiso3n' label
        table_eiso3n.index = table_eiso3n.index.reorder_levels(order=['eiso3c', 'exporter', 'ecode'])
        return table_iiso3n, table_eiso3n

    # ---------------------- #
    # - Product Codes Meta - #
    # ---------------------- #

    def intertemporal_productcodes_raw_data(self, force=False, verbose=False):
        """
        Construct a table of productcodes by year
        
        Parameters
        ----------
        force       :   bool, optional(default=False)
                        Force method to be performed on incomplete data. Useful for testing. 

        """
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("[ERROR] Not a Complete Dataset!")
        #-RAW DATA-#
        data = self.raw_data        
        #-Split Codes-#
        if not check_operations(self, u"(split_countrycodes)"):                         #Requires iiso3n, eiso3n
            if verbose: print "Running .split_countrycodes() as is required ..."
            self.split_countrycodes(dataset=False, verbose=verbose)
        #-Core-#
        table_sitc4 = data[['year', 'sitc4']].drop_duplicates()
        table_sitc4['code'] = 1
        table_sitc4 = table_sitc4.set_index(['sitc4', 'year']).unstack(level='year')
        #-Drop TopLevel Name in Columns MultiIndex-#
        table_sitc4.columns = table_sitc4.columns.droplevel()   #Removes Unnecessary 'code' label
        return table_sitc4

    def intertemporal_productcodes_dataset(self, tabletype='indicator', meta=True, countries='None', cpidx=True, source_institution='un', level=-1, force=False, verbose=False):
        """
        Construct a table of productcodes by year
        This is different to the RAW DATA method as it adds in meta data such as SITC ALPHA MARKERS AND Official SITCR2 Indicator
        
        Parameters
        ----------
        tabletype   :   str, optional(default='indicator')
                        Set type of table 'indicator', 'value', or 'composition'
        meta        :   bool, optional(default=True)
                        Adds SITCR2 Marker in the Index
        countries   :   str, optional(default='None')
                        Specify country identifier 'exporter', 'importer' [Default = 'None' performs analysis at the product level w/out country disaggregation]
        cpidx       :   bool, optional(default=True)
                        Specify country index
        source_institution  :   str, optional(default='un')
                                Specify source_institution for data retrieval
        level       :   int, optional(default=-1)
                        Specify level setting for composition table
        force       :   bool, optional(default=False)
                        Force method to be performed on incomplete data. Useful for testing. 

        """
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("[ERROR] Not a Complete Dataset!")
        #-Core-#
        if tabletype == 'value':
            return self.intertemporal_productcodes_dataset_values(meta=meta, countries=countries, cpidx=cpidx, source_institution=source_institution, \
                                                                    force=force, verbose=verbose)
        elif tabletype == 'composition':
            return self.intertemporal_productcodes_dataset_compositions(meta=meta, countries=countries, cpidx=cpidx, source_institution=source_institution, \
                                                                    level=level, force=force, verbose=verbose)
        else:                                                                                                                                       # 'indicator' if not other match found
            return self.intertemporal_productcodes_dataset_indicator(meta=meta, countries=countries, cpidx=cpidx, source_institution=source_institution, \
                                                                    force=force, verbose=verbose)               
    


    def intertemporal_productcodes_dataset_indicator(self, meta=True, countries='None', cpidx=True, source_institution='un', force=False, verbose=False):
        """
        Construct a table of productcodes by year
        This is different to the RAW DATA method as it adds in meta data such as SITC ALPHA MARKERS AND Official SITCR2 Indicator
        
        Parameters
        -----------
        meta        :   bool, optional(default=True)
                        Adds SITCR2 Marker in the Index
        countries   :   str, optional(default='None')
                        Specify which index type to use 'exporter', 'importer'
        cpidx       :   bool, optional(default=True)
                        <update this>
        source_institution  :   str, optional(default='un')
                                Specify source institution for meta data
        force       :   bool, optional(default=False)
                        Force method to be performed on incomplete dataset

        """
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("[ERROR] Not a Complete Dataset!")
        sitcl = "sitc%s" % self.level
        #-DATASET-#
        data = self.dataset
        #-ParseCountries-#
        if countries == 'exporter':
            idx = ['year', 'exporter', sitcl]
        elif countries == 'importer':
            idx = ['year', 'importer', sitcl]
        else:
            idx = ['year', sitcl]
        #-Core-#                                                    
        table_sitc = data[idx].drop_duplicates()
        table_sitc['attr'] = 1                                                                                          
        table_sitc = table_sitc.set_index(idx).unstack(level='year')
        table_sitc.columns = table_sitc.columns.droplevel()                               #Drop TopLevel Name 'attr' in Columns MultiIndex
        #-Add Coverage Stats-#                                                            #Note this isn't classified as meta
        total_coverage = len(table_sitc.columns)
        table_sitc['Coverage'] = table_sitc.sum(axis=1)
        table_sitc['%Coverage'] = table_sitc['Coverage'] / total_coverage               
        #-Add Meta Devider-#
        table_sitc.insert(total_coverage,'META', '|')
        #-Add in Meta for ProductCodes-#
        if meta:
            #-SITCR2-#
            pidx = table_sitc.index.names
            table_sitc = table_sitc.reset_index()
            sitc = SITC(revision=2, source_institution=source_institution)
            codes = sitc.get_codes(level=self.level)
            table_sitc['SITCR2'] = table_sitc[sitcl].apply(lambda x: 1 if x in codes else 0)
            #-AX IDENTIFIERS-#
            table_sitc['SITCA'] = table_sitc['sitc%s' % self.level].apply(lambda x: 1 if re.search("[aA]",x) else 0)
            table_sitc['SITCX'] = table_sitc['sitc%s' % self.level].apply(lambda x: 1 if re.search("[xX]",x) else 0)
            #-ProductCode Names-#
            table_sitc["SITCNAME"] = table_sitc['sitc%s' % self.level].apply(lambda x: concord_data(sitc.code_description_dict(), x, issue_error="."))
            #-Set Index-#
            table_sitc = table_sitc.set_index(pidx + ['SITCR2', 'SITCA', 'SITCX'])
        if not cpidx and countries in ['exporter', 'importer']:
            if meta:
                table_sitc = table_sitc.reorder_levels([1,0,2,3,4]).sort_index()                                #Swap Country and Product
            else:
                table_sitc = table_sitc.reorder_levels([1,0]).sort_index() 
        return table_sitc

    def intertemporal_productcodes_dataset_values(self, meta=True, countries='None', cpidx=True, source_institution='un', force=False, verbose=False):
        """
        Construct a table of productcodes by year containing Values
        This is different to the RAW DATA method as it adds in meta data such as SITC ALPHA MARKERS AND Official SITCR2 Indicator
        

        Parameters
        ----------
        meta        :   bool, optional(default=True)
                        Adds SITCR2 Marker in the Index
        countries   :   str, optional(default='None')
                        Specify which index type to use 'exporter', 'importer'
        cpidx       :   bool, optional(default=True)
                        <update this>
        source_institution  :   str, optional(default='un')
                                Specify source institution for meta data
        force       :   bool, optional(default=False)
                        Force method to be performed on incomplete dataset

        """
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("[ERROR] Not a Complete Dataset!")
        sitcl = 'sitc%s' % self.level
        #-DATASET-#
        data = self.dataset 
        #-ParseCountries-#
        if countries == 'exporter':
            idx = ['year', sitcl, 'exporter', 'value']
            gidx = ['exporter', sitcl, 'year']
        elif countries == 'importer':
            idx = ['year', sitcl, 'importer', 'value']
            gidx = ['importer', sitcl, 'year']
        else:
            idx = ['year', sitcl, 'value']
            gidx = [sitcl, 'year']
        #-Core-#
        table_sitc = data[idx].groupby(gidx).sum()
        table_sitc = table_sitc.unstack(level='year')   
        table_sitc.columns = table_sitc.columns.droplevel()                                 #Drop 'value' Name in Columns MultiIndex
        #-Add in Meta for ProductCodes-#
        if meta:
            #-Add Meta Devider-#
            table_sitc['META'] = "|"
            #-RowTotals-#
            yearcols = []
            for year in self.years:
                yearcols.append(year)
            table_sitc['Tot'] = table_sitc[yearcols].sum(axis=1)
            table_sitc['Avg'] = table_sitc[yearcols].mean(axis=1)
            table_sitc['Min'] = table_sitc[yearcols].min(axis=1)
            table_sitc['Max'] = table_sitc[yearcols].max(axis=1)
            #-Coverage Stats-#
            coverage = self.intertemporal_productcodes_dataset_indicator(meta=False, countries=countries, cpidx=cpidx, force=force)[['Coverage', '%Coverage']]
            table_sitc = table_sitc.merge(coverage, left_index=True, right_index=True)
            #-SITCR2-#
            pidx = table_sitc.index.names
            table_sitc = table_sitc.reset_index()
            sitc = SITC(revision=2, source_institution=source_institution)
            codes = sitc.get_codes(level=self.level)
            table_sitc['SITCR2'] = table_sitc[sitcl].apply(lambda x: 1 if x in codes else 0)
            #-AX IDENTIFIERS-#
            table_sitc['SITCA'] = table_sitc['sitc%s' % self.level].apply(lambda x: 1 if re.search("[aA]",x) else 0)
            table_sitc['SITCX'] = table_sitc['sitc%s' % self.level].apply(lambda x: 1 if re.search("[xX]",x) else 0)
            #-ProductCode Names-#
            table_sitc["SITCNAME"] = table_sitc['sitc%s' % self.level].apply(lambda x: concord_data(sitc.code_description_dict(), x, issue_error="."))
            #-Set Index-#
            table_sitc = table_sitc.set_index(pidx + ['SITCR2', 'SITCA', 'SITCX'])
        if not cpidx and countries in ['exporter', 'importer']:
            if meta:
                table_sitc = table_sitc.reorder_levels([1,0,2,3,4]).sort_index()                                #Swap Country and Product
            else:
                table_sitc = table_sitc.reorder_levels([1,0]).sort_index() 
        return table_sitc

    def intertemporal_productcodes_dataset_compositions(self, meta=True, countries='None', cpidx=True, source_institution='un', level=-1, force=False, verbose=False):
        """
        Construct a table of productcodes by year
        This is different to the RAW DATA method as it adds in meta data such as SITC ALPHA MARKERS AND Official SITCR2 Indicator
        
        Parameters
        ----------
        meta        :   bool, optional(default=True)
                        Adds SITCR2 Marker in the Index
        countries   :   str, optional(default='None')
                        Specify which index type to use 'exporter', 'importer'
        cpidx       :   bool, optional(default=True)
                        <update this>
        source_institution  :   str, optional(default='un')
                                Specify source institution for meta data
        force       :   bool, optional(default=False)
                        Force method to be performed on incomplete dataset

        ..  Issue
            -----
            1. np.nan is being teated as 100% in composition Tables

        """
        if self.complete_dataset != True:
            if force == False:
                raise ValueError("[ERROR] Not a Complete Dataset!")
        #-Aggregation Level Option-#
        if level == -1:
            level = self.level - 1
        sitcl = 'sitc%s' % self.level
        #-DATASET-#
        data = self.dataset 
        #-ParseCountries-#
        if countries == 'exporter':
            idx = ['year', sitcl, 'exporter', 'value']
            gidx = ['exporter', sitcl, 'year']
        elif countries == 'importer':
            idx = ['year', sitcl, 'importer', 'value']
            gidx = ['importer', sitcl, 'year']
        else:
            idx = ['year', sitcl, 'value']
            gidx = [sitcl, 'year']
        #-Core-#
        table_sitc = self.intertemporal_productcode_valuecompositions(level=level, countries=countries)         
        #-Add in Meta for ProductCodes-#
        if meta:
            #-Add Meta Devider-#
            table_sitc['META'] = "|"
            #-Mean/Min/Max-#
            yearcols = []
            for year in self.years:
                yearcols.append(year)
            table_sitc['Avg'] = table_sitc[yearcols].mean(axis=1)
            table_sitc['Min'] = table_sitc[yearcols].min(axis=1)
            table_sitc['Max'] = table_sitc[yearcols].max(axis=1)
            table_sitc['AvgNorm'] = table_sitc[yearcols].sum(axis=1).div(len(self.years))       #Normalised by Number of Years (Average Composition over Time) np.nan = 0
            #-Coverage-#
            coverage = self.intertemporal_productcodes_dataset_indicator(meta=False, countries=countries, cpidx=True, force=force)[['Coverage', '%Coverage']]   #need cpindex to merge
            table_sitc = table_sitc.merge(coverage, left_index=True, right_index=True)
            #-SITCR2-#
            pidx = table_sitc.index.names
            table_sitc = table_sitc.reset_index()
            sitc = SITC(revision=2, source_institution=source_institution)
            codes = sitc.get_codes(level=self.level)
            table_sitc['SITCR2'] = table_sitc[sitcl].apply(lambda x: 1 if x in codes else 0)
            #-AX IDENTIFIERS-#
            table_sitc['SITCA'] = table_sitc['sitc%s' % self.level].apply(lambda x: 1 if re.search("[aA]",x) else 0)
            table_sitc['SITCX'] = table_sitc['sitc%s' % self.level].apply(lambda x: 1 if re.search("[xX]",x) else 0)
            #-ProductCode Names-#
            table_sitc["SITCNAME"] = table_sitc['sitc%s' % self.level].apply(lambda x: concord_data(sitc.code_description_dict(), x, issue_error="."))
            #-Set Index-#
            table_sitc = table_sitc.set_index(pidx+['SITCR2', 'SITCA', 'SITCX'])
        if not cpidx and countries in ['exporter', 'importer']:
            if meta:
                table_sitc = table_sitc.reorder_levels([1,0,2,3,4]).sort_index()                                #Swap Country and Product
            else:
                table_sitc = table_sitc.reorder_levels([1,0]).sort_index() 
        return table_sitc

    def intertemporal_productcode_valuecompositions(self, level=-1, countries='None', verbose=False):
        """
        Produce Value Composition Tables for Looking at SITC4 relative to some other level of aggregation

        Parameters
        ----------
        level       :   int, optional(default=-1)
                        Aggregation Level to compute compositions for. Defaults to one level up. 
        countries   :   str, optional(default='None')  
                        Allow for Tables to be generated for exporter, level or importer, level or just level 

        ..  Future Work
            ----
            1. Write Tests
            2. This could be a dataframe.py utility
        """
        #-DATASET-#
        data = self.dataset
        #-Aggregation Level Option-#
        if level == -1:
            level = self.level - 1
        sitcld = 'sitc%s'% self.level
        sitcl = 'sitc%s' % level
        #-ParseCountries-#
        if countries == 'exporter':
            idx = ['year', sitcld, 'exporter', 'value']             #base
            gidx = ['exporter', sitcld, 'year']                     #groupby base idx
            lidx = ['year', sitcl, 'exporter', 'value']             #level idx
            glidx = ['year', sitcl, 'exporter']                     #groupby level idx
            midx = ['year', sitcld, sitcl.upper(), 'exporter']      #merge idx
            msidx = ['year', 'exporter', sitcld]
        elif countries == 'importer':
            idx = ['year', sitcld, 'importer', 'value']
            gidx = ['importer', sitcld, 'year']
            lidx = ['year', sitcl, 'importer', 'value']
            glidx = ['year', sitcl, 'importer']
            midx = ['year', sitcld, sitcl.upper(), 'importer']
            msidx = ['year', 'importer', sitcld]
        else:
            idx = ['year', sitcld, 'value']
            gidx = [sitcld, 'year']
            lidx = ['year', sitcl, 'value']
            glidx = ['year', sitcl]
            midx = ['year', sitcld, sitcl.upper()]
            msidx = ['year', sitcld]
        #-Core-#
        #-SITC4-#
        table_sitc = data[idx].groupby(gidx).sum().reset_index()
        table_sitc[sitcl] = table_sitc[sitcld].apply(lambda x: str(x)[:level])
        #-SITCL-#
        table_sitcl = data[idx].groupby(gidx).sum().reset_index()
        table_sitcl[sitcl] = table_sitcl[sitcld].apply(lambda x: str(x)[:level])
        table_sitcl = table_sitcl[lidx].groupby(glidx).sum().reset_index()
        #-Construct Table-#
        table = table_sitc.merge(table_sitcl, on=glidx)
        table[sitcl.upper()] = table['value_x'] / table['value_y'] * 100
        table = table[midx]
        table = table.set_index(msidx)
        table = table.unstack(level='year')
        table.columns = table.columns.droplevel()
        return table 

    def intertemporal_productcode_exporters(self, meta=True, force=False, verbose=False):
        """
        Compute Number of Country Exporters for Any Given ProductCode

        Parameters
        ----------
        meta    :   bool, optional(default=True)
                    Include meta data in tables
        force   :   bool, optional(default=False)
                    Force method to be performed on incomplete dataset        

        """
        df = self.intertemporal_productcodes_dataset_indicator(meta=meta, countries='exporter')
        df = df.reset_index()
        sitcl = 'sitc%s' % self.level
        if meta:
            df = df.groupby([sitcl, 'SITCR2']).sum()
            #-Higher Aggregation of Coverage Stats-#
            df = df.drop(['Coverage', '%Coverage'], axis=1)
            coverage = self.intertemporal_productcodes_dataset_indicator(meta=False, force=force)[['Coverage', '%Coverage']]
            df = df.merge(coverage, left_index=True, right_index=True)
        else:
            df = df.groupby([sitcl]).sum()
        return df


    def intertemporal_productcode_simple_adjustments_table(self, tabletype="indicator", cleanup=True, low_value_settings=(1,4), official_coverage=True, verbose=True):
        """
        Generate a Simple Product Code Adjustment Table

        This Method Generates an Intertemporal ProductCode Table that uses Meta Data to construct an Indicator for 
        whether the chapter level above the dataset contains any non-sitcr2 codes. This can assist in identifying 
        groups of products that contain compositional changes over time and are eligible for collapse to produce 
        an intertemporal productcode classification

        Parameters
        ----------
        tabletype   :   str, optional(default="indicator")
                        Produces an Intertemporal Table with Simple Indicators as Default (Can Choose "value")
        cleanup     :   bool, optional(default=True)
                        Cleanup Construction Variables

        """
        table = self.intertemporal_productcodes_dataset(tabletype=tabletype, meta=True, verbose=verbose)
        idx = list(table.index.names)
        table = table.reset_index()
        table["NOTSITCR2"] = table["SITCR2"].apply(lambda x: False if x == 1 else True)  #defines sitc revision 2 code
        if self.level < 2:
            raise ValueError("Level Cannot be Less than 2")
        table["sitc%s"%(self.level-1)] = table["sitc%s"%self.level].apply(lambda x: x[0:self.level-1])     #defines higher chapter level
        #-Group Analysis-#
        #-Check Any Non SITCR2 in Higher Chapter Level-#
        indicator = table[["NOTSITCR2","sitc%s"%(self.level-1)]].groupby("sitc%s"%(self.level-1))["NOTSITCR2"].any()              #For Each Higher Chapter Level see if any codes are not SITCR2          
        indicator.name = "NOTSITCR2-ING"
        table = table.join(indicator, on="sitc%s"%(self.level-1), how="left")
        idx.append("NOTSITCR2-ING")
        #-Compute Number of Items in Group-#
        num_items = table[["SITCR2","sitc%s"%(self.level-1)]].groupby("sitc%s"%(self.level-1))["SITCR2"].count()
        num_items.name = "SITC-COUNT-ING"
        table = table.join(num_items, on="sitc%s"%(self.level-1), how="left")
        idx.append("SITC-COUNT-ING")
        #-Compute Number of Non SITCR2 Items in Group-#
        num_sitcr2 = table[["SITCR2","sitc%s"%(self.level-1)]].groupby("sitc%s"%(self.level-1))["SITCR2"].sum()
        num_sitcr2.name = "SITCR2-SUM-ING"
        table = table.join(num_sitcr2, on="sitc%s"%(self.level-1), how="left")
        idx.append("SITCR2-SUM-ING")
        table["NUM-NOTSITCR2-ING"] = table["SITC-COUNT-ING"] - table["SITCR2-SUM-ING"]
        idx.append("NUM-NOTSITCR2-ING")
        #-Check if any group members are intertemporally inconsistent-#
        table["NotIntertempConsistent"] = table["%Coverage"].apply(lambda x: False if x == 1 else True)
        inconsistent = table[["NotIntertempConsistent", "sitc%s"%(self.level-1)]].groupby("sitc%s"%(self.level-1))["NotIntertempConsistent"].any()
        inconsistent.name = "NotIntertempConsistent-ING"
        table = table.join(inconsistent, on="sitc%s"%(self.level-1), how="left")
        idx.append("NotIntertempConsistent-ING")
        #-Check for Low Value Group Members-#
        if tabletype != "composition":
            data = self.intertemporal_productcode_simple_adjustments_table(tabletype="composition", verbose=verbose).reset_index()
        else:
            data = table
        rowavgnorm, rowmax = low_value_settings
        table["LowValue"] = data[["AvgNorm", "Max"]].apply(lambda row: True if (row["AvgNorm"] < rowavgnorm)&(row["Max"] < rowmax) else False, axis=1)
        low_value = table[["LowValue", "sitc%s"%(self.level-1)]].groupby("sitc%s"%(self.level-1))["LowValue"].any()
        low_value.name = "LowValue-ING"
        table = table.join(low_value, on="sitc%s"%(self.level-1), how="left")
        idx.append("LowValue")
        idx.append("LowValue-ING")
        #-Check Official Codes Coverage-#
        table["OfficialNotIC"] = table[["SITCR2", "%Coverage"]].apply(lambda row: True if (row["%Coverage"]<1)&(row["SITCR2"]==True) else False , axis=1)
        officialnotic = table[["OfficialNotIC", "sitc%s"%(self.level-1)]].groupby("sitc%s"%(self.level-1))["OfficialNotIC"].any()
        officialnotic.name = "OfficialNotIC-ING"
        table = table.join(officialnotic, on="sitc%s"%(self.level-1), how="left")
        idx.append("OfficialNotIC")
        idx.append("OfficialNotIC-ING")
        #-Set Index-#
        table = table.set_index(idx)
        if cleanup:
            del table["NOTSITCR2"]
            del table["sitc%s"%(self.level-1)]
            del table["NotIntertempConsistent"]
        return table

    def intertemporal_productcode_lists(self, tabletype="value", return_table=False, include_special=(True, "6200"), value_check=(True, 1, 4), official_coverage=True, verbose=True):
        """
        Return a list of items to Drop and Collapse to Produce an Intertemporally Consistent Set of Codes for NBER

        Returns
        -------
        drop_items, collapse_items
        OR
        drop_items, collapse_items, table
        """

        def all_sitcr2_in_group(df, level, column="VC", indicator="VK"):
            """ Check to see if non_sitcr2 code is in a chapter group """
            sitcg = "sitc%s"%(level-1)
            df[sitcg] = df["sitc%s"%level].apply(lambda x: x[0:level-1])
            df["ALLSITCR2"] = df["SITCR2"].apply(lambda x: True if x == 1 else 0)
            columns = [sitcg, "ALLSITCR2", column]
            result = df[columns].groupby([column, sitcg]).all()                 #Column will split results between VK and VD groups within the chapter definitions
            #-Cleanup Table-#
            del df[sitcg]
            del df["ALLSITCR2"]
            return result.ix[indicator]

        def check_vc_rule(allsitcr2ing, row):
            if row["VC"] == "VD":
                return False
            #-Parse Other Elements within that Group-#
            try:
                return allsitcr2ing.ix[row["sitc%s"%self.level][0:self.level-1]]["ALLSITCR2"]
            except:
                return False #SITC Code not in Results Table
 
        #-Core-#
        value_check, rowavgnorm, rowmax = value_check
        include_special, SpecialCase = include_special
        table = self.intertemporal_productcode_simple_adjustments_table(tabletype=tabletype, low_value_settings=(rowavgnorm, rowmax), verbose=verbose)
        idx = list(table.index.names)
        table = table.reset_index()
        #-Drop-#
        table["D"] = (table["SITC-COUNT-ING"] - table["NUM-NOTSITCR2-ING"]).apply(lambda x: "D" if x == 0 else ".")
        drop_items = set(table.loc[table.D == "D"]["sitc%s"%self.level].unique())
        #-Collapse-#
        #-Collapse Items that contain non official SITC R2 Codes within the SITC group to higher chapter level-#
        table["C"] = table["NOTSITCR2-ING"].apply(lambda x: "C" if x == True else ".")
        collapse_items = set(table.loc[table.C == "C"]["sitc%s"%self.level].unique())
        #-Itertemporal Check-#
        #-Collapse Items that have productcodes within the SITC group that are intertemporally incomplete-#
        table["IC"] = table["NotIntertempConsistent-ING"].apply(lambda x: "IC" if x == True else ".")        #Post 1974 and 1984 the products should not get dropped according to this rule (Check this!)
        table["IC"] = table[["SITC-COUNT-ING", "NotIntertempConsistent-ING", "IC"]].apply(lambda row: "ID" if (row["SITC-COUNT-ING"] == 1) & (row["NotIntertempConsistent-ING"]) else row["IC"] , axis=1)
        intertemp_collapse_items = set(table.loc[table.IC == "IC"]["sitc%s"%self.level].unique())
        intertemp_drop_items = set(table.loc[table.IC == "ID"]["sitc%s"%self.level].unique())
        if verbose:
            print "[INFO] Dropping Items becuase they cannot be collapsed ..."
            print drop_items
            print "[INFO] Collapsing Items becuase SITC group contains a non official sitc code ..."
            print collapse_items 
            print "[INFO] Collapsing Items becuase SITC group contains a code that is not intertemporally complete and cannot be collapsed..."
            print intertemp_collapse_items
            print "[INFO] Dropping Items becuase they are intertemporally incomplete and cannot be collapsed ..."
            print intertemp_drop_items
        #-Remove Drop Items from Collapse Items Due to Multiple Rules (Drop takes Preference to Collapse)-#
        drop_items = drop_items.union(intertemp_drop_items)
        collapse_items = collapse_items.union(intertemp_collapse_items).difference(drop_items)                            #Can be Overlap based on Identifying Rules
        if value_check:
            #-Value Check-#
            table["VC"] = table[["SITCR2", "LowValue"]].apply(lambda row: "VD" if (row["LowValue"] == True)&(row["SITCR2"]==False) else ".", axis=1)
            #table["VC"] = table[["LowValue-ING", "VC"]].apply(lambda row: "VK" if (row["LowValue-ING"])&(row["VC"]!="VD") else row["VC"], axis=1) #This might not be a 100% reliable so undertake a Manual Review of these items. This requires checking if there are still nonsitr2 codes or significant intertemp inconsistent lines
            table["VC"] = table[["LowValue-ING", "VC"]].apply(lambda row: "VC" if (row["LowValue-ING"])&(row["VC"]!="VD") else row["VC"], axis=1) #This might not be a 100% reliable so undertake a Manual Review of these items. This requires checking if there are still nonsitr2 codes or significant intertemp inconsistent lines
            #-Keep Adjustments-#
            allsitcr2ing = all_sitcr2_in_group(table, level=self.level, column="VC", indicator="VC")
            table["VC"] = table[["sitc%s"%self.level, "VC"]].apply(lambda row: "VK" if check_vc_rule(allsitcr2ing, row) == True else row["VC"], axis=1)
            #-Set's-#
            value_collapse_items = set(table.loc[table.VC == "VC"]["sitc%s"%self.level].unique())
            value_keep_items = set(table.loc[table.VC == "VK"]["sitc%s"%self.level].unique())
            value_drop_items = set(table.loc[table.VC == "VD"]["sitc%s"%self.level].unique())
            if verbose:
                print "[INFO] Dropping Items due to insignificant values ..."
                print value_drop_items
                print "[INFO] Reassign from 'C' to 'K' to Keep Items due to dropping items within the sitc group ... (manually check these items)"
                print value_keep_items      #-!!-Not Adding this to Rule by Default-!!-#
            #-Adjust-#
            drop_items = drop_items.union(value_drop_items) - value_keep_items
            collapse_items = collapse_items.union(value_collapse_items) - value_keep_items - value_drop_items
        if official_coverage:
            #-Check Official Codes that don't have complete intertemporal coverage-#
            table["OC"] = table["OfficialNotIC-ING"].apply(lambda x: "OC" if x else ".")
            table["JVCOC"] = table[["VC", "OC"]].apply(lambda row: "OC" if (row["VC"]!=".")&(row["OC"]=="OC") else ".", axis=1)
            official_coverage_collapse = set(table.loc[table.JVCOC == "OC"]["sitc%s"%self.level].unique())
            #-Adjust-#
            drop_items = drop_items - official_coverage_collapse
            collapse_items = collapse_items.union(official_coverage_collapse)
        if include_special:
            #-Update with Special Codes Set Manually-#
            from .meta import IntertemporalProducts
            if SpecialCase == "6200":  
                print "[INFO] Using special cases for IC6200"
                special_drop = set(IntertemporalProducts().IC6200SpecialCases["L%s"%self.level]["drop"])
                special_collapse = set(IntertemporalProducts().IC6200SpecialCases["L%s"%self.level]["collapse"])
                special_keep = set(IntertemporalProducts().IC6200SpecialCases["L%s"%self.level]["keep"])
                recode = IntertemporalProducts().IC6200SpecialCases["L%s"%self.level]["recode"]                     #Dictionary
            elif SpecialCase == "7400":
                print "[INFO] Using special cases for IC7400"
                special_drop = set(IntertemporalProducts().IC7400SpecialCases["L%s"%self.level]["drop"])
                special_collapse = set(IntertemporalProducts().IC7400SpecialCases["L%s"%self.level]["collapse"])
                special_keep = set(IntertemporalProducts().IC7400SpecialCases["L%s"%self.level]["keep"])
                recode = IntertemporalProducts().IC7400SpecialCases["L%s"%self.level]["recode"]                     #Dictionary
            elif SpecialCase == "8400":
                print "[INFO] Using special cases for IC8400"
                special_drop = set(IntertemporalProducts().IC8400SpecialCases["L%s"%self.level]["drop"])
                special_collapse = set(IntertemporalProducts().IC8400SpecialCases["L%s"%self.level]["collapse"])
                special_keep = set(IntertemporalProducts().IC8400SpecialCases["L%s"%self.level]["keep"])
                recode = IntertemporalProducts().IC8400SpecialCases["L%s"%self.level]["recode"]                     #Dictionary            
            special_recode = set(recode.keys())
            table["SP"] = table["sitc%s"%self.level].apply(lambda x: "SK" if x in special_keep else ".")
            table["SP"] = table[["sitc%s"%self.level,"SP"]].apply(lambda row: "SC" if row["sitc%s"%self.level] in special_collapse else row["SP"], axis=1)
            table["SP"] = table[["sitc%s"%self.level,"SP"]].apply(lambda row: "SD" if row["sitc%s"%self.level] in special_drop else row["SP"], axis=1)
            table["SP"] = table[["sitc%s"%self.level,"SP"]].apply(lambda row: "SR" if row["sitc%s"%self.level] in special_recode else row["SP"], axis=1)
            if verbose: 
                print "[INFO] Integrating Special Requests ..."
                print "special_drop = %s" % special_drop
                print "special_collapse = %s" % special_collapse
                print "special_keep = %s" % special_keep
                print "special_recode = %s" % special_recode
            #-Adjust-#
            drop_items = drop_items.union(special_drop) - special_keep - special_recode
            collapse_items = collapse_items.union(special_collapse) - special_keep - special_drop - special_recode
        #-Final Rule-#
        table["RULE"] = "K"     #Default Rule
        table["RULE"] = table[["sitc%s"%self.level,"RULE"]].apply(lambda row: "C" if row["sitc%s"%self.level] in collapse_items else row["RULE"], axis=1)
        if include_special:
            table["RULE"] = table[["sitc%s"%self.level,"RULE"]].apply(lambda row: "R(%s)"%recode[row["sitc%s"%self.level]] if row["sitc%s"%self.level] in special_recode else row["RULE"], axis=1)    
        table["RULE"] = table[["sitc%s"%self.level,"RULE"]].apply(lambda row: "D" if row["sitc%s"%self.level] in drop_items else row["RULE"], axis=1)
        warnings.warn("This requires a manual check in the event some K's should in fact be C due to the presence of nested within group SITCR2")
        table["CHECK"] = table[["C", "VC", "RULE"]].apply(lambda row: "<-- CHECK" if (row["VC"]=="VK")&(row["C"]=="C") else "", axis=1)
        #-Sortedness-#
        drop_items = sorted(drop_items)
        collapse_items = sorted(collapse_items)
        if verbose:
            print "Overall (Intertemporal Adjustments)"
            print "[INFO] Dropping Rows with SITC Codes: %s" % drop_items
            print "[INFO] Collapsing Rows with SITC Codes: %s" % collapse_items
        if return_table:
            return drop_items, collapse_items, table.set_index(idx)
        else:
            return drop_items, collapse_items

    def intertemporal_productcode_adjustments(self, drop_items, collapse_items, verbose=True):
        """ 
        Adjustments based on drop_items and collapse_items

        STATUS: IN-WORK

        """
        
        def check_collapse(value, collapse_code):
            if value == collapse_code:
                value = collapse_code[0:self.level-1]+"0"
            return value
        
        data = self.dataset.copy() 
        oshape = data.shape 
        #-Drop Items-#
        keep_items = set(data.sitc4.unique()).difference(set(drop_items))
        data = data.loc[data["sitc%s"%(self.level)].isin(keep_items)]
        #-Collapse Items-#
        for code in collapse_items:
            data["sitc%s"%self.level] = data["sitc%s"%self.level].apply(lambda x: check_collapse(x, code))
        data = data.groupby(data.columns).sum() #Collapse Repeated ProductCodes
        fshape = data.shape 
        if verbose: print "From %s to %s due to operation" % (oshape, fshape)
        return data



    # --> EXPERIMENTAL <-- #

    def intertemporal_productcode_adjustments_table(self, force=False, verbose=False):
        """
        [EXPERIMENTAL] Documents and Produces the Adjustments Table for Meta Data Reference 
        
        STATUS: EXPERIMENTAL

        Parameters
        ----------
        force       :   bool, optional(default=False)
                        Force method to be performed on incomplete dataset

        Notes
        -----
        1. Looking at SITCL3 GROUPS ::

            [1] For each Unofficial 'sitc4' Code Ending with '0' check if Official Codes in the SAME LEAF are CONTINUOUS. IF they ARE keep the CHILDREN as they are 
                disaggregated classified goods ELSE wrap up the data into the Unofficial '0' Code as these groups have intertemporal classification issues for the 1962-2000
                dataset

                [a] is dropping the '0' unofficial category systemically dropping certain countries data from the dataset?** See [2] in Notes
                [b] What is the value of unofficial codes categories by year?

        2. Looking at 'A' and 'X' Groups ::

            [1] Remove 'A' and 'X' Codes as they are assignable and hold relatively little data (these will be captured in SITCL3 Dataset as a robustness check)
                Supporting Evidence can be considered with .compute_valAX_sitclevel() method

            Method
            ------
            [1] Collapse A and X Codes into the Unofficial Code Line (IF AVAILABLE)
                IF NOT AVAILABE: DELETE
            [2] Check EACH SITCL3 GROUP and compare the Unofficial Code '0' with the Official Codes within the group. 
                IF Children == FULL COVERAGE (i.e. 39) then keep Children AND DELETE UNOFFICIAL Line
                IF Children != FULL COVERAGE (i.e 30) then collapse sum the Children into the Unoficial line item

        Notes
        -----
        1. '025*'' and '011*'' are good examples and test cases    
        2. Check meta/xlsx/intertemporal_sitc4_compositions_wmeta_adjustments.xlsx to review and check output
            {A} We should check how many countries are using the unofficial code vs. the official codes in each group as this may effect intercountry 
                variation. By deleting the Unofficial Level 3 Code this may be systemically removing exports from some developing countries. A full
                treatment of countries needs to be done for each SITCL3 subgroup to decide the best collapse and deletion strategy.
                Case Study: '057*' => Check Exporters using '0570' and exporters using '0571' to '0577'
        3. This table suggests that SITCL3 should be used for inter-temporal consistency re: 2-A remark. 

        """
        warnings.warn("EXPERIMENTAL")
        #-Core-#
        comp = self.intertemporal_productcodes_dataset(tabletype='composition', meta=True, level=3, force=force)
        idxnames = comp.index.names
        colnames = comp.columns
        comp = comp.reset_index()
        comp['SITCL3'] = comp['sitc4'].apply(lambda x: str(x)[:3])
        comp['4D'] = comp['sitc4'].apply(lambda x: str(x)[3:])           
        # comp = comp.loc[(comp.SITCL3 == '011') | (comp.SITCL3 == '025')]  #Test Filter. Think about special cases when remove
        r = pd.DataFrame()
        for idx, df in comp.groupby(by=['SITCL3']):
            s1 = df[['SITCR2', '%Coverage']].groupby(by=['SITCR2']).mean()['%Coverage'] #Average Coverage Within SITCL3
            try: 
                avg_official_coverage = s1[1]
            except: 
                avg_official_coverage = 0
            try:
                avg_notofficial_coverage = s1[0]
            except:
                avg_notofficial_coverage = 0
            s2 = df[['SITCR2', 'Avg']].groupby(by=['SITCR2']).sum()['Avg']
            try:
                comp_official = s2[1]
            except:
                comp_official = 0
            try:
                comp_notofficial = s2[0]
            except: 
                comp_notofficial = 0
            if avg_official_coverage > 0.95 and comp_official >0.95:
                df['ACTION'] = df['SITCR2'].apply(lambda x: 'K' if x == 1 else 'D')              #Keep Offical Codes, Delete Unofficial
            else:
                df['ACTION'] = 'C' # Collapse ALL Codes
            r = r.append(df[list(idxnames)+list(colnames)+['ACTION']])
        r = r.set_index(list(idxnames))
        return r


    def compute_cases_intertemporal_sitc4_3digits(self, force=False, verbose=False):
        """
        [EXPERIMENTAL] Compute a Cases Table for SITC4 Intertemporal ProductCodes at the 3 Digit Level

        STATUS: EXPERIMENTAL

        Notes
        -----
        1. Cases ::
        
            For Each SITC Level 3 Group apply a classifier for the following cases

            'Case1'     :   Avg(Official Coverage)                  > 95% (Official Codes are represented across 95% of the years on average)
                            Avg(Composition % of Official Codes)    > 95% (most of the value lies in Official encodings across al 39 years)
                            {This shows groups that are represented by a majority of trade flows in official SITCR2 Codes across the whole dataset i.e. 0011)

            'Case2'     :   Unofficial Code Ending in '0' is present
                            {This will highlight groups that have an Unofficial 3Digit Level Code Present in the data. 
                            These groups suffer heavily from inter-country heterogeneity in how similar products are grouped} 

            'Case3'     :   ??

        """
        warnings.warn("EXPERIMENTAL")
        #-Core-#
        comp = self.intertemporal_productcodes_dataset(tabletype='composition', meta=True, level=3, force=force)
        idxnames = comp.index.names
        colnames = comp.columns
        comp = comp.reset_index()
        comp['SITCL3'] = comp['sitc4'].apply(lambda x: str(x)[:3])
        comp['4D'] = comp['sitc4'].apply(lambda x: str(x)[3:])           
        # comp = comp.loc[(comp.SITCL3 == '011') | (comp.SITCL3 == '025')]  #Test Filter. Think about special cases when remove
        r = pd.DataFrame()
        for idx, df in comp.groupby(by=['SITCL3']):
            #-Case1-#
            s1 = df[['SITCR2', '%Coverage']].groupby(by=['SITCR2']).mean()['%Coverage'] #Average Coverage Within SITCL3
            try: avg_official_coverage = s1[1]
            except: avg_official_coverage = 0
            s2 = df[['SITCR2', 'Avg']].groupby(by=['SITCR2']).sum()['Avg']
            try: comp_official = s2[1]
            except: comp_official = 0
            if avg_official_coverage > 0.95 and comp_official >0.95:
                df['CASE1'] = 1
            else:
                df['CASE1'] = 0
            #-Case2-#
            if len(df.loc[(df.sitc4 == str(idx)+'0') & (df.SITCR2 == 0)]) == 1:
                df['CASE2'] = 1
            else:
                df['CASE2'] = 0
            r = r.append(df[list(idxnames)+list(colnames)+['CASE1', 'CASE2']])
        r = r.set_index(list(idxnames))
        return r


    # -------------- #
    # - Converters - #
    # -------------- #

    def convert_stata_to_hdf_yearindex(self, format='table', verbose=True):
        """
        Convert the Raw Stata Source Files to a HDF File Container indexed by Y#### (where #### = year)

        ..  Future Work
            -----------
            1. Integrity Checking against original dta file hash?
            2. Move this to a Utility?
            3. Is there a way to make this work across 4 cores writing separate container names? 
                May require separate h5 files

        """
        #Note: This might write into a dataset!
        years = self._available_years
        hdf_fn = self._source_dir + self.__cache_dir + self._fn_prefix + str(years[0])[-2:] + '-' + str(years[-1])[-2:] + '_yearindex' + '.h5'     
        pd.set_option('io.hdf.default_format', format)
        hdf = pd.HDFStore(hdf_fn, complevel=9, complib='zlib')
        self.__raw_data_hdf_yearindex = hdf                                     #SHould this be a filename rather than a Container?

        #-Convert Files -#                  
        for year in self._available_years:
            dta_fn = self._source_dir + self._fn_prefix + str(year)[-2:] + self._fn_postfix
            if verbose: print "Converting file: %s to file: %s" % (dta_fn, hdf_fn)
            #pd.read_stata(dta_fn).to_hdf(hdf_fn, 'Y'+str(year))
            hdf.put('Y'+str(year), pd.read_stata(dta_fn), format=format)
            
        print hdf
        hdf.close()
        gc.collect()


    def convert_raw_data_to_hdf(self, format='table', verbose=True):
        """
        Convert the Entire RAW Data Compilation to a HDF File with index 'raw_data'

        Parameters
        ----------
        format  :   str, optional(default='table')
                    Specify HDF File type

        ..  Future Work
            -----------
            1. Integrity Checking against original dta file hash?
            2. Move this to a Utility?

        """
        years = self._available_years
        hdf_fn = self._source_dir + self.__cache_dir + self._fn_prefix + str(years[0])[-2:] + '-' + str(years[-1])[-2:] +  '_raw' + '.h5'
        hdf = pd.HDFStore(hdf_fn, complevel=9, complib='zlib')
        hdf.put('raw_data', self.__raw_data, format=format)
        if verbose: print hdf
        hdf.close()
        gc.collect()

    #---------#
    #-Pickles-#
    #---------#

    def to_pickle(self, fn, data='dataset', verbose=False):
        """
        Send self.raw_data or self.dataset to a pickle file

        Parameters
        ----------
        fn  :   str
                Specify filename 
        data    :   str, optional(default='dataset')
                    Specify which data source to write (dataset or raw_data)

        Notes 
        -----
        1. This uses a LOT of memory when trying to write to the file. 

        """
        fl = open(fn, 'wb') 
        if data == 'dataset':
            pickle.dump(self.dataset, fl)
        elif data == 'raw_data' or data == 'raw':
            pickle.dump(self.raw_data, fl)
        else:
            raise ValueError("data must be either 'dataset' or 'raw_data'")
        fl.close()

    # - Issues with Raw Data - #
    # ------------------------ #

    def issues_with_raw_data(self):
        """
        Documents observed issues with the RAW DATA
        
        Warnings
        --------
        1. This produces files from where it is called

        Notes
        -----
        1. Missing SITC4 Codes (28 observations) -> './missing_sitc4.xlsx'
        """
        #-Missing Codes-#
        codelength = self.raw_data['sitc4'].apply(lambda x: len(x))
        self.raw_data[codelength != 4].to_excel('missing_sitc4.xlsx')


    # ----------------------- #
    # - Construct Test Data - #
    # ----------------------- #

    def testdata_collapse_to_valuesonly(self, size=10):
        """
        Produce Test Data to check method: collapse_to_valuesonly

        Parameters
        ----------
        size    :   int, optional(default=10)
                    sample size of 10 different duplicate scenarios
        
        Returns 
        -------
        Tuple(data, result)
            data    :   which contains all instances of the random sample 
            result  :   which contains the collapse_to_valuesonly result    

        Notes
        -----
        1. Sample Data can be saved to csv or xlsx and aggregated to check against result
        """ 
        idx = ['year', 'icode', 'importer', 'ecode', 'exporter', 'sitc4']
        #-Find Duplicate Lines by IDX-#
        dup = self.raw_data.duplicated(subset=idx)                                      
        #-Generate a Random Sample-#
        sample = random_sample(self.raw_data[dup], size)
        sample = sample.reset_index()
        
        #-Find All Rows Contained in the Sample from the Dataset to check Collapse-#
        data = pd.DataFrame()
        for i, row in sample[idx].iterrows():
            data = data.append(find_row(self.raw_data[idx], row))
        #-Full Table of Results-#
        data = self.raw_data.ix[data.index]
        
        #-Compute Result-#
        self.collapse_to_valuesonly()                                       #Note: This actually runs the collapse and produces self.dataset
        result = pd.DataFrame()
        for i, row in sample[idx].iterrows():
            result = result.append(find_row(self.dataset[idx], row))
        result = self.dataset.ix[result.index]

        return data, result

    def testdata_construct_dynamically_consistent_dataset(self, size=20):
        """
        Product Tests Data to check: construct_dynamically_consistent_dataset

        STATUS: NOT IMPLEMENTED

        """
        raise NotImplementedError


    #--------------#
    #-Case Studies-#
    #--------------#

    def case_study_useofsitc3_exporterhetero_code(self, code):
        """
        Composition Study on SITC Code [ie. code='057']

        Parameters
        ----------
        code    :   str
                    Specify an SITC ProductCode

        Notes
        -----
        1. Case Study ::

            This study products a table of 'exporter' compositional data. The Composition is the percent 
            of exports from that country attributed to each SITC4 digit code for a level 3 code such as '057'. 
            This table shows  significant cross-country heterogeneity in how countries "use" the coding system. 

            In the 1960's data for Australia coded '0570' makes up ~36% of the category. If a dataset is 
            constructed using only official codes then 36% of Australia's export in this family of products 
            would be dropped from the sample from the 1960's. 
            However in the case of Brazil, this line item '0570' is not used much at all (~0.3%) and it's exports 
            would be relatively unchanged. 

            Note: Cross Sections can still be studied with the high level of disaggregation. But for dynamic studies,
            these compositional affects will skew export lines showing export growth and decline in cases of compositional 
            shift from one to another coding system.

            Main Outcome
            ------------
            [1] For dynamic consistency between 1962 and 2000 the only real option is to aggregate families of goods to the 3-Digit Level
                This aggregation will also capture A and X adjustments and countries usuing the high level classification with records 
                found in '0' categories that aren't always found in the SITCR2 classification. 
            [2] For dynamic consistency from 1984 onwards, it is possible to use SITCR2 official codes. 

        """
        self.countries_only()
        df = self.intertemporal_productcodes_dataset(tabletype='composition', countries='exporter')
        df = df.reset_index()
        df['sitc3'] = df['sitc4'].apply(lambda x: str(x)[:3])
        df = df.set_index(['sitc3', 'exporter', 'sitc4'])
        df = df.ix[str(code)[0:3]]
        df.name = code
        return df












    # --------------------------------------- #
    # - Below is Temporary Work (Ideas etc) - #
    # --------------------------------------- #

    def compute_importer_value_percentofdataset(self):
        """ 
        Simple Compute of Importer Share in RAW DATA Dataset
        
        Note
        ----
        [1] These are not trade shares
        """
        total = self.raw_data['value'].sum()            #Note this includes WLD etc
        perc = self.raw_data.groupby(['importer'])['value'].sum() / total * 100
        return perc

    def compute_exporter_value_percentofdataset(self):
        """ 
        Simple Compute of Importer Share in RAW DATA Dataset
        
        Note
        ----
        [1] These are not trade shares
        """
        total = self.raw_data['value'].sum()            #Note this includes WLD etc
        perc = self.raw_data.groupby(['exporter'])['value'].sum() / total * 100
        return perc

    def return_nes_items(self, column):
        """ 
        Returns Note Elsewhere Specified (NES) Items through matching on a Regular Expression

        column  :   'importer' or 'exporter'

        """
        return self.raw_data[self.raw_data[column].isin(set([x for x in a.raw_data[column] if re.search(r'\bnes', x.lower())]))]


    def build_countrynameconcord_add_iso3ciso3n(self, force=False, verbose=False):
        """ 
        Builds a CountryName Concordance and adds in iso3c and iso3n codes to dataset
        
        STATUS: DEPRECATED
        
        Notes
        -----
        [1] Better to Match on ISO3N through countrycodes component
        """
        #-Build Country Name Concordance-#
        if verbose: print "[INFO] Building Country Name Concordance and adding iso3c and iso3n names"
        concord = self.countryname_concordance_using_cc(return_concord=True, force=force)
        #-Add ISO3C and ISO3N Data-#
        #-Importers-#
        self._dataset = self._dataset.merge(concord, left_on=['importer'], right_on=['countryname'])
        del self._dataset['countryname']
        self._dataset = self._dataset.rename_axis({'iso3c' : 'iiso3c', 'iso3n' : 'iiso3n'}, axis=1)
        #-Exporters-#
        self._dataset = self._dataset.merge(concord, left_on=['exporter'], right_on=['countryname'])
        del self._dataset['countryname']
        self._dataset = self._dataset.rename_axis({'iso3c' : 'eiso3c', 'iso3n' : 'eiso3n'}, axis=1)

    def load_data(years=[], keep_columns='all', verbose=None):
        """
        Load Data

        years           : specify years
        keep_columns    : specify columns in the dataset to keep

        Notes:
        ------
        [1] read_stata in pandas doesn't allow for importing columns
        [2] May want to re-write the __init__ portion to fetch data using this method
        [3] This still imports a LOT of necessary data!
        """
        data = pd.DataFrame()
        if years == []:
            years = self._available_years
        for year in years:
            fn = self._source_dir + self._fn_prefix + str(year)[-2:] + self._fn_postfix
            if verbose: print "Fetching 'countryname' Year: %s from file: %s" % (year, fn)
            if keep_columns != 'all':
                data = data.append(pd.read_stata(fn)[keep_columns])
            else:
                data = data.append(pd.read_stata(fn))
        return data

    def generate_countryname_concordance_files(self, verbose=False):
        """
        Generate a Global CountryName Concordance File for NBERWTF Dataset

        STATUS: **ON HOLD** 
                (Given this saves no time (and memory isn't binding) this function is currently ON HOLD)
                Feature Request made to pydata/pandas #7935

        Tasks
        -----
        [1] Load CountryName Only from dta files (Make a self.load_data(keep_columns=None) where keep_columns=['countryname'] in this method?)
        [2] Have to read all the data in pandas.read_stata() but much more memory efficient to discard the rest of the results for updating package files etc.
        [2] Write Concordance using pycountrycode to package data folder

        Output (DATA_PATH)
        ------------------
        nberfeenstrawtf(countryname)_to_iso3c.py, nberfeenstrawtf(countryname)_to_iso3n.py, 

        Notes:
        ------
        [1] Add to docstring "manually checked: False" as the results are determined by a regular expression
            Early Versions should be checked
        [2] The only advantage to this method is memory efficiency. It isn't any quicker. 
        """

        countrynames = set()
        
        #-Load 'countrynames' from ALL Available Years-#
        for year in self._available_years:
            fn = self._source_dir + self._fn_prefix + str(year)[-2:] + self._fn_postfix
            if verbose: print "Fetching 'countryname' Year: %s from file: %s" % (year, fn)
            data = pd.read_stata(fn)[['importer', 'exporter']]                                  #Note: This still reads in ALL of the .dta file! Feature Request made to pydata/pandas #7935
            importers = set(data['importer'])
            exporters = set(data['exporter'])
            countrynames = countrynames.union(importers).union(exporters)
            del data
        
        #- Concordance -#
        if verbose: print "Writing CountryName to ISO3C and ISO3N Files to %s" % DATA_PATH
        
        #-ISO3C-#
        iso3c = countryname_concordance(list(countrynames), concord_vars=('countryname', 'iso3c'))
        #-Write Py File-#
        iso3c.name = u"%s to %s" % ('countryname', 'iso3c')
        fl = "%s_%s_%s.py" % (self._name, 'countryname', 'iso3c')
        docstring   =   u"Concordance for %s to %s\n\n" % ('countryname', 'iso3c')  + \
                        u"%s" % self
        from_idxseries_to_pydict(iso3c, target_dir=DATA_PATH, fl=fl, docstring=docstring)
        
        #-ISO3C-#
        iso3n =  countryname_concordance(list(countrynames), concord_vars=('countryname', 'iso3n'))
        #-Write Py File-#
        iso3n.name = u"%s to %s" % ('countryname', 'iso3n')
        fl = "%s_(%s)_%s.py" % (self._name, 'countryname', 'iso3n')
        docstring   =   u"Concordance for %s to %s\n\n" % ('countryname', 'iso3n')  + \
                        u"%s" % self
        from_idxseries_to_pydict(iso3n, target_dir=DATA_PATH, fl=fl, docstring=docstring)


    def file_raw_data_to_hdf(self, verbose=True):
        """
        Move self.raw_data attribute to reference a h5 file on disk (to free up memory)
        
        Notes
        -----
        [1] Move to Generic Class of DatasetConstructors?
        [2] This isn't particularly Useful in this context, as attributes are derived from raw_data and it will 
            be automatically reloaded to memory. 
        """
        try:
            fn = self._source_dir + self.__raw_data_hdf_fn
            if verbose: print "[INFO] Attaching HDFStore to: %s" % fn
            self.__raw_data_hdf = pd.HDFStore(fn)                           #Check if file already exists
        except:
            if verbose: print "[INFO] raw data h5 file not found! Constructing one ..."
            self.convert_raw_data_to_hdf(verbose=verbose)               #self.__raw_data_hdf = hdf  
        if verbose: print   "[INFO] Deleting in-memory raw_data\n"                                   + \
                            "You can still access raw_data but it will need to be loaded into memory"
        del self.__raw_data