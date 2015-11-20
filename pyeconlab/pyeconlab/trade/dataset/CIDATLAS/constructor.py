"""
Atlas of Complexity Dataset Constructor
=======================================

Constructor Class for Atlas of Complexity Dataset

"""

import os
import gc
import pandas as pd
import warnings
import shutil

from .base import AtlasOfComplexity
from .dataset import CIDAtlasTradeData, CIDAtlasExportData, CIDAtlasImportData
from pyeconlab.util import check_directory, check_operations, update_operations, from_idxseries_to_pydict, concord_data


class CIDAtlasDataConstructor(AtlasOfComplexity):
    """
    Constructor for Atlas of Complexity Data (CID)
    """

    def __init__(self, source_dir, trade_classification, dtype, years=[], ftype='hdf', reduce_memory=False, standardize_dataset=False, reset_cache=False, verbose=True):
        """
        Constructor for the CID Atlas of Economic Complexity Data
        
        ..  Inheritance
            -----------
            1. AtlasOfComplexity -> Provides Meta Data on AtlasOfComplexity Dataset
        
        Parameters
        ----------
        source_dir              :   str
                                    Specify source directory containing raw tsv files
        trade_classification    :   str
                                    Type of Source Files to Load ["SITCR2", "HS92"]
        dtype                   :   str
                                    Specify Data Type to work with ["trade", "export", "import"]
        years                   :   list, optional(default=[])
                                    Apply a Year Filter [Default: All Years Available in the Data]
        skip_setup              :   bool, optional(default=True)
                                    [Testing] This allows you to skip __init__ setup of object to manually load the object with csv data etc. 
                                    This is mainly used for loading test data to check attributes and methods etc. 
        reduce_memory           :   bool, optional(default=False)
                                    This will delete self.__raw_data after initializing self.dataset with the raw_data
                                    [Warning: This will render properties that depend on self.__raw_data inoperable]
                                    Useful when building datasets to be more memory efficient as the operations don't require a record of the original raw_data
        standardize_dataset     :   bool, optional(default=False)
                                    Standardize dataset into Trade, Export, Import Values Only from RAW Files.

        """
        #-Setup Attributes-#
        self.name = "Atlas Of Complexity (CID) Dataset"
        self.dtype = dtype.lower()
        if self.dtype not in self.source_dtypes:
            raise ValueError("%s is not a valid data type [Valid: %s]" % (self.dtype, self.source_dtypes))
        self.classification = trade_classification
        if self.classification not in self.source_classifications:
            raise ValueError("%s is not a valid classification [Valid: %s]" % (self.classification, self.source_source_classifications))
        self.revision = trade_classification[-2:]
        self.level = self.source_level
        self.notes = ""
        self.operations = ""
        self.complete_dataset = False
        
        #-Parse Years-#
        if verbose: print "[INFO] Fetching CIDAtlas Data from %s" % source_dir
        if years == []:
            self.complete_dataset = True                        
            years = self.source_years[self.classification]    
        #-Assign to Attribute-#
        self.years = years 
        #-Files-#
        self.__source_dir = check_directory(source_dir)
        self.__cache_dir = "cache/"
        #-Load Data-#
        if ftype=="tsv" or reset_cache:
            self.load_raw_from_tsv(reset_cache=reset_cache)
        else:
            if not os.path.exists(self.__source_dir + self.__cache_dir):
                self.load_raw_from_tsv()
            else:
                self.load_raw_from_hdf()
        #-Reduce Memory-#
        if reduce_memory:
            self.dataset = self.__raw_data
            self.__raw_data = None
        else:
            self.dataset = self.__raw_data.copy(deep=True)                
        
        #-Standardize-#
        if standardize_dataset:
            self.construct_standardized_dataset()

    @property
    def raw_data(self):
        return self.__raw_data
    

    def load_raw_from_tsv(self, reset_cache=False, verbose=True):
        """ Load Raw Data from TSV """
        #-Data Type-#
        if self.dtype == "trade":
            fl = self.__source_dir+self.source_trade_datafl[self.classification]
        elif self.dtype == "export" or self.dtype == "import":
            fl = self.__source_dir+self.source_exportimport_datafl[self.classification]
        #-Classification-#
        if self.classification == "SITCR2":
            self.productcode = "sitc4"
        elif self.classification == "HS92":
            self.productcode = "hs4"
        #-Load Data-#
        if verbose: print "[INFO] Loading Raw Data From %s ..." % (fl)
        self.__raw_data = pd.read_table(fl, dtype={self.productcode : str})             #Could Use chunksize to be more memory efficient
        #-Parse Years-#
        if not self.complete_dataset:
            warnings.warn("This method must import all data ... best to use hdf cache file for specific years")
            self.__raw_data = self.__raw_data.loc[self.__raw_data['year'].isin(self.years)]
            self.__raw_data = self.__raw_data.reset_index()
            del self.__raw_data["index"]
            gc.collect()
        #-Construct a Cache Folder-#
        if not os.path.exists(self.__source_dir + self.__cache_dir):
            print "[INFO] Setting up a Cache Directory ..."
            os.makedirs(self.__source_dir + self.__cache_dir)
            reset_cache = True
        #-Cache-#
        hdf_fn = self.__source_dir + self.__cache_dir + "cidatlas_%s_%s_year.h5" % (self.classification, self.dtype)
        if reset_cache or os.path.exists(hdf_fn) == False:
            if verbose: print "[INFO] Writing raw_data to %s" % hdf_fn
            #-Construct HDF File-#
            hdf = pd.HDFStore(hdf_fn, complevel=9, complib='zlib')
            for year in self.years: 
                if verbose: print "[INFO] Processing year %s ..." % year
                data = self.__raw_data.loc[self.__raw_data['year'] == year].reset_index()
                del data["index"]
                hdf.put('Y'+str(year), data, format='table')    
            if verbose: print hdf
            hdf.close()
        gc.collect()

    def load_raw_from_hdf(self, verbose=True):
        """ Load Raw Data from HDF Cache """
        #-Data Type-#
        hdf_fn = self.__source_dir + self.__cache_dir + "cidatlas_%s_%s_year.h5" % (self.classification, self.dtype) 
        if not os.path.exists(hdf_fn):
            self.load_raw_from_tsv(verbose=verbose)
        #-Data-#
        self.__raw_data = pd.DataFrame()  
        for year in self.years:
            if verbose: print "[INFO] Loading RAW DATA for year: %s ..." % (year)
            self.__raw_data = self.__raw_data.append(pd.read_hdf(hdf_fn, key='Y'+str(year)))

    def load_country_data(self, verbose=True):
        """ Load Country Meta Data File """
        self.country_data = pd.read_table(self.__source_dir + self.source_country_datafl)
        self.country_data.rename(columns={'id' : 'iso3c', 'name' : 'countryname'}, inplace=True)
        self.country_data["iso3c"] = self.country_data["iso3c"].apply(lambda x: x.upper())

    def load_product_data(self, verbose=True):
        """ Load Product Meta Data File """
        self.product_data = pd.read_table(self.__source_dir + self.source_product_datafl[self.classification], dtype={'id' : str})
        self.product_data.rename(columns={'id':'productcode', 'name' : 'productname'}, inplace=True)

    #-Datasets-#

    def construct_standardized_dataset(self, verbose=True):
        """ Construct a Standardized Dataset """
        if verbose: print "[INFO] Running .construct_standardized_dataset()"
        #-Reshape Data Contents and Fix Names-#
        if self.dtype == "trade":
            olength = self.dataset.shape[0]
            #-Export Data-#
            export_cols = self.dataset.columns.drop("import_val")
            export_recodes = {
                "origin" : "eiso3c",
                "destination" : "iiso3c",
                "export_val"  : "value",
            }
            exports = self.dataset[export_cols]
            exports.rename(columns=export_recodes, inplace=True)
            #-Import Data-#
            import_cols = self.dataset.columns.drop("export_val")
            import_recodes = {
                "origin" : "iiso3c",
                "destination" : "eiso3c",
                "import_val" : "value",
            }
            imports = self.dataset[import_cols]
            imports.rename(columns=import_recodes, inplace=True)
            del self.dataset
            gc.collect()
            #-Combine-#
            if self.classification == "HS92":
                productcode = u'hs4'
            elif self.classification == "SITCR2":
                productcode = u'sitc4'
            order = [u'year', u'eiso3c', u'iiso3c', productcode, u'value']
            self.dataset = exports.append(imports)[order]
            del exports
            del imports
            gc.collect()
            self.dataset = self.dataset.reset_index()
            del self.dataset["index"]
            #-Checks-#
            assert 2*olength == self.dataset.shape[0]
            gc.collect()
        elif self.dtype == "export":
            olength = self.dataset.shape[0]
            export_cols = self.dataset.columns.drop(["import_val", "import_rca"])
            export_recodes = {
                "origin" : "eiso3c",
                "export_val"  : "value",
                "export_rca"  : "rca",
            }
            exports = self.dataset[export_cols].copy()
            exports.rename(columns=export_recodes, inplace=True)
            self.dataset = exports
            #-Checks-#
            assert olength == self.dataset.shape[0]          
        elif self.dtype == "import":
            olength = self.dataset.shape[0]
            import_cols = self.dataset.columns.drop(["export_val", "export_rca"])
            import_recodes = {
                "origin" : "iiso3c",
                "import_val" : "value",
                "import_rca" : "rca",
            }
            imports = self.dataset[import_cols].copy()
            imports.rename(columns=import_recodes, inplace=True)
            self.dataset = imports
            #-Checks-#
            assert olength == self.dataset.shape[0]
        #-Fix ISO3C Codes-#
        if self.dtype == "export" or self.dtype == "trade":
            self.dataset["eiso3c"] = self.dataset["eiso3c"].apply(lambda x: x.upper())
            gc.collect()
        if self.dtype == "import" or self.dtype == "trade":
            self.dataset["iiso3c"] = self.dataset["iiso3c"].apply(lambda x: x.upper())
            gc.collect()

    # def construct_rca_dataset(self, verbose=True):
    #     """ Construct RCA Datasets """
    #     if verbose: print "[INFO] Running .construct_rca_dataset() ... "
    #     if self.dtype == "trade":
    #         warnings.warn("There is no RCA data in the source trade data file!")
    #         return None
    #     if self.dtype == "export":



    def countries_only(self, verbose=True):
        """ Return a Dataset that only Contains Countries """ 
        from .meta import iso3c_notcountries
        #-Adjust Dataset-#
        if self.dtype == "export" or self.dtype == "trade":
            country_list = set(self.dataset.eiso3c.unique())
            drop_list = country_list.intersection(set(iso3c_notcountries))
            keep_list = country_list.difference(set(iso3c_notcountries))
            if verbose: print "[INFO] ... dropping eiso3c codes %s" % drop_list
            self.dataset = self.dataset.loc[self.dataset["eiso3c"].isin(keep_list)]
            gc.collect()
        if self.dtype == "import" or self.dtype == "trade":
            country_list = set(self.dataset.iiso3c.unique())
            drop_list = country_list.intersection(set(iso3c_notcountries))
            keep_list = country_list.difference(set(iso3c_notcountries))
            if verbose: print "[INFO] ... dropping iiso3c codes %s" % drop_list
            self.dataset = self.dataset.loc[self.dataset["iiso3c"].isin(keep_list)]
            gc.collect()

    # def to_level(self, level, verbose=True):
    #     """ Drop Dataset to a Specified Level"""
    #     if level >= self.level:
    #         raise ValueError("Level (%s) is the same or higher for data already in the dataset! Must be less than %s" (level, self.level))
    #     if self.classification == "SITCR2":
    #         self.dataset["sitc%s"%level] = self.dataset["sitc%s"%self.level].apply(lambda x: x[0:level])
    #         self.dataset = self.dataset.groupby()
    #     elif self.classification == "HS92":

    #     else:
    #         raise ValueError("Classification is SITCR2 or HS92")


    #-To Methods-#

    def attach_attributes_to_dataset(self, df):
        """ 
        Attach Attributes to the Dataset DataFrame for Transfer when building new objects
        """
        #-Attach Attributes to dataset object for transfer-#
        df.txf_name             = self.name
        df.txf_data_type        = self.dtype
        df.txf_classification   = self.classification
        df.txf_revision         = self.revision
        df.txf_complete_dataset = self.complete_dataset
        df.txf_notes            = self.notes
        df.txf_source_revision  = self.classification       #No Conversion Methods in this Constructor
        df.txf_units_value_str  = self.units_value_str
        return df

    def to_dataset(self, value="value", generic=False):
        """
        Convert to a Dataset Object
        """
        #-Prepare Data for Object Standard Input-#
        data = self.dataset
        if self.classification == "HS92":
            productcode = "hs4"
        elif self.classification == "SITCR2":
            productcode = "sitc4"
        data = data.rename_axis({productcode : 'productcode'}, axis=1)
        if self.dtype == "trade":
            cols = ["year", "eiso3c", "iiso3c", productcode, value]
            data = data[cols]
            data = self.attach_attributes_to_dataset(data)   
            if generic:
                return CPTradeData(data)
            return CIDAtlasTradeData(data)
        elif self.dtype == "export":
            cols = ["year", "eiso3c", 'productcode', value]
            data = data[cols]
            data = self.attach_attributes_to_dataset(data)   
            if generic:
                return CPExportData(data)
            return CIDAtlasExportData(data)
        elif self.dtype == "import":
            cols = ["year", "iiso3c", 'productcode', value]
            data = data[cols]
            data = self.attach_attributes_to_dataset(data)   
            if generic:
                return CPImportData(data)
            return CIDAtlasImportData(data)
        else:
            raise ValueError("data_type (%s) is not 'trade', 'export' or 'import'" % self.data_type)  