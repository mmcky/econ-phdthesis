"""
Generic Country Dataset Construction Classes
============================================

Types
-----
1. Trade Dataset    (Bilateral Trade Flows)
2. Export Dataset   (Export Trade Flows)
3. Import Dataset   (Import Trade Flows)

Notes
-----
1. These objects get inherited by specific datasets. Any child dataset object (i.e. NBERFeenstraWTF) needs to have local class level properties for 
    self.__data, and any other relevant locally stored attributes (self.__exports, self.__imports etc.). This avoids the need to use super etc. when 
    assigning and accessing data

Trade Datasets
--------------
1. Feenstra/NBER Data                           [NBERFeenstraWTF]
2. BACI Data                                    [TBD]
3. CEPII Data                                   [TBD]


Future Work
-----------
1. Implement an interface for Quantity Data ['year', 'exporteriso3c', 'importeriso3c', 'value', 'quantity']
2. Impliment Geographic Aggregates
3. Should I go one level up for to_pickle(), to_hdf() as just dataset objects? The inheritance is getting a bit confusing. 
    [For now -> Duplicate to_pickle functionality at the base level of each type of dataset (i.e. CTradeDataset, CPTradeDataset)]
    [This is because from_pickle differs based on C and CP Data as to what attributes are loaded therefore is overwritten anyway]
"""

import pandas as pd
import cPickle as pickle
import warnings

#-Country Trade Dataset-#

class CTradeDataset(object):
    """
    Generic Country Level Trade Dataset Object
    -------------------------------------------

    This Object Impliments a Standard Interface for Incoming Data allowing methods to be writen easily
    This handles data entry to the object and attributes that belong to all types of Data

    Parameters
    ----------
    data        :   str, pd.DataFrame
                    Provide Data to populate the class (DataFrame or Pickle)

    ..  Future Work
        -----------
        [1] from_hdf
    """

    # - Attributes - #
    name            = None
    years           = None
    available_years = None
    source_web      = None
    raw_units       = None
    raw_units_str   = None
    interface       = {
                         'trade'  : ['year', 'eiso3c', 'iiso3c', 'value'], 
                         'export' : ['year', 'eiso3c', 'value'],
                         'import' : ['year', 'iiso3c', 'value'],
                      }

    def __init__(self, data):   
        if type(data) == pd.DataFrame:
            self.from_dataframe(data)
        elif type(data) == str:
            fn, ftype = data.split('.')
            if ftype == 'pickle':
                self.from_pickle(fn=data)
            elif ftype == 'h5':
                self.from_hdf(fn=data)
            else:
                raise ValueError('Uknown File Type: %s' % ftype)

    @property 
    def num_years(self):
        """ 
        Number of Years
        """
        loc = self.data.index.names.index('year')
        return self.data.index.levshape[loc]

    #-IO-#

    def load_dataframe(self, df, dtype):
        """
        Populate Object from Pandas DataFrame
        """
        #-Force Interface Variables-#
        if type(df) == pd.DataFrame:
            # - Check Incoming Data Conforms - #
            columns = set(df.columns)
            for item in self.interface[dtype]:
                if item not in columns: 
                    raise TypeError("Need %s to be specified in the incoming data" % item)
            #-Set Attributes-#
            self.data = df.set_index(self.interface[dtype][:-1])    #Index by all values except 'value'
            #-Infer Years-#
            self.years = list(df['year'].unique())
        else:
            raise TypeError("data must be a dataframe that contains the following interface columns:\n\t%s" % self.interface[dtype])

    def to_pickle(self, fn):
        """ 
        Save Data as a Pickle Object
        """
        with open(fn, 'w') as f:
            pickle.dump(self, f)
        f.close()

    def from_pickle(self, fn):
        """ 
        Load Object from Pickle
        
        Notes
        -----
        1. Load Object from Pickle and assign current object with data. All non-derived items should be transfered.  
        """
        fl = open(fn, 'r')
        obj = pickle.load(fl)
        if type(obj) != self.__class__:
            raise ValueError("Pickle Object doesn't contain a %s object!\nIt's type is: %s" % (str(self.__class__).split('.')[-1].split("'")[0], str(obj.__class__).split('.')[-1].split("'")[0]))
        #-Populate Object-#
        self.data = obj.data
        self.years = obj.years

    def to_hdf(self, fn):
        """
        Populate Object from HDF File
        """
        raise NotImplementedError

    def from_hdf(self, fn):
        """
        Populate Object from HDF File
        """
        raise NotImplementedError

    #-Methods-#

    def check_interface(self, columns, dtype):
        """ 
        Checking Incoming Data Conforms to Interface
        
        Parameters
        ----------
        columns     :   pd.Index, List, Set
                        Provide Columns to Check Data Interface
        dtype       :   str('trade', 'export', 'import')
                        Determine Type of Data

        Raises
        ------
        TypeError
            If column isn't specified in the incoming data

        """
        columns = set(columns)
        for item in self.interface[dtype]:
            if item not in columns: 
                raise TypeError("Need %s to be specified in the incoming data" % item)

    #-Country / Aggregates Filters-#

    def geo_aggregates(self, members):
        """
        Geographic Aggregates

        STATUS: IN-WORK

        Note: Given this methods location, it will have to try eiso3c and iiso3c as this is inherited by both Trade, Import and Export Datasets

        members = dict {'iso3c' : 'region'}
        Subsitute Country Names for Regions and Collapse.sum()
        """
        raise NotImplementedError

#-Country Level Trade Data-#

class CTradeData(CTradeDataset):
    """ 
    Generic Country **Trade** Data Object
    
    Interface: ['year', 'eiso3c', 'iiso3c', 'value']

    Future Work:
    -----------
    [1] Implement an interface for Quantity Data ['year', 'exporteriso3c', 'importeriso3c', 'value', 'quantity'], 
    """

    def __repr__(self):
        string = "Class: %s\n" % (self.__class__)                           + \
                 "Years: %s\n" % (self.years) +  " [Available Years: %s]\n"     % (self.available_years)        + \
                 "Number of Importers: %s\n" % (self.num_importers)         + \
                 "Number of Exporters: %s\n" % (self.num_exporters)         + \
                 "Number of Trade Flows: %s\n" % (self.data.shape[0])
        return string

    #-Properties-#

    @property 
    def data(self):
        return self.__data
    @data.setter
    def data(self, values):
        self.__data = values

    @property 
    def exports(self):
        try:
            return self.__exports
        except:
            self.export_data()
            return self.__exports
    @exports.setter
    def exports(self, values):
        self.__exports = values

    @property 
    def imports(self):
        try:
            return self.__imports
        except:
            self.import_data()
            return self.__imports
    @imports.setter
    def imports(self, values):
        self.__imports = values

    @property 
    def num_exporters(self):
        """
        Number of Exporters
        """
        try:
            loc = self.data.index.names.index('eiso3c')
        except:
            warnings.warn("'eiso3c' is not found in the data", UserWarning)
        return self.data.index.levshape[loc]

    @property 
    def num_importers(self):
        """ 
        Number of Importers
        """
        try:
            loc = self.data.index.names.index('iiso3c')
        except:
            warnings.warn("'iiso3c' is not found in the data", UserWarning)
        return self.data.index.levshape[loc]

    #-Data Import Methods-#

    def from_dataframe(self, df):
        self.load_dataframe(df, dtype='trade')

    #-Exports / Imports Data-#

    def export_data(self, return_data=False):
        """
        Collapse to obtain Export Data
        """
        warnings.warn("This method aggregates across iiso3c for every eiso3c. This most likely will not include NES regions if they have been discarded in the constructor (as they do not belong to any given importer)", UserWarning)
        self.exports = self.data.reset_index()[['year', 'eiso3c', 'productcode', 'value']].groupby(['year', 'eiso3c', 'productcode']).sum()
        if return_data:
            return self.exports

    def import_data(self, return_data=False):
        """
        Collapse to obtain Import Data
        """
        warnings.warn("This method aggregates across iiso3c for every eiso3c. This most likely will not include NES regions if they have been discarded in the constructor (as they do not belong to any given importer)", UserWarning)
        self.imports = self.data.reset_index()[['year', 'iiso3c', 'productcode', 'value']].groupby(['year', 'iiso3c', 'productcode']).sum()
        if return_data:
            return self.imports

#-Country Export Data-#

class CExportData(CTradeDataset):
    """ 
    Generic Export Dataset Object

    Interface: ['year', 'eiso3c', 'productcode', 'value']

    """
    def __repr__(self):
        """ Representation String Of Object """
        string = "Class: %s\n" % (self.__class__)                           + \
                 "Years: %s\n" % (self.years) +  " [Available Years: %s]\n" % (self.available_years)        + \
                 "Number of Exporters: %s\n" % (self.num_exporters)         + \
                 "Number of Export Flows: %s\n" % (self.data.shape[0])
        return string

    #-Data Import Methods-#

    def from_dataframe(self, df):
        self.load_dataframe(df, dtype='export')

    #-Properties-#

    @property 
    def data(self):
        return self.__data
    @data.setter
    def data(self, values):
        self.__data = values

    @property 
    def exports(self):
        return self.__data

    @property 
    def num_exporters(self):
        """ Number of Exporters """
        try:
            loc = self.data.index.names.index('eiso3c')
        except:
            warnings.warn("'eiso3c' is not found in the data", UserWarning)
        return self.data.index.levshape[loc]

#-Country Import Data-#

class CImportData(CTradeDataset):
    """ 
    Generic Country Import Dataset Object

    Interface: ['year', 'iiso3c', 'value']

    """ 
    
    def __repr__(self):
        string = "Class: %s\n" % (self.__class__)                           + \
                 "Years: %s\n" % (self.years) +  " [Available Years: %s]\n" % (self.available_years)        + \
                 "Number of Importers: %s\n" % (self.num_importers)         + \
                 "Number of Import Flows: %s\n" % (self.data.shape[0])
        return string

    #-Properties-#
    
    @property 
    def data(self):
        return self.__data
    @data.setter
    def data(self, values):
        self.__data = values

    @property 
    def imports(self):
        return self.__data

    @property 
    def num_importers(self):
        """ Number of Importers """
        try:
            loc = self.data.index.names.index('iiso3c')
        except:
            warnings.warn("'iiso3c' is not found in the data", UserWarning)
        return self.data.index.levshape[loc]

    #-Data Import Methods-#

    def from_dataframe(self, df):
        self.load_dataframe(df, dtype='import')
