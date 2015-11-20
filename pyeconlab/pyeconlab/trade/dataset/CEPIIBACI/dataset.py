"""
CEPII/BACI Dataset Objects
==========================

Supporting Constructor: BACIConstructor

Dependancies
------------
1. pyeconlab.trade => CPTradeDataset, CPTradeData, CPExportData, CPImportData
2. pyeconlab.trade.dataset.CEPIIBACI.base => BACI 

Product Classification
----------------------
1. HS92 Years: 1995-2011
2. HS96 Years: 1998-2011
3. HS02 Years: 2003-2011

Types
-----
1. Trade Data   (Bilateral Trade Flows)
2. Export Data  (Export Trade Flows)
3. Import Data  (Import Trade Flows)
"""

import numpy as np
import pandas as pd
import cPickle as pickle

#-Generic Containers-#
from .base import BACI
from pyeconlab.trade.dataset import CPTradeDataset, CPTradeData, CPExportData, CPImportData

#-These are Just Wrappers to Put a more appropriate Class Name Around Generic Dataset Objects and to provide source details -#
#-May want to hide some source details as they clutter the . namespace                                                      -#

#-Trade-#

class BACITradeData(BACI, CPTradeData):
    """ 
    BACI TRADE Dataset Wrapper

    See Also
    --------
    pyeconlab.trade.dataset.CEPIIBACI.base
    pyeconlab.trade.dataset.CPTradeData
    
    """
    __data = pd.DataFrame()

    #-Class Properties-#

    @property 
    def data(self):
        return self.__data
    # @data.setter
    # def data(self, values):
    #   self.__data = values

    def set_data(self, value, force=False):
        """ Force Assign New Dataset """
        if force:
            self.__data = value
        else:
            raise ValueError("'force' must be manually set using the force flag")

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

#-Exports-#

class BACIExportData(BACI, CPExportData):
    """ 
    BACI Export Dataset

    See Also
    --------
    pyeconlab.trade.dataset.CEPIIBACI.base
    pyeconlab.trade.dataset.CPTradeData

    """
    __data = pd.DataFrame()

    @property 
    def data(self):
        return self.__data
    # @data.setter
    # def data(self, values):
    #   self.__data = values

    def set_data(self, value, force=False):
        """ Force Assign New Dataset """
        if force:
            self.__data = value
        else:
            raise ValueError("'force' must be manually set using the force flag")

#-Imports-#

class BACIImportData(BACI, CPImportData):
    """ 
    BACI Import Dataset

    See Also
    --------
    pyeconlab.trade.dataset.CEPIIBACI.base
    pyeconlab.trade.dataset.CPTradeData
    
    """
    __data = pd.DataFrame()

    @property 
    def data(self):
        return self.__data
    # @data.setter
    # def data(self, values):
    #   self.__data = values

    def set_data(self, value, force=False):
        """ Force Assign New Dataset """
        if force:
            self.__data = value
        else:
            raise ValueError("'force' must be manually set using the force flag")