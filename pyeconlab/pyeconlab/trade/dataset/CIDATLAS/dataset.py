"""
Dataset Class for Atlas of Complexity Data

NOTE: ON HOLD (Focus on Generic CPEXPORTDATA)

"""

import pandas as pd

from .base import AtlasOfComplexity
from pyeconlab.trade.dataset import CPTradeDataset, CPTradeData, CPExportData, CPImportData

#------------#
#-Trade DAta-#
#------------#

class CIDAtlasTradeData(AtlasOfComplexity, CPTradeData):
    """
    Atlas of Complexity Trade Dataset

    File Interfaces
    ---------------
    "SITC"  : ["year", "origin", "destination", "sitc4", "export_val", "import_val"]
    "HS"    : ["year", "origin", "destination", "hs4", "export_val", "import_val"]
    """
    pass




#-------------#
#-Export Data-#
#-------------#

class CIDAtlasExportData(AtlasOfComplexity, CPExportData):
    """
    Atlas of Complexity Export Dataset
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

#-------------#
#-Import Data-#
#-------------#

class CIDAtlasImportData(AtlasOfComplexity, CPImportData):
    """
    Atlas of Complexity Import Data
    """
    pass