"""
Subpackage: pyeconlab.trade.dataset
===================================

Purpose: 
--------    
1. Construct Datasets from RAW Data Sources
2. Clean Datasets 
3. Create Dataset Objects

Organization
------------

This subpackage is built using the following structure. 
Supporing base classes are contained at the top level

dataset_c.py:

    This module provides a generic country level dataset object.
    This consists of three types: Trade, Export, and Import based on the type of trade flows. 

dataset_cp.py:

    This module provides a generic country-product level dataset object.
    This consists of three types: Trade, Export, and Import based on the type of trade flows.

Specific datasets are built using a subpackage - named after the source institution:

    1. CEPIIBACI
    2. NBERWTF

These subpackages follow a standard arrangement using constructors, and dataset classes.

"""

#-Generic Dataset Objects-#
from .dataset_c import CTradeDataset, CTradeData, CExportData, CImportData
from .dataset_cp import CPTradeDataset, CPTradeData, CPExportData, CPImportData

#-NBER Feenstra World Trade Flows-#
from .NBERWTF.constructor import NBERWTFConstructor
from .NBERWTF.dataset import NBERWTF, NBERWTFTradeData, NBERWTFExportData, NBERWTFImportData

#-BACI World Trade Flows-#
from .CEPIIBACI.base import BACI
from .CEPIIBACI.constructor import BACIConstructor
from .CEPIIBACI.dataset import BACITradeData, BACIExportData, BACIImportData

#-CID Atlas of Complexity-#
from .CIDATLAS.base import AtlasOfComplexity
from .CIDATLAS.dataset import CIDAtlasTradeData, CIDAtlasExportData, CIDAtlasImportData