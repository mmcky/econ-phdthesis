"""
Trade Package
=============

Sub-Packages:
-------------
1. classification: 	

	Contains SITC, HS Classification Data Objects (i.e. SITC to HS Mappings)

2. concordances: 

	Contains Concordance Data for Trade Analysis (i.e. CountryName -> ISO3C Mappings etc)

3. data: 

	Data Constructors & Compilers

4. test:
	
	Provides Test Suites

Trade System Classes
--------------------
ProductLevelExportSystem.py 	[Object Structure Based on Pandas DataFrames (Long x Wide)]
ProductLevelExportNetwork.py 	[Object Structure Based on Networkx]

Notes
-----
1. Dataset Subpackage does not need to be imported at this level as it's included as a package in setup.py
2. This package does NOT contain actual TradeData and those files must be specified by user at this time. 

Future Work for Package
-----------------------
1. productspace =>	General Functions Suited to ProductSpace Research [Some of this is integrated into Systems, Networks]
2. Work on Exanding support from just export systems to the full array of network models used in trade

"""							

#-Datasets-#
from .dataset import 	CTradeDataset, CTradeData, CExportData, CImportData, 	\
						CPTradeDataset, CPTradeData, CPExportData, CPImportData

#-Specific Datasets-#
from .dataset.CEPIIBACI import BACIConstructor, BACITradeData, BACIExportData, BACIImportData
from .dataset.NBERWTF import NBERWTFConstructor, NBERWTFTradeData, NBERWTFExportData, NBERWTFImportData
from .dataset.CIDATLAS import CIDAtlasDataConstructor, CIDAtlasTradeData, CIDAtlasExportData, CIDAtlasImportData

# - Systems - #
from .systems.ProductLevelExportSystem import ProductLevelExportSystem
from .systems.DynamicProductLevelExportSystem import DynamicProductLevelExportSystem
# from .ProductLevelExportNetwork import ProductLevelExportNetwork