"""
Generic Dataset Constructor Class
=================================

STATUS: ON-HOLD (Most Constructors are fairly specialised so this may not be wise or useful)

ALL Data Constructors Requires the separate Download of Source Data and the Specification of a Source Data Directory
Note: Every effort will be made to keep the interface up to date if any dataset format or filenames change

This Class will load the RAW Data from RAW Source Files from Various Formats, and undertake Standardization to Produce Harmonized Key Variables

The main usefulness of Constructor Classes is to simply remove compilation code from the Core Data Objects as this process only occurs infrequently

Harmonisation:
-------------
	1. 	country 	: 	iso3c 	(str)
	2. 	year 		: 	yyyy 	(int)
	3.  productcode : 	SITC or HS (str) 	Note: Str is used to prevent issues with leading 0's etc.

Datasets:
--------
	1. Feenstra/NBER Data 						[Last Checked: 03/07/2014]
	2. BACI Data
	3. CEPII Data
	4. UNCTAD Revealed Capital,Labour, and Land
	....

Issues:
-------
	1. 	How best to Incorporate the Source Dataset files. They can be very large 
		[Currently will pull in from a MyDatasets Object]
	2. 	This will be pretty slow to derive data each time from the raw data and will drive unnecessary wait times
		Current Direction: Separate Constructor Classes to Dataset Classes and pickle

Future Work:
-----------
	1. 	How to Handle Custom Altered Made Datasets such as Intertemporally Consistent NBER-BACI Data?
		Current Direction: Focus on making RAW Data Easily Acceptable such that alterations can be made by a Project without the initial setup
	2. 	Integrate simple md5sum Dataset Management from MyDatasets Project
"""

import pandas as pd
import numpy as np

# - Dataset Object - #
from dataset import NBERFeenstraWTF

# - Data Constructors - #

class GenericConstructor(object):
	'''
		Not Currently Required as will Integrate with ProductLevelExportSystem etc. (.from_csv())
	'''
	pass


######## - IN WORK - ##########

# - Place these in their appropriate SubPackage - #

class BACI(object):
	pass

class CEPII(object):
	pass

