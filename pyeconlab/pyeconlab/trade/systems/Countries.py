'''
	[THiS NEEDS UPDATING]

	Project: 	TradeSystem
	Class: 		Countries
	Author: 	Matthew McKay <mamckay@gmail.com>

	Purpose: 
	--------
		[1] Country() can serve as Node Objects in TradeSystem Networks
		[2] Countries() holds a catalogue of pre-computed country objects for network formation
 	AND
 	  	[3] Reconstruct Country Data Pickles

	Example:
	--------
		A = Countries()
		A['AUS'] 	: 	Country() 		# Instance of Country Australia
		
		# - Core Attributes - #
		A.name 		: 	str()
		A.iso3c		: 	str()
		A.iso3n		: 	str()
		
		# - Optional Attributes - #
		A.gdp 		: 	pd.Series 		
		A.gdppc		: 	pd.Series
		A.wdi 		: 	Dict('wdi-code' : pd.Series) 		#Dictionay of WDI data for Australia
'''

### --- Standard Library Imports --- ###

import os
import sys
import cPickle as pickle
import re
import pprint
import pandas as pd
import itertools as it

import pycountry as pc 								#May need to pip install pycountry as not included in Conda. ISO country data

### --- Add Custom Local Libraries --- ###

# library_dir = "work/lib_python/"

# if sys.platform.startswith('win'):
#     #Tuned to Win7 Platform References#
#     abs_path = os.path.expanduser("~")             #Note this method won't work if library is on a different drive letter!#
#     abs_path = abs_path + "\\" + library_dir  
# elif sys.platform.startswith('darwin'):             #OS X - NEEDS TESTING
#     abs_path = os.path.expanduser("~")
#     abs_path = abs_path + "/" + library_dir
# elif sys.platform.startswith('linux'):
#     abs_path = os.path.expanduser("~")
#     abs_path = abs_path + "/" + library_dir

# projects = ['mydatasets'] 							#'wdi_lab' superseeded by local WDI.py
# for project in projects:
#     sys.path.append(abs_path+"/"+project)

# Superseded by Local WDI Library: WDI.py
# try:
#     import wdi_lab as wdi
# except:
#     print "[wdi_lab] The WDI_LAB Repository cannot be found!"
#     sys.exit()

# try: 
# 	import mydatasets as md
# except:
# 	print "[mydatasets] The MyDatasets Repository cannot be found!"
# 	sys.exit()


### --- Add Local Folder Libraries --- ###
import pyeconlab.wdi as wdi

### --- Country Class --- ###

class Country():
	'''
		Country Object for Network Node or Edge Data
		
		Dependancies:
			[1] pycountry as pc
	'''

	## -- Setup Routines -- ##

	def __init__(self, ccd, ccd_type='iso3c'):
		
		self.iso_country = True
		## -- Parse Input -- ##
		if ccd_type == 'iso3c':
			self.iso3c = ccd 
			try: 
				self.cntry = pc.countries.get(alpha3=ccd)  							#Current Countries
				self.historic = False
			except:
				try:
					self.cntry = pc.historic_countries.get(alpha3=ccd)  		 	#Historic Countries
					self.historic = True
				except:
					self.cntry = ccd 												#Non ISO Countries
					self.iso_country = False
		else:
			raise ValueError("ISO3C is the only ccd type implimented!")

		## -- Core Items -- ## 							#These should be immutable from external calls
		if self.iso_country:													
			self.name = self.cntry.name
			self.iso3n = self.cntry.numeric
		else:
			self.name = None
			self.iso3n = None
		## -- Convenient Data Containers -- ##
		self.gdp = None
		self.gdp_desc = None 				#To check type of GDP Figure
		self.gdppc = None
		self.gdppc_desc = None
		self.wdi = dict()
		## -- Pre-Computable Items -- ##
		self.neighbours = None 				#List of Geographic Neighbours
		self.trading_partners = None		#List of Trading Partners
		## -- Other Attributes -- ##
		self.attr = dict()

	## -- Class Python Functions -- ##

	def __repr__(self):
		return str(self.iso3c) 				#Default Representation is iso3c code

	def __lt__(self, other): 				#Default Sortedness is by Alphanumeric ISO3C Code
		return self.iso3c < other.iso3c

	## -- IO -- ##

	def build_from_wdi(self, wdiobj, series_codes=None, verbose=False):
		'''
			Fill Country Object with WDI data from a WDI object
		'''
		## -- Check wdi is a WDI object -- ##
			## WORK HERE ##
		if not isinstance(wdiobj, wdi.WDI): 									# Not setting self.wdi = False as the Country Object may already be filled
			if verbose: print "Incoming wdi is not a WDI object!"
			return False
		self.wdi = wdiobj.cntry_data(cntry=self.iso3c, series_codes=series_codes, verbose=verbose)


	def from_df(self, cntry, df, verbose=False):
		'''
			Construct Country() from DataFrame 
		'''
		raise NotImplementedError

	def adjust_for_pickling(self):
		# -- Remove  PyCountry -- #
		if self.iso_country:
			self.cntry = None

	def adjust_from_pickling(self):
		# - Restore PyCountry Object - # 
		if self.iso_country:
			if self.historic:
				self.cntry = pc.historic_countries.get(alpha3=self.iso3c)  		 	#Historic Countries
			else:
				self.cntry = pc.countries.get(alpha3=self.iso3c) 

### --- Countries Class --- ###

class Countries():
	'''
		Countries Object for Storing Country() instances
		<iso3c> : Country()
	'''

	## -- Setup and Initialise -- ##

	def __init__(self, verbose=False):
		## -- Check if ./pickles/products.pickle exists -- ##
		# if os.path.isfile('pickles/countries.pickle'):
		# 	if verbose: print "Loading data from default pickle file: ./pickles/countries.pickle"
		# 	self.countries = pickle.load('pickles/countries.pickle')
		# else:
		# 	if verbose: print "No Default Pickle Data Found ... Creating an empty Dict() but will need to run a constructer"
		# 	self.countries = dict()
		self.countries = dict()
		self.country_codes = sorted(self.countries.keys())

	## -- Python Standard Methods -- ##

	def __getitem__(self, value):
		''' Return Country Object
			Future Work:
				[1] Extend to Slices based on sorted keys()
		'''
		if value in self.country_codes:
			return self.countries[value]
		else:
			raise ValueError("Country (%s) is not in Countries Object" % value)

	def __repr__(self):
		return str(sorted(self.countries.keys()))

	## -- IO -- ##

	def from_df(self, df, verbose=False):
		''' 
			Load Country Data from DataFrame
							 |>	   index         <||>	columns						<|
			Incoming Format: <is3c>, <series_code>, <name>, <series_name>, <years> ... [Format From WDI Tables]
			
			Notes:
				[1] This is largely written prior to WDI.py. Is there a standard df form required here?
				[2] Not Required unless there is a standard country file used in project work
		'''
		## -- Check Data is Well Formed -- ##
		if df.index.names != [u'iso3c', u'name', u'series_code', u'series_description']:
			raise ValueError("DataFrame Index is not named correctly. It should be pd.Index([u'iso3c', u'name', u'series_code', u'series_description']) but it is %s" % (df.index)) 
		else:
			if verbose: print "Loading Country data from DataFrame"
			for country in set(df.index.levels[0]):  			#Is there a better way to do this wiht df.index['iso3c'] for example?
				if verbose: print "Loading Country: %s" % country
				c = Country(iso3c=country)
				c.from_df(country, df.ix[country]) 					#Currently Explicitely Passing a country name rather than relying on .name architecture of pandas?
				self.countries[country] = c
		return self.countries

	def from_csv(self, fn, verbose=False):
		'''
			Load Product Data from: <filename>. 
			Incoming Format: <name>, <iso3c>, <series_name>, <series_code>, <years> ... [Format From WDI Tables]
			Notes: Not Required?
		'''
		raise NotImplementedError	

	def to_pickle(self, fn='pickles/countries.pickle', verbose=False):
		'''
			Save Countries() to Pickle File
			Notes:
				[1] pycountry objects don't pickle
		'''	
		# - Test Directory - #
		directory = fn.split(r'/')[0] 														#This requires / at the beginning of a filename if not in directory!
		if not os.path.isdir(directory):
			print "[Notice] Pickle directory not found ... creating ./%s" % directory
			os.makedirs(directory)
		if verbose: print "Pickling Countries() Instance to %s" % fn
		# -- Removing pyCountry Objects -- #
		print "[Warning]: Pickling Removes country.cntry objects (pycountry) as they don't serialise well"
		for cntry in self.countries.keys():
			self.countries[cntry].adjust_for_pickling()
		# - Write Pickle File - #
		fl = open(fn, 'wb')
		pickle.dump(self, fl)
		fl.close()
		if verbose: print "Pickling finished!"
		return True

	def from_pickle(self, fn='pickles/countries.pickle', verbose=False):
		'''
		Restore Countries() from Pickle File
		Notes:
			[1] Reconsruct country.cntry objects
			[2] Currently this returns object of self and therefore must use like: C = Countries(); C = C.from_pickle()
		Issues:
		 	[1] Restoring from pickle takes ~100s which is longer than compiling the Data from WDI objects!
		'''
		if verbose: print "Restoring Countries() object from %s" % fn
		fl = open(fn, 'rb')
		obj = pickle.load(fl)
		fl.close()
		# - Restore PyCountry Objects - #
		for cntry in obj.countries.keys():
			obj.countries[cntry].adjust_from_pickling()
		if verbose: print "Finished restoring!"
		return obj

	def build_from_wdi(self, wdiobj, verbose=False):
		''' 
			Build A set of Countries from the WDI Object 
		'''
		if not isinstance(wdiobj, wdi.WDI): 									# Not setting self.wdi = False as the Country Object may already be filled
			if verbose: print "Incoming wdi is not a WDI object!"
			return False
		if verbose: print "Adding %s countries from the passed WDI Object" % len(wdiobj.country_codes)
		for iso3c in wdiobj.country_codes:
			c = Country(iso3c)
			c.build_from_wdi(wdiobj)
			self.countries[iso3c] = c
		self.country_codes = sorted(self.countries.keys())
		return self.countries


	## -- Additions & Removals -- ##

	def add_country(self, cntry_code, df_data, save_changes=True, verbose=False):
		'''
			Add Product to Products()
		'''
		raise NotImplementedError	

	def remove_country(self, cntry_code, save_changes=True, verbose=False):
		'''
			Remove Product from Products()
		'''
		raise NotImplementedError	


### --- Main --- ###

if __name__ == '__main__':
	print "Libray File for Countries Class"

	# ## -- Testing Country() -- ##

	# # - Country Instance Testing - #
	# print
	# print "Testing Country 'AUS'"
	# A = Country('AUS')
	# print A.name
	# print A.iso3n
	# print A.historic
	# print A

	# ## -- Itegration with WDI Testing -- ##
	# print
	# print "Integration with WDI Objects"
	# B = wdi.WDI('WDI_Data.csv',source_ds='d1352f394ef8e7519797214f52ccd7cc', hash_file_sep=r' ', verbose=True)
	# A.build_from_wdi(B)
	# print A.wdi[0:10]
	# A.build_from_wdi(B, series_codes=[r'NY.GDP.MKTP.CD'], verbose=True)
	# print A.wdi
	# A.build_from_wdi(B, series_codes=r'NY.GDP.MKTP.CD', verbose=True)  					#This will return a pd.Series
	# print A.wdi
	# # - Not WDI Object Test - #
	# print
	# A.build_from_wdi(A, verbose=True)

	# ## -- Old Country Codes -- ##
	# print 
	# print "Testing Old Country Code: 'SUN'"
	# A = Country('SUN')
	# print A.name
	# print A.iso3n
	# print A.historic
	# print A

	# ## -- Testing Countries -- ##
	# print
	# print "Testing Countries Module"
	# C = Countries()
	# # - Build from WDI Method - #
	# C.build_from_wdi(B)
	# aus = C['AUS']
	# print aus.name
	# print aus.iso3n

	# # Writing Pickle #
	# print "Testing Pickle"
	# C.to_pickle(verbose=True)

	## ABOVE TESTING WORKS ##

	# Restoring #
	# D = Countries()
	# D = D.from_pickle(verbose=True) 			#How to write this to self within the method?
	# aus = D['AUS']
	# print aus.name
	# print aus.iso3n
	# print aus.cntry
