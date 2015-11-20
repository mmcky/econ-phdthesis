"""
Project: 	PyEconLab
Class: 		Dynamic ProductLevelExportSystem - Cross Section [Pandas Core Data Structure]
Author: 	Matthew McKay <mamckay@gmail.com> 

Dynamic Product Level Export Systems

Current Work
------------
[1] Convert to PyEconLab [IN-WORK]

Dependancies:
------------
	ProductLevelExportSystem

Architecture Question:
---------------------
	Should this have the following simplifing structure
		DynamicProductLevelExportSystem {Contains Core Attributes and Methods of Objects}
			Diffusion 				{Diffusion Analysis}
			Plotting 
			Animation 
			Experimental			{Place for Random Ideas like network construct_cp_emergence_graph()}

Future Work:
-----------
	[1] MultiCore Methods Deliver ~50 percent improvement compared to serial versions (over 4 cores). 
		There must be large overheads in this relatively simply method of splitting the analysis. 

Issues:
------
	[1] Using this library from another project folder breaks multicore methods with an import error?	
"""

### --- Standard Library Imports --- ###

from __future__ import division

### TO DO: CHECK WHICH ARE REQUIRED IN THIS MODULE ###

import sys
import re
import warnings
import numpy as np
import pandas as pd
import networkx as nx
from networkx.algorithms import bipartite
import matplotlib.pyplot as plt
import cPickle as pickle

### -- Project Imports --- ###
import pyeconlab.wdi as wdi
from Countries import Country, Countries 			#Move to Package Countries Subpackage
from Products import Product 						#Move to Package Trade/Classifications?

### Remove After Refactor
from ProductLevelExportSystem import *

### --- Parallel Computing Settings --- ###
NUM_CORES = 4

class DynamicProductLevelExportSystem(object):
	"""
	Dynamic System of Product Level Export Data [X(year,country,product,value), M(year,country,product,value)]
	Conventions:
	------------ 
		[1] Incoming Edge = Import & Outgoing Edge = Export
	
	Types: 
		Dict{ 'Year' : BiPartiteGraph('country' -> 'product', edge = values)}
		Dict{ 'Year' : MultiDiGraph('country' -> 'world', edge = product object)}

	Input: <year>, <exporter>, <importer>, <product>, <value>


	Notes:
	-----
		[1] Class Needs to Extend object for Properties to work correctly

	Questions:
	----------
		[1] Should Class Properties return Dictionaries or DataFrames? [Currently Dictionaries]
			Could set a return_type in the class. self.return_df = False / True
			Dictionaries are useful for simple year filters etc. (Can use return_dataframe() to get a dataframe)

	Future Work:
	-----------
		[1] Improve Complete_trade_network attribute to reflect true for ALL cross-sections (i.e. [True, True, True .... True])
			A.complete_trade_network = True would set complete_trade_network = True for All PLES (SETTER METHOD)
		[2] Remove #self.ples[year].complete_trade_network = self.complete_trade_network from matrices_constructor() methods
	"""
	
	## -- Base Class Functions -- ##

	def __init__(self, fn='', series_name='export', replace={}, verbose=False):
		
		## -- Core Cross-Section Object Tables -- ##
		self.ples 			= dict()						# In-Memory Core Data
		self.ples_struct 	= 'ProductLevelExportSystem'	
		self.compile_dtypes = ['DataFrame']   				# List of Objects to Load ['DataFrame', 'MultiDiGraph', 'BiPartiteGraph']
		# - Panel Level Attributes - #
		self.years = None
		self.country_classification = None 			# Country Classification 'iso3c'
		self.product_classification = None 			# Product Classification 'SITCR2L4'
		self.global_panel = None					# True/False
		# - Private Class Attributes - #
		self._complete_trade_network = None 		# True/False
		# - Data Source Details - #
		self.data_file = None
		
		# - MultiCore Computing Settings - #
		try:
			from IPython.parallel import Client
			c = Client()
			self.multicore = True 					
		except:
			self.multicore = False
		
		# - Parse Options - #
		if re.search("\.csv", fn):
			print "[NOTICE] Populating Object from csv file with default kwargs: %s (May use .from_csv() method for more options)" % fn
			self.from_csv(fn, replace=replace, verbose=verbose)


	## -- Python Class Methods -- ##

	def __repr__(self):
		def ordered_string(data):
			keys = sorted(data.keys())
			rep = ''
			for key in keys: 
				rep = rep + 'Year(%s): %s\n' % (key, data[key])
			return rep
		return ordered_string(self.ples)

	def __getitem__(self, index):
		if isinstance(index, int) or isinstance(index, np.integer):
			return self.ples[index]
		elif isinstance(index, slice):
			if index.step == None: step = 1
			else: step = index.step
			ryrs = range(index.start,index.stop,step)			#Could use (index.stop+1) .. but probably best to stick with convention
			try:
				r = dict()
				for year in ryrs:
					r[year] = self.ples[year]
				return r
			except:
				print "[WARNING] Years in Object: %s" % self.years
				raise IndexError("Index is out of range for the underyling data")
		else:
			raise TypeError("Index must be a year or slice of years")

	####################################
	## -- Property and Get Methods -- ##
	####################################

	@property
	def data(self):
		if self.years == None: return None
		data = dict()
		for year in self.years:
			data[year] = self.ples[year].data
		return data

	def get_data(self, year=None, rtype=dict, order=['year', 'country', 'productcode'], sort_index=True, verbose=False):
		"""
		More complete get method for ProductLevelExportSystem.data
		
		Parameters:
		-----------
			year 	: 	int, optional(default==None)
						Obtain Year Specific Data
		
		if year == None
		~~~~~~~~~~~~~~~
			rtype 	: 	type, optional(default=dict)
						'dict' : {year : DataFrame}
						'long' : pd.DataFrame() - 'Long Format'
						'wide' : pd.DataFrame() - 'Wide Format'
						'panel': pd.Panel()
			order 	: 		list, optional(default=['year', 'country', 'productcode'])
							Specify an Index Order for 'Long Format'
			sort_index 	: 	optional(default=True)
							Return Sorted Objects [Default = True]

		"""
		if year == None:
			# - Return ALL years within the Panel - #
			if rtype == dict: 											#Default Behaviour
				return self.data
			elif rtype == 'long':
				# - Construct Long Panel - #
				data = pd.Panel(self.data).to_frame(filter_observations=False)   #Don't Filter Incomplete Data
				data = data.stack().unstack(level='minor')
				# - Rename Index Values - # 									#Why doesn't pd.Panel() support naming of dict keys?
				data.columns.name = None
				idx_names = list(data.index.names)
				idx_names[-1] = 'year'
				data.index.set_names(names=idx_names, inplace=True)
				# - Reorder so can be fed back into from_df() method - #
				if order != None:
					if len(order) != 3: raise ValueError("Order must be an order of 'country', 'productcode', and 'year' for LONG format")
					data = data.reorder_levels(order=order)
				if sort_index: data.sort_index(inplace=True)
				return data
			elif rtype == 'wide':
				data = pd.Panel(self.data).to_frame(filter_observations=False)   #Don't Filter Incomplete Data
				# data = data.reset_index(level='minor')
				# del data['minor']
				data.index = data.index.droplevel(level='minor')
				data.columns.name = 'year'
				if sort_index: data.sort_index(inplace=True)
				return data
			elif rtype == 'panel':
				data = pd.Panel(self.data).to_frame(filter_observations=False)   #Don't Filter Incomplete Data
				# Does this Need Sorting? #
				return data
			else:
				raise ValueError("rtype must be: dict, wide, long, or panel")
		# - Return Data for a Single Year - #
		else:
			return self.ples[year].data

	@property 
	def cp_matrix(self):
		if self.years == None: return None
		data = dict()
		for year in self.years:
			data[year] = self.ples[year].cp_matrix
		return data

	def get_cp_matrix(self, year):
		return self.ples[year].cp_matrix

	@property
	def rca(self):
		if self.years == None: return None
		data = dict()
		for year in self.years:
			data[year] = self.ples[year].rca
		return data		

	def get_rca(self, year):
		return self.ples[year].rca

	@property
	def rca_num(self):
		if self.years == None: return None
		data = dict()
		for year in self.years:
			data[year] = self.ples[year].rca_num
		return data

	@property
	def rca_den(self):
		if self.years == None: return None
		data = dict()
		for year in self.years:
			data[year] = self.ples[year].rca_den
		return data

	@property
	def mcp(self):
		if self.years == None: return None
		data = dict()
		for year in self.years:
			data[year] = self.ples[year].mcp
		return data		

	def get_mcp(self, year):
		return self.ples[year].mcp

	@property
	def proximity(self):
		if self.years == None: return None
		data = dict()
		for year in self.years:
			data[year] = self.ples[year].proximity
		return data	

	def get_proximity(self, year=None, rtype=dict, order=['year', 'productcode1', 'productcode2'], sort_index=True, verbose=False):
		"""
		More complete get method for ProductLevelExportSystem.proximity
		
		Parameters:
		-----------
			year 	: 	int, optional(default==None)
						Obtain Year Specific Data
		
		if year == None
		~~~~~~~~~~~~~~~
			rtype 	: 	type, optional(default=dict)
						'dict' : {year : DataFrame}
						'long' : pd.DataFrame() - 'Long Format'
						'wide' : pd.DataFrame() - 'Wide Format'
						'panel': pd.Panel()
			order 	: 		list, optional(default=['year', 'productcode1', 'productcode2'])
							Specify an Index Order for 'Long Format'
			sort_index 	: 	optional(default=True)
							Return Sorted Objects [Default = True]

		"""
		if year == None:
			# - Return ALL years within the Panel - #
			if rtype == dict: 													#Default Behaviour
				return self.proximity
			elif rtype == 'long':
				# - Construct Long Panel - #
				data = pd.Panel(self.proximity).to_frame(filter_observations=False)   	#Don't Filter Incomplete Data
				data.index.set_names(names=['productcode1', 'productcode2'], inplace=True)
				data.columns.name = 'year'
				data = pd.DataFrame(data.stack(), columns=['proximity'])
				if order != None:
					if len(order) != 3: raise ValueError("Order must be an order of 'productcode1', 'productcode2', and 'year' for LONG format")
					data = data.reorder_levels(order=order)
				if sort_index: data.sort_index(inplace=True)
				return data
			elif rtype == 'wide':
				data = pd.Panel(self.proximity).to_frame(filter_observations=False)   	#Don't Filter Incomplete Data
				data.index.set_names(names=['productcode1', 'productcode2'], inplace=True)
				data.columns.name = 'year'
				if sort_index: data.sort_index(inplace=True)
				return data
			elif rtype == 'panel':
				data = pd.Panel(self.data).to_frame(filter_observations=False)   		#Don't Filter Incomplete Data
				# Does this Need Sorting? #
				return data
			else:
				raise ValueError("rtype must be: dict, wide, long, or panel")
		# - Return Data for a Single Year - #
		else:
			return self.ples[year].proximity

	@property
	def ubiquity(self):
		if self.years == None: return None
		data = dict()
		for year in self.years:
			data[year] = self.ples[year].ubiquity
		return data	

	def get_ubiquity(self, year):
		return self.ples[year].ubiquity

	@property
	def diversity(self):
		if self.years == None: return None
		data = dict()
		for year in self.years:
			data[year] = self.ples[year].diversity
		return data	

	def get_diversity(self, year):
		return self.ples[year].diversity

	@property
	def eci(self):
		if self.years == None: return None
		data = dict()
		for year in self.years:
			data[year] = self.ples[year].eci
		return data	

	def get_eci(self, year):
		return self.ples[year].eci

	@property
	def pci(self):
		if self.years == None: return None
		data = dict()
		for year in self.years:
			data[year] = self.ples[year].pci
		return data	

	def get_pci(self, year):
		return self.ples[year].pci

	@property
	def complete_trade_network(self):
		for year in self.years:
			if self.ples[year].complete_trade_network == False:
				self._complete_trade_network = False
				return self._complete_trade_network 								 # Can Return as only need one instance								
		self._complete_trade_network = True
		return self._complete_trade_network

	@complete_trade_network.setter 
	def complete_trade_network(self, value):
		if type(value) != bool:
			raise ValueError("value must be of type bool (True/False)")
		else:
			for year in self.years:
				self.ples[year].complete_trade_network = value
			self._complete_trade_network = value

	## -- General Data Retrieval -- ##

	def get_series_fmatrix(self, data, row, column, series_name='', years=None, verbose=False):
		""" Obtain a Series from Dict(DataFrames) """	
		if type(years) != list:
			years = sorted(data.keys())
		series_data = []
		for year in years:
			series_data.append(data[year].get_value(index=row, col=column))
		# - Construct pd.Series - #
		s = pd.Series(series_data, index=years)
		if series_name == '': 											#Attempt to find the name of the data from the first matrix
			try: series_name = data[years[0]].name + '(Row: %s; Column: %s)' % (row, column)
			except: pass
		s.name = series_name
		return s

	def get_series_flist(self, data, index, series_name='', years=None, verbose=False):
		""" Obtain a Series from Dict(Series) """
		if type(years) != list:
			years = sorted(data.keys())
		series_data = []
		for year in years:
			series_data.append(data[year].get_value(label=index))
		# - Construct pd.Series - #
		s = pd.Series(series_data, index=years)
		if series_name == '':
			try: series_name = data[years[0]].name
			except: pass
		s.name = series_name
		return s 

	def reshape_data(self, data, rshape, datashape='cp', rtype='df', year_filter=None, sort_index=True, drop_na=False, verbose=False):
		"""
		General Function that Obtains Data and returns the desired shape
		
		Parameters:
		-----------
		data 	: 	property
					Passed Property (i.e. DynPLES.rca) [Needs to be cxp data]
		rtype 	: 	str
					Specify a Return Shape ["long", "wide", "panel"]
					'long' : pd.DataFrame() - 'Long Format'
					'wide' : pd.DataFrame() - 'Wide Format'
					'panel': pd.Panel()     - 'Wide Format Useful for Finding Time Series'
		year_filter : 	list, optional(default=None *All Years*)
						Year Filter
		sort_index 	: 	bool, optional(default='True')
						Return Sorted Index Object
		drop_na 	: 	bool, optional(default=False)
						Drop NaN values

		"""
		# - Parse dshape - #
		if datashape == 'cp': idx_names = ['year', 'country', 'productcode']
		if datashape == 'pp': idx_names = ['year', 'productcode1', 'productcode2']
		# - Parse Year Option - #
		if type(year_filter) == list:
			data = data[year_filter]
		# - Prepare Data as a Panel - #
		panel = pd.Panel(data).to_frame(filter_observations=False)   		#Don't Filter Incomplete Data
		# - Name Panel Based on datashape - #
		if datashape == 'cp':
			panel.index.set_names(names=['country', 'productcode'], inplace=True)
			panel.columns.set_names(names=['year'], inplace=True)
		elif datashape == 'pp':
			panel.index.set_names(names=['productcode1', 'productcode2'], inplace=True)
			panel.columns.set_names(names=['year'], inplace=True)
		else:
			raise NotImplementedError("Datashape must be 'cp' or 'pp'")
		# - Reshape Data - #
		if rshape == 'long':
			# - Construct Long Series - #
			rdata = panel.stack(level='year', dropna=drop_na) 							#Default = False
			# - Name Data From First Object in Data Dictionary - #
			rdata.name = data[self.years[0]].name 										#Assume Homogenous Dictionaries
			# - Reorder so can be fed back into from_df() method - #
			if type(idx_names) == list:
				if len(idx_names) != len(rdata.index.names): 
					raise ValueError("Order must be of Length %s to match rdata index currently named (%s)" % (len(rdata.index.names), rdata.index.names))
				rdata = rdata.reorder_levels(order=idx_names) 								#Default is ['year', 'country', 'productcode']
			if sort_index: 
				rdata = rdata.sort_index()
			if rtype == 'df':
				rdata = pd.DataFrame(rdata)
			return rdata
		elif rshape == 'panel':
			# - Name Data From First Object in Data Dictionary - #
			panel.name = data[self.years[0]].name
			return panel
		elif rshape == 'wide':
			# - Arrange into Wide C x P - #
			if datashape == 'cp': rdata = panel.stack(level='year').unstack(level='productcode')
			if datashape == 'pp': rdata = panel.stack(level='year').unstack(level='productcode2')
			rdata = rdata.reorder_levels(order=idx_names[0:2]) 							#Default is ['Year', 'Country']
			if sort_index: 
				rdata.sort_index(inplace=True)
			# - Name Data From First Object in Data Dictionary - #
			rdata.name = data[self.years[0]].name
			return rdata
		else:
			raise ValueError("rtype must be: long, panel or wide")

	def long_data_to(self, long_data, rshape='cp', rtype='df', verbose=False):
		""" Change Long DataFrame to Matrix """
		# - Parse rshape - #
		if rshape == 'cp':
			dname = long_data.columns[0]
			wide_data = long_data.unstack(level='productcode')
			wide_data.columns = wide_data.columns.droplevel(level=0)
			wide_data.name = dname
			# - Ensure 'year', 'country' - #
			wide_data = wide_data.reorder_levels(order=['year', 'country'])
		elif rshape == 'pp':
			dname = long_data.columns[0]
			wide_data = long_data.unstack(level='productcode2')
			wide_data.columns = wide_data.columns.droplevel(level=0)
			wide_data.name = dname
			# - Ensure 'year', 'country' - #
			wide_data = wide_data.reorder_levels(order=['year', 'productcode1'])
		else:
			raise ValueError("rshape must be cp or pp")
		# - Parse rtype - #
		if rtype == 'df':
			return wide_data
		elif rtype == 'dict':
			rdict = dict()
			for year in wide_data.index.levels[0]:
				rdict[year] = wide_data.ix[year]
				rdict[year].name = dname
			return rdict
		else:
			raise ValueError("rtype must be 'df' or 'dict'")

	#########################
	## -- Setup Methods -- ##
	#########################

	def compute(self, items=['rca', 'mcp', 'proximity', 'diversity', 'ubiquity'], verbose=False):
		"""
		Methods to Populate the underlying ProductLevelExportSystem Objects
		
		Notes:
		-----
			1. Not sure this is worth it as each method has settings and default settings may not be desired. 

		"""
		for item in items:
			if verbose: print "Computing %s Data" % item
			raise NotImplementedError

	###########################
	## -- Network Methods -- ##
	###########################

	##--!!--Should this be moved to object with network backend?--!!-##

	def construct_network(self, ntype='bipartitegraph', c=None, p=None, verbose=False):
		""" Construct Network of ntype from self.ples """
		for year in self.years:
			self.ples[year].construct_network(ntype, c, p, verbose)

	def construct_bipartite(self, verbose=False):
		""" Construct BiPartite Networks from self.ples """
		for year in self.years:
			self.ples[year].network = self.ples[year].construct_bipartite(verbose)

	def construct_multidi(self, verbose=False):
		""" Construct a MutliDiGraph Network from self.ples """
		for year in self.years:
			self.ples[year].network = self.ples[year].construct_multidi(verbose)

	##############
	## -- IO -- ##
	##############

	# - CSV - #

	def change_data_series_name(self, replace={'exports' : 'export'}, verbose=False):
		"""
		Change Data Series Name in (self.data)

		STATUS: DEPRICATED (ADDED as option to from_csv() and from_df())

		Parameters
		----------
		replace 	: 	dict, optional(default={'exports' : 'export'})
						dict("exports" : "export")

		Typical Usage is when loading "exports" and want to change to "export" for internal consistency
		
		"""
		warnings.warn("[DEPRICATED] use replace={} in from_csv() or from_df()", UserWarning)
		# - Info - #
		for item in sorted(replace.keys()):
			print "[Info] Changing Series named: %s to :%s" % (item, replace[item])
		for year in self.years:
			column = self.ples[year].data.columns
			new_column = []
			for item in column:
				try:
					new_column.append(replace[item]) 	#Found in Lookup Table
				except:
					new_column.append(item) 			#Not Found in Lookup Table
			if verbose: print "[%s] Changing %s to %s" % (year, column, new_column)
			self.ples[year].data.columns = pd.Index(new_column)


	def from_csv(self, fn, country_classification='ISO3C', cntry_obj=None, product_classification='SITCR2L4', prod_obj=None, dtypes=['DataFrame'] , years=None, replace={}, verbose=False):
		"""
		Import Data from Standard CSV File

		File Interface = <year>, <country>, <productcode>, <export-value>

		Parameters
		----------
		fn 						:  	str
									Specify File Name
		country_classification 	: 	str, optional(default="ISO3C")
									Specify Country Classification (if appropriate)
		cntry_obj 				: 	collection, optional(default=None)
									Use country objects rather than names
		product_classification 	: 	str, optional(default="SITCR2L4")
									Specify Product Classification
		prod_obj 				: 	collection, optional(default=None)
									Use product objects rather than names
		dtypes 					: 	list(str), optional(default=['DataFrame'])
									Specify which dtypes to compile
		years 					: 	list(int), optional(default=None **All Years**)
									Specify Year Filter
		replace  				: 	dict, optional(default={})
									Replace Column Names from File

		Notes:
		------
			1. Import into DataFrame and then call from_df() method
			2. ProductCode dtype == 'str' to handle leading zero's easily
			3. Pass Country and Product Objects around as they are instances of separate class

		Future Work:
		------------
			1. 	Move Network Construction to a construct_network() method in ProductLevelExportSystem Class
				Then No long need to carry around Country and Product Objects
		"""
		# - Import CSV - #
		if verbose: print "Loading Dynamic Product Level Export System From: %s" % fn
		self.data_file = fn
		df = pd.DataFrame(pd.read_csv(fn, dtype={'productcode' : str})) 			# Ensure DataFrame Object for Index
		df.set_index(keys=['year'], inplace=True)
		if type(years) == list: 													# Apply Year Filter
			df = df.ix[years]
		# - Set Passed Attributes - #
		self.country_classification = country_classification
		self.product_classification = product_classification
		# - Construct ProductLevelExportSystem for each year - #
		self.from_df(df, cntry_obj=cntry_obj, prod_obj=prod_obj, dtypes=dtypes, replace=replace, verbose=verbose)

	# - DataFrames - #

	def from_df(self, df, cntry_obj=None, prod_obj=None, dtypes=['DataFrame'], replace={}, verbose=False):
		"""
		Construct ProductLevelExportSystem from LONG Pandas DataFrame Object

		DataFrame Interface

				'country' 	'productcode' 	'export'
		<year> 	

		Parameters
		----------
		df 			: 	pd.DataFrame 
						Dataframe Containing Export Data
		cntry_obj 	: 	collection, optional(default=None)
						Supply Country Objects
		prod_obj  	: 	collection, optional(default=None)
						Supply Product Objects 
		dtypes 		:	list(str), optional(default=['DataFrame'])
						List of Objects to Compile
		replace 	: 	dict, optional(default={})
						Replace a column name in the incoming dataframe

		Notes
		-----
			1. 	This Class DynamicProductLevelExportSystem is dealing with the Dynamic elements of trade data. 
				Therefore, 'country', 'productcode' etc are not indices at this stage

		Future Work
		------------
			1. 	Move Network Construction to a construct_network() method in ProductLevelExportSystem Class
				Then No long need to carry around Country and Product Objects
			2.  Add df.notes = ''
			3.  Should Column and IDX interfaces be moved to a utility function?

		"""
		# - Init Years from DF - #
		self.years = sorted(set([int(x) for x in df.index]))
		#-Check Replacements-#
		if replace != {}:
			df.rename(columns=replace, inplace=True)
		# - Check Columns Interface - #
		colnames = set(df.columns)
		column_interface = ['country', 'productcode', 'export']
		for item in column_interface:
			if item not in colnames:
				print "[INFO] Current DataFrame Has Columns: %s" % colnames
				print "[INFO] Interface requirements is: ('country', 'productcode', 'export')"
				raise ValueError("%s is not contained in the incoming dataframe!" % item)
		# - Check Index Interface - #
		idxnames = df.index.names
		idx_interface = ['year']
		for item in idx_interface:
			if item not in idxnames:
				print "[INFO] Current DataFrame is indexed by: %s" % idxnames
				print "[INFO] Interface requirements is: ('year')"
				raise ValueError("%s is not indexed correctly in the incoming dataframe!" % item)
		for year in self.years:
			if verbose: print "\nComputing ProductLevelExportSystem for Year: %s" % year
			cross_section = df.ix[year].set_index(keys=['country', 'productcode'])
			# - Construct PLES - #
			ples = ProductLevelExportSystem()
			ples.from_df(cross_section, self.country_classification, self.product_classification, dtypes, year, cntry_obj=cntry_obj, prod_obj=prod_obj, verbose=verbose)
			ples.data_file = self.data_file  		#- Inform Ples Objects of source_file - #
			self.ples[year] = ples
		return self.ples


	def from_rca_df(self, rca, rca_notes='', verbose=False):
		"""
		Construct a DynamicProductLevelExportSystem from an RCA Matrix
		Note: Useful when sources is already RCA Data

		DataFrame Interface

				'country' 'productcode' 'rca'
		<year>	

		Parameters
		----------
		rca 	: 	pd.DataFrame
					DataFrame containing RCA Data
		rca_notes : str, optional(default='')
					Attach any notes to the object

		Notes
		-----
			1. Migrate away from carrying C objects and P Objects and outsource network_construction to construct_network(etc.)
			2. No dtypes implimented as doesn't make sense

		"""
		# - Init Years from DF - #
		self.years = sorted(set(df.index))
		for year in self.years:
			if verbose: print "\nComputing ProductLevelExportSystem for Year: %s (From RCA Data)" % year
			cross_section = df.ix[year].set_index(keys=['country', 'productcode'])
			# - Construction of PLES - #
			ples = ProductLevelExportSystem()
			ples.from_rca_df(cross_section, self.country_classification, self.product_classification, year, verbose=verbose)
			self.ples[year] = ples
		return self.ples

	## -- Pickle Methods -- ##
	##########################

	def to_pickle(self, fn='', verbose=False):
		"""
		Preserve Entire Object in a Pickle File

		Pickle Interface 
		~~~~~~~~~~~~~~~~
		(self.ples, self.ples_struct, self.compile_dtypes, self.years, self.country_classification, 	\
				self.product_classification, self.global_panel, self._complete_trade_network, self.data_file)
		
		Parameters
		----------
		fn 	: str, optional(fn='')
			  Specify a custom file name

		"""
		if not re.search(".pickle", fn):
			print "Auto-Generating a name for the object"
			# - For Now use the main data_file amended to pickle file - #
			fn = "./pickles/" + self.data_file.split("/")[-1].split(".")[0] + "_object.pickle"        	# - This could be improved as may not be the best auto-name - #
		interface = (self.ples, self.ples_struct, self.compile_dtypes, self.years, self.country_classification, 	\
					self.product_classification, self.global_panel, self._complete_trade_network, self.data_file)
		with open(fn, 'w') as f:
			pickle.dump(interface, f)
		f.close()
		if verbose: print "Pickle written to: %s" % fn
		return True


	def from_pickle(self, fn, verbose=False):
		"""
		Restore Entire Object From a Pickle File

		Pickle Interface
		~~~~~~~~~~~~~~~~
		(self.ples, self.ples_struct, self.compile_dtypes, self.years, self.country_classification, 	\
				self.product_classification, self.global_panel, self._complete_trade_network, self.data_file)
		
		Parameters
		----------
		fn 	: 	str
				Specify filename

		"""
		with open(fn) as f:
			(self.ples, self.ples_struct, self.compile_dtypes, self.years, self.country_classification, self.product_classification, \
				self.global_panel, self._complete_trade_network, self.data_file) = pickle.load(f)
		f.close()
		if verbose: print "Pickle read from: %s" % fn
		return True

	## ----------------------------- ##
	## -- Specific Matrix Methods -- ##
	## ----------------------------- ##

	def pickle_matrices(self, matrices, fn=None, directory='./pickles/'):
		"""
		Pickle a Dictionary of Matrices
		
		Useful when doing analysis that takes time and just want to preserve a single Matrix (i.e PCI)
		
		Parameters
		----------
		matrices 	: 	Pass Property or Matrices to Preserve
		fn 			: 	str, optional(default=matrix.name)
						Specify custom filename
		directory 	: 	str, optional(default="./pickles/")
						Specify custom directory

		Usage
		-----
		A.pickle_matrices(A.rca, 'rca') # - DynPLES

		Future Work
		-----------
			1. Check Directory Exists
		"""
		if type(fn) != str:
			fn = matrices[0].name
		rfn = directory + fn + '.pickle'
		pickle.dump(matrices, fn)
		print "Saved pickle file of matrices (%s) to: %s" % (fn, rfn)
		return True

	def restore_pickled_matrices(self, matr, matr_name, fn, directory='./pickles/'):
		"""
		Restore Pickled Matrices to self.rca etc.
		
		Parameters
		----------
		matr  		: 	Pass in Property or Matrix
		matr_name	: 	Specify Matrix Name
		fn 			: 	str, optional(default=matrix.name)
						Specify custom filename
		directory 	: 	str, optional(default="./pickles/")
						Specify custom directory

		Usage
		-----
		A.restore_pickled_matrix(A.rca, 'RCA', 'rca.pickle')

		Note
		----
			[1] If change A.rca etc to use setter() then this will need to be updated!
		"""
		fn = directory + fn
		matr = pd.read_pickle(fn)
		matr.name = matr_name
		print "Read Pickle from File (%s) and created a matrix named: %s" % (fn, matr.name)
		#self.matrix = matr
		return self.matrix

	## -------------------------- ##
	## -- Data Reshape Methods -- ##
	## -------------------------- ##

	def as_cp_matrices(self, years=None, matrix_type='pandas', value='export', verbose=False):
		"""
		Compute CP Matrix in each ProductLevelExportSystem
		
		Parameters
		----------
		years 	:	list(int), optional(years=None **All**)
					Year Filter
		matrix_type : 	str, optional(default='pandas')
						Specify type of matrix 
		value 		: 	str, optional(default='export')
						Specify Data Name

		"""
		if years == None: years = self.years
		for year in years:
			if verbose: print "Computing cp matrix for year: %s" % year
			self.ples[year].as_cp_matrix(matrix_type=matrix_type, value_name=value, verbose=verbose)
		## -- Return Getter Method? -- ##
		return self.cp_matrix

	# - Should these be Static Methods @staticmethod - #

	def return_panel(self, matrix, minor_axis, verbose=False):
		"""
		Return pd.Panel Representation of a dict(matrix) or Property of Class

		Parameters
		----------
		matrix  	: 	Specify Matrix or Property
		minor_axis  : 	Specify name of minor axis
		
		Future Work:
		-----------
			1. Impliment a years filter?

		"""
		years = sorted(matrix.keys())
		panel = pd.Panel(matrix)
		panel.name = matrix[years[0]].name 		# - Homogenous Data - #
		panel.items.name = 'year'
		panel.minor_axis.name = minor_axis
		return panel

	def from_dict_c_to_df(self, data):
		""" Convert Dict(c_matrix) to DataFrame """
		df = pd.DataFrame(data)
		data_name = data[data.keys()[0]].name 				# - Homogenous Data Types - #
		df.index.names = ['country']
		df.columns.names = ['year']
		# - Return Wide - #
		return df

	def from_dict_p_to_df(self, data):
		""" Convert Dict(p_matrix) to DataFrame """
		df = pd.DataFrame(data)
		data_name = data[data.keys()[0]].name 				# - Homogenous Data Types - #
		df.index.names = ['productcode']
		df.columns.names = ['year']
		# - Return Wide - #
		return df

	def from_dict_cp_to_df(self, data):
		""" Convert Dict(cp_matrix) to DataFrame """
		panel = self.return_panel(data, minor_axis='productcode')
		data_name = panel.name
		df = panel.to_frame(filter_observations=False)
		df.name = data_name
		df.index.names = ['country', 'productcode']
		df.columns.names = ['year']
		# - Return Wide - #
		return df

	def from_dict_pp_to_df(self, data):
		""" Convert Dict(pp_matrix) to DataFrame """
		panel = self.return_panel(data, minor_axis='productcode')
		data_name = panel.name
		df = panel.to_frame(filter_observations=False)
		df.name = data_name
		df.index.names = ['productcode1', 'productcode2']
		df.columns.names = ['year']
		# - Return Wide - #
		return df

	def return_dataframe(self, matrix, matrix_shape='cp', years=None, out_shape='wide', verbose=True):
		"""
		Return DataFrame Representation of a Property

		Parameters
		----------
		matrix 			: 	Property or Matrix
		matrix_shape 	:   Specify Matrix Shape
							'cp' -> Rows: Country x Columns: ProductCode
							'pp' -> Rows: ProductCode x Columns: ProductCode
							'c'  -> Rows: Country (with 1 Series); Currently enforces out_shape to be long
							'p'  -> Rows: Products (with 1 Series); Currently enforces out_shape to be long
		years 			: 	list(int), optional(default=None **All**)
							Specify a year filter
		out_shape 		: 	str, optional(default='wide')
							Specify out_shape of dataframe

		Notes:
		-----
			1. Should this be the default return structure?
			2. Integrate matrix_shape into a self.matrix_shape property
			3. Original Code has the option to output data indexed by a specific order for Long Formatted Data. 

		"""
		# - Apply Year Filer -# 
		if type(years) == list:
			data = dict()
			for year in years:
				data[year] = matrix[year]
		else:
			data = matrix.copy()

		# - Parse Matrix Types - #
		if matrix_shape == 'cp':
			df = self.from_dict_cp_to_df(data)
			out_index_order=['year', 'country', 'productcode']
		elif matrix_shape == 'pp':
			df = self.from_dict_pp_to_df(data)
			out_index_order=['year', 'productcode1', 'productcode2']
		elif matrix_shape == 'c':
			df = self.from_dict_c_to_df(data)
			out_index_order = ['year', 'country']
		elif matrix_shape == 'p':
			df = self.from_dict_p_to_df(data)
			out_index_order = ['year', 'productcode']
		else:
			raise ValueError("matrix_shape must be of type 'cp', 'pp', 'c', or 'p'")
		
		# - Return Structure - #
		if out_shape == 'wide':
			return df
		elif out_shape == 'long':
			# - Convert Wide to Long - #
			s = df.stack() 								# - Return a Series Object - #
			return s.reorder_levels(out_index_order)
		else:
			raise ValueError("out_type must be 'wide' or 'long'")
	

	#######################################
	## -- International Trade Methods -- ##
	#######################################

	def product_shares(self, series_name='export'):
		"""
			Return Product Shares Across DynPLES
		"""
		result = dict()
		for year in self.years:
			result[year] = self.ples[year].product_shares(series_name)
		return result

	def country_shares(self, series_name='export'):
		"""
			Return Country Shares Across DynPLES
		"""
		result = dict()
		for year in self.years:
			result[year] = self.ples[year].country_shares(series_name)
		return result

	def rca_matrices(self, years=None, series_name='export', fillna=False, clear_temp=True, complete_data=False, decomposition=False, verbose=False):
		"""
		Compute Revealed Comparative Advantage (RCA) Matrices for ProductLevelExportSystem
		RCA - Belassa Definition (Balassa, 1965)

		Parameters
		----------
		years : list(int), optional(default=None **All**)
				Specify a year list
		
		Notes
		-----
			[1] Need a load_supp_data method() in the event the trade system is incomplete
		"""
		if years == None: years = self.years
		for year in years:
			if verbose: print "Computing RCA matrix for year: %s" % year
			self.ples[year].rca_matrix(series_name, fillna, clear_temp, complete_data, decomposition, verbose) 		

	def rca_decomposition_tables(self):
		"""
			Return a Dict(pd.DataFrame) of RCA Decomposition Tables
		"""
		rca_decomposition_tables = dict()
		for year in self.years:
			rca_decomposition_tables[year] = self.ples[year].rca_decomposition_table()
		return rca_decomposition_tables

	def srca_matrices(self, years=None, series_name='export', fillna=False, clear_temp=True, verbose=False):
		"""
		Compute Symmetric RCA Matrices by applying transformation: (RCA-1)/(RCA+1) {Log Estimate}
		"""
		if years == None: years = self.years
		result = dict()
		for year in years:
			if verbose: print "Computing Symmetric RCA Matrix for year: %s" % year
			result[year] = self.ples[year].srca_matrix(series_name, fillna, clear_temp, verbose)
		return result

	def proudman_rca_matrices(self, years=None, complete_data=False, set_property=False, verbose=False):
		"""
		Compute Proudman RCA matrices
		"""
		if years == None: years = self.years
		result = dict()
		for year in years:
			if verbose: print "[INFO] Computing Proudman Normalised RCA Matrix for year: %s"%year 
			if self.ples[year].rca == None:
				self.ples[year].rca_matrix(complete_data=complete_data)
			result[year] = self.ples[year].proudman_rca_matrix(set_property=set_property)
		if not set_property:
			return result

	def yu_rca_matrices(self, years=None, fillna=False, apply_factor=True, return_mcp=False, set_property=False, verbose=False):
		"""
		Compute Yu Normalised RCA Matrices [Yu, 2009]
		"""
		if years == None: years = self.years
		result = dict()
		for year in years:
			if verbose: print "[INFO] Computing Yu Normalised RCA Matrix for year: %s"%year 
			result[year] = self.ples[year].yu_rca_matrix(fillna=fillna, apply_factor=apply_factor, return_mcp=return_mcp, set_property=set_property)
		if not set_property:
			return result

	def rcav_matrix(self, years, fillna=False, complete_data=False, return_intermediates=False, verbose=False):
		"""
		Revealed Comparative Advantage Variation
		Cai (2008) "Towards a more general measure of revealed comparative advantage variation", Applied Economics Letters, 15:9, 723-726

		STATUS: NEEDS TESTING

		A more general measure of revealed comparative advantage variation.

		Parameters
		----------
		years 	: 	Tuple(start_year, future_year)
					Provide Comparison Years

		"""
		if type(years) != tuple:
			raise ValueError("Need to specify (base_year, future_year) in years tuple")
		else:
			base_year, future_year = years
		#-Compute Beta-#
		g = (self.ples[future_year].total_export - self.ples[base_year].total_export) / self.ples[base_year].total_export
		gk = (self.ples[future_year].total_product_export - self.ples[base_year].total_product_export).div(self.ples[base_year].total_product_export)
		cik = (self.ples[base_year].data["export"]).div(self.ples[base_year].total_country_export)
		beta = (1+g) / (1+(cik.mul(gk)).sum())
		#-RCAV-#
		rca_base = self.ples[base_year].rca_matrix(complete_data=complete_data)
		rca_future = self.ples[future_year].rca_matrix(complete_data=complete_data)
		self.rcav = rca_base - beta * rca_future
		if fillna:
			self.rcav = self.rcav.fillna(0.0)
		if return_intermediates:
			return self.rcav, g, gk, cik, beta
		return self.rcav


	##########################################################
	## -- ProductSpace: Analysis and Computation Methods -- ##
	##########################################################

	## - Iterators for PLES Functions -- ##
	#######################################

	## These are Constructors and therefore returning a property of the object may lead users to access these matrices by building them everytime!

	def mcp_matrices(self, years=None, cutoff=1.0, fillna=True, verbose=False):
		"""
			Compute Mcp Matrices for ProductLevelExportSystem
			Options:
			-------
				[1] years 		= list of years (Default: ALL)
		"""
		if years == None: years = self.years
		for year in years:
			if verbose: print "Computing Mcp matrix for year: %s" % year
			self.ples[year].mcp_matrix(cutoff, fillna, verbose) 				
		## -- Q: Should I Return Getter Method? -- ##
		#return self.mcp

	## -- Proximity Matrices -- ##

	def proximity_matrices(self, years=None, matrix_type='symmetric', clear_temp=True, fillna=False, verbose=False):
		"""
			Compute Mcp Matrices for ProductLevelExportSystem
			Options:
			-------
				[1] years 		= list of years (Default: ALL)
		"""	
		# - Disabled MultiCore => No Significant Performance Boost (Overheads ~= Performance Gain) for this type of Matrix - #
			# if self.multicore == True:
			# 	return self.multicore_proximity_matrices(years=years, verbose=verbose)
		return self.serial_proximity_matrices(years, matrix_type, clear_temp, fillna, verbose)
		
	def serial_proximity_matrices(self, years=None, matrix_type='symmetric', clear_temp=True, fillna=False, verbose=False):
		"""
			Compute Mcp Matrices for ProductLevelExportSystem
			Options:
			-------
				[1] years 		= list of years (Default: ALL)
		"""	

		if years == None: years = self.years
		for year in years:
			if verbose: print "Computing Mcp matrix for year: %s" % year
			self.ples[year].compute_proximity(matrix_type, clear_temp, fillna, verbose)
		## -- Q: Should I Return Getter Method? -- ##
		#return self.proximity

	def multicore_proximity_matrices(self, years=None, verbose=True):
		"""
			Multicore Implimentation of Proximity Matrices

			**Note:** This will only compute the proximity_matrix with default kwargs due to requirement to pickle objects!

			Options: 
			--------
				[1] years 		= list of years (Default: ALL)

			Notes:
			-----
				[1] Requires ipcluster start -n 4 (ipython notebook)
				[2] No significant performance boost. Overheads ~= Performance Gain
		"""
		## -- User Warnings & Reminders -- ##
		print "[REMINDER] This will only compute the proximity_matrix with default kwargs due to requirement to pickle objects!"

		## -- Setup MultiCore Client -- ##
		from IPython.parallel import Client
		c = Client()

		if years == None: years = sorted(self.years) 				# - Should be sorted but make sure - #
		# - Setup an Execution List to preserve year ordering - #
		ples_list = []
		for year in years:
			ples_list.append(self.ples[year])
		
		# - Compute Across Cluster - #
		results = c[:].map_sync(lambda x: x.proximity_matrix(), ples_list)
		
		# # - Assign Results - #
		year = years[0]
		for result in results:
			self.ples[year].proximity = result
			year += 1
		# - Should this return the getter method? - #
		#return self.proximity

	## -- Centrality Measures -- ##
	def compute_average_centrality(self, normalized=True, sum_not_mean=False, verbose=False):
		"""
			Compute Average Centrality from Mcp and Proximity matrices
			
			Status: IN-TESTING
			
			Parameters
			----------
			normalized 	: 	bool, optional(default=True)
							Normalize by the Total Number of Products; if False the denominator is the number of products exported by that country
			sum_not_mean : 	bool, optional(default=False)
							Sum's the mean proximity multiplied by country export basket

			Notes
			----- 
				1. sum_not_mean = Haussman uses SUM() rather than normalised mean -> Same Overall Graph Shape!
			
			Return
			------
			avg_centrality 
		"""
		avg_centrality = dict()  
		for year in self.years:
			avg_centrality[year] = self.ples[year].compute_average_centrality(normalized, sum_not_mean, verbose)
		return avg_centrality


	## -- Ubiquity and Diversity -- ##

	def compute_ubiquity(self, years=None, verbose=False):
		"""
			Compute Product Ubiquity for all ProductLevelExportSystem's
			Options:
			-------
				[1] years 		= list of years (Default: ALL)
		"""
		if years == None: years = self.years
		for year in years:
			if verbose: print "Computing Product Ubiquity for year: %s" % year
			self.ples[year].compute_ubiquity(verbose)
		## -- Q: Should I Return Getter Method? -- ##
		#return self.ubiquity

	def compute_diversity(self, years=None, verbose=False):
		"""
			Compute Country Diversity for all ProductLevelExportSystem's
			Options:
			-------
				[1] years 		= list of years (Default: ALL)
		"""
		if years == None: years = self.years
		for year in years:
			if verbose: print "Computing Country Diversity for year: %s" % year
			self.ples[year].compute_diversity(verbose)
		## -- Q: Should I Return Getter Method? -- ##
		#return self.diversity

	## -- ECI / PCI -- ##

	def compute_eci(self, years=None, verbose=False):
		"""
			Compute Country Complexity Indicator
			Notes:
			-----
				[1] Multicore with 4 Cores ~55 percent performance gain
		"""
		if self.multicore == True:
			if verbose: print "Running MultiCore Method"
			return self.multicore_compute_eci(years, verbose)
		if verbose: print "Running Single Process Method"
		return self.serial_compute_eci(years, verbose)

	def serial_compute_eci(self, years=None, verbose=False):
		"""
			Compute Country Complexity Indicator for all PLES (Serial)
			Options:
			-------
				[1] years 		= list of years (Default: ALL)
		"""
		if years == None: years = self.years
		for year in years:
			if verbose: print "Computing Country Complexity Indicator for year: %s" % year
			self.ples[year].compute_eci(verbose)

	def multicore_compute_eci(self, years=None, verbose=False):
		"""
			Compute Country Complexity Indicator for all PLES (Parallel Computing)
			Options:
			-------
				[1] years 		= list of years (Default: ALL)
		"""
		## -- Setup MultiCore Client -- ##
		from IPython.parallel import Client
		c = Client()

		if years == None: years = sorted(self.years) 			# - Should be sorted but make sure - #
		# - Setup an Execution List to preserve year ordering - #
		ples_list = []
		for year in years:
			ples_list.append(self.ples[year])
		
		# - Compute Across Cluster - #
		results = c[:].map_sync(lambda x: x.compute_eci(), ples_list)
		
		# - Assign Results - #
		year = years[0]
		for result in results:
			self.ples[year].eci = result
			year += 1
		# - Should this return? - #
		#return self.eci

	def compute_pci(self, years=None, verbose=False):
		"""
			Compute Product Complexity Indicator
			Notes:
			-----
				[1] Multicore with 4 Cores ~55 percent performance gain
		"""
		if self.multicore == True:
			if verbose: print "Running MultiCore Method"
			return self.multicore_compute_pci(years, verbose)
		if verbose: print "Running Single Process Method"
		return self.serial_compute_pci(years, verbose)

	def serial_compute_pci(self, years=None, verbose=False):
		"""
			Compute Product Complexity Indicator for all PLES (Serial)
			Options:
			-------
				[1] years 		= list of years (Default: ALL)
		"""
		if years == None: years = self.years
		for year in years:
			if verbose: print "Computing Product Complexity Indicator for year: %s" % year
			self.ples[year].compute_pci(verbose)

	def multicore_compute_pci(self, years=None, verbose=False):
		"""
			Compute Product Complexity Indicator for all PLES (Parallel Computing)
			Options:
			-------
				[1] years 		= list of years (Default: ALL)
		"""
		## -- Setup MultiCore Client -- ##
		from IPython.parallel import Client
		c = Client()

		if years == None: years = sorted(self.years) 			# - Should be sorted but make sure - #
		# - Setup an Execution List to preserve year ordering - #
		ples_list = []
		for year in years:
			ples_list.append(self.ples[year])
		
		# - Compute Across Cluster - #
		results = c[:].map_sync(lambda x: x.compute_pci(), ples_list)
		
		# - Assign Results - #
		year = years[0]
		for result in results:
			self.ples[year].pci = result
			year += 1
		# - Should This return? - #
		#return self.pci

	## -- Adjustment Function for ECI/PCI -- ##

	def auto_adjust_eci_sign(self, cntry_datum=('DEU', '+ve'), verbose=False):
		"""
			Auto Adjust ECI computations based on a Country Datum
			Convention: +ve is higher complexity

			Options:
			--------
				cntry_datum 	: 	Tuple(ISO3C, '+ve' or '-ve') [Default: ('DEU', '+ve')]

			Future Work:
			-----------
				1. Check if Country is in the dataset.
				2. Added to ProductLevelExportSystem so could rework this function to use that in each PLES 
		"""
		if verbose: print "Applying .auto_adjust to ECI sign"
		cntry, sign = cntry_datum
		for year in self.years:
			# - Error Check the Country - #
			if cntry not in self.ples[year].countries:
				raise ValueError("(%s) not found in Country List for year: %s") % (cntry, year)
			# - Correct Signs using Datum - #
			if sign == '+ve':
				if self.ples[year].eci[cntry] > 0:
					pass
				else:
					if verbose: print "Switching sign of Year: %s" % year 
					self.ples[year].eci = self.ples[year].eci * -1
			elif sign == '-ve':
				if self.ples[year].eci[cntry] < 0:
					pass
				else:
					if verbose: print "Switching sign of Year: %s" % year 
					self.ples[year].eci = self.ples[year].eci * -1
			else:
				raise ValueError("sign must be either '+ve' or '-ve'")

	def adjust_eci_sign(self, years, verbose=False):
		"""
			Adjust ECI for sign (due to current inconsistencies returned from compute_eci())

			Future Work:
			-----------
				[1] Figure out why compute_eci() produces inconsistencies in year ranks (by sign)
				[2] Adjust Automatically - Check Highest and Lowest Ranked Country in first PLES and ensure +ve and -ve throughout the years [Assuming no LARGE deviations in RANK]
		"""
		for year in years:
			self.ples[year].eci = self.ples[year].eci * -1

	
	def auto_adjust_pci_sign(self, product_datum=('3330', '-ve'), verbose=False):
		"""
			Auto Adjust PCI computations based on a Product Datum
			Convention: +ve is higher complexity

			Options:
			--------
				product_datum 	: 	Tuple(SITCR2L4, '+ve' or '-ve') [Default: ('3330', '-ve')]

			Notes:
			-----
				1. current default product_datum is using SITCR2L4!
		"""
		if verbose: print "Applying .auto_adjust to PCI signs"
		productcode, sign = product_datum
		for year in self.years:
			# - Error Check the Productcode - #
			if productcode not in self.ples[year].products:
				raise ValueError("(%s) is not found in the Product List for year: %s") % (productcode, year)
			# - Correct Signs using datum - #
			if sign == '+ve':
				if self.ples[year].pci[productcode] > 0:
					pass
				else:
					if verbose: print "Switching sign of Year: %s" % year 
					self.ples[year].pci = self.ples[year].pci * -1
			elif sign == '-ve':
				if self.ples[year].pci[productcode] < 0:
					pass
				else:
					if verbose: print "Switching sign of Year: %s" % year 
					self.ples[year].pci = self.ples[year].pci * -1
			else:
				raise ValueError("sign must be either '+ve' or '-ve'")


	def adjust_pci_sign(self, years, verbose=False):
		"""
			Adjust ECI for sign (due to current inconsistencies returned from compute_eci())

			Future Work:
			-----------
				[1] Figure out why compute_eci() produces inconsistencies in year ranks (by sign) [Assuming no LARGE deviations in RANK]
		""" 
		for year in years:
			self.ples[year].pci = self.ples[year].pci * -1

	### --- ProductSpace: Panel Set Functions --- ###
	################################################

	def set_single_proximity_matrix(self, prox_matrix, verbose=False):
		"""
			Set a Single Proximity Matrix in ALL Cross-Sections
			Future Work:
			-----------
				[1] Develop some Futher Checks on incoming prox_matrix like checking index names etc?
		"""
		if type(prox_matrix) == pd.DataFrame: 				# Data has been passed through: Assumed Well Formed!
			use_prox = prox_matrix
		elif re.match('[0-9]{4}', prox_matrix): 			# Year Has Been Specified
			use_prox = A[prox_matrix].proximity
		elif prox_matrix == 'average':
			if verbose: print "Computing an Average Proximity Matrix across ALL years in DynamicProductLevelExportSystem"
			prox_matrix = self.get_proximity(rtype='wide').mean(axis=1).unstack(level='productcode2') 				#Note: mean(1.0 + np.nan) = 1.0
			use_prox = prox_matrix
		else:
			raise ValueError("prox_matrix must be of type pd.DataFrame, year or 'average'")					
		# - Populate Data Structures - #
		for year in self.years:
			self[year].proximity = use_prox 			# Note: This might be turned into an immutable and may need to change to set_proximity()
		return True
													

	#########################
	## -- Panel Methods -- ##	
	#########################

	def dynamic_global_panel(self, fillna=False, series_name='export', verbose=False):
		"""
		Ensure Global Panel For Dynamic Analysis
		
		Parameters
		-----------
		fillna 	: 	bool, optional(default=False)
					fill np.nan objects with 0
		series_name : 	str, optional(default='export')
						Provide Series Name

		Future Work
		----------- 
		1. Add inplace option?

		"""
		warnings.warn("[WARNING] This returns a new dynamic system ... not inplace", UserWarning) 
		data = self.get_data(rtype='wide')
		data = data.stack().unstack(level='country').stack(dropna=False).unstack('year')									#Wide Data Series
		data = data.stack().unstack(level='productcode').stack(dropna=False).unstack(level='year').stack(dropna=False) 		#Long Data Series
		data.name = series_name
		data = data.reset_index().set_index(keys='year') 	#Prepare Balanced Panel For from_df()
		if fillna: 
			data = data.fillna(0.0)
		# - Construct a New DynPLES - #
		DynPLES = DynamicProductLevelExportSystem()
		# - Carry Across Data Attributes - #
		DynPLES.product_classification = self.product_classification
		DynPLES.country_classification = self.country_classification 
		DynPLES.from_df(data)
		DynPLES.global_panel = True
		DynPLES.complete_trade_network = self.complete_trade_network
		return DynPLES


	def assign_data_to_matrixattr(self, data_name, years=None, verbose=True):
		"""
			Assign Object (ProductLevelExportSystem) Data to Object.matrix
			Useful for Smoothing Function which acts on matrix
		"""
		if years == None: years = self.years
		for year in years:
			if data_name == 'rca': 										#If store data in ProductLevelExportSystem self.matrix['rca'] then could build out ability to just pass in dataname!
				self.ples[year].matrix = self.ples[year].rca
			else:
				raise NotImplementedError("Only implimented for RCA")
			self.ples[year].matrix_notes = data_name
		return True


	###############################
	## -- Time-Series Methods -- ##	
	###############################

	def compute_smoothed_data(self, data_dict, dshape, smoother=(1,1,1), rtype='dict', dropna=False, verbose=False):
		"""	
			Smoothing Function that takes in a Property and returns smoothed version of the data

			Input:
			-----
				[1] data  		=> 	Data Matrix or Series (i.e. self.data, self.rca etc.)
				
			Options:
			-------
				[1]	smoother 	=> 	Smoother Tuple (pre-period, cur-period, post-period) [Default: 3YRMA (1,1,1)]

			Future Work:
			-----------
				[1] Write a Super Conversion method that detects incoming data arrangement and converts to any combination requested. 
		"""
		# - Setup Defaults - #
		if dshape == 'cp': 
			selection=['country', 'productcode']
			idx_order = ['year', 'country', 'productcode']
		elif dshape == 'pp': 
			selection=['productcode1', 'productcode2']
			idx_order = ['year', 'productcode1', 'productcode2']
		else:
			raise ValueError("dshape needs to be 'cp' or 'pp'")
		# - Parse Smoother - #
		(pre_periods, cur_period, post_periods) = smoother
		if verbose: 
			print "Smoothing: %s-year moving average" % sum(smoother)
			print "Previous Years: %s" % pre_periods
			print "Future Years: %s" % post_periods
		years = sorted(data_dict.keys()) 								# Could also use self.years?
		if type(data_dict[years[0]]) == pd.DataFrame:
			# - Convert Data to Long Format - #
			long_data = self.reshape_data(data_dict, datashape=dshape, rshape='long') 
			window = sum(smoother)
			#data = data.groupby(level=selection).apply(lambda x: pd.rolling_mean(x, window=window, center=True))
			data = pd.rolling_mean(long_data.unstack(level=selection), window=window, center=True).stack(level=selection, dropna=dropna)  # This Delivers 10 x the Performance
			# - Remove begining and end np.nan years - #
			years = data.index.levels[0]          				# - data is indexed by ('year', 'country', 'productcode')
			keep_yrs = years[pre_periods:post_periods*-1]
			exclude_yrs =  list(set(years) - set(keep_yrs))
			data = data.unstack(level='year').stack(level=0, dropna=False) 	# Level 0 is the SeriesName #
			data.drop(labels=exclude_yrs, axis=1, inplace=True)
			data = data.unstack(level=-1).stack(level='year', dropna=False).reorder_levels(order=idx_order).sort_index()
			data.name = data_dict[years[0]].name + " [Smoothed=(%s,%s,%s)]" % smoother
			data.columns = [data.name]
			# - Return it to the Incoming Shape - #
			if rtype == 'long':
				return data
			else:
				return self.long_data_to(data, rshape=dshape, rtype=rtype, verbose=verbose)
		else:
			raise NotImplementedError

	def compute_smoothed_trade_data(self, smoother=(1,1,1), method='pandas', data_name='self.data', years=None, verbose=False):
		"""
			Compute Smoothed Trade Data (i.e. 3YRMA) and Return a New DynamicProductLevelExportSystem
			Note: This function only acts on self.data
			Assumptions: Centering of Smoother = True

			Input:
			-----
				[1] smoother 	=> 	Smoother Tuple (pre-period, cur-period, post-period). 
									(1,1,1) = [D(t-1) + D(t) + D(t+1)] * 1/3 [Default: 3YRMA]
			
			Options:
			--------
				[1] method 		=> 	'pandas' : Leverage Pandas 
									'numpy'  : <possible if Pandas is slow (Currently NotImplemented)
				[2] data_name 	=> 	'data' : Able to Smooth Core Export Data in self.data
									'matrix' : Able to compute any Wide DataFrame (i.e. matrix)

			Notes:
			------
				[1] This will return DynamicProductLevelExportSystem that are build on self.data and self.matrix. 
				How will Country and Product Objects be treated?  => split the from_df() function into a simple from_df() to self.data and construct_network()
				Then Networks can be constructed from self.data and Node and Edge Objects
				[2] This function was written when originally thinking about that data structure of the object to be self.data, self.matrix
		"""

		## -- Parse Options -- ##
		if years == None: years = self.years
		(pre_periods, cur_period, post_periods) = smoother
		print "[NOTICE] Currently this function ONLY does centered smoothers (i.e. (1,1,0)==(0,1,1)==(1,0,1) ALL EQUAL TO (0,1,1))\nValid Smoothers: (1,1,1), (2,1,2) etc"
		if verbose: 
			print "Smoothing: %s-year moving average" % sum(smoother)
			print "Previous Years: %s" % pre_periods
			print "Future Years: %s" % post_periods
		## -- Compute -- ##
		if method == 'pandas':
			# - Core Data - #
			if data_name == 'self.data':
				data = self.get_data(rtype='long')
				selection = ['country', 'productcode']
				window = sum(smoother)
				data = data.groupby(level=selection).apply(lambda x: pd.rolling_mean(x, window=window, center=True)) # Raw=True prevents series generation but doesn't work for groupby objects
				#data = pd.rolling_mean(data.unstack(level=['country', 'productcode']), window=3, center=True).stack(level=['country', 'productcode'])  # This Delivers 10 x the Performance
				# - Remove begining and end np.nan years - #
				years = data.index.levels[0]          				# - data is indexed by ('year', 'country', 'productcode')
				years = years[pre_periods:post_periods*-1]
				idx = zip(['export']*len(years), years)
				data = data.unstack(level='year')[idx].stack().reorder_levels(order=['year', 'country', 'productcode']).sort_index()
				# - Construct return system - #
				dynples = DynamicProductLevelExportSystem()
				dynples.from_df(data.reset_index().set_index(keys=['year'])) 
				# - Carry Over Attributes - #
				dynples.country_classification = self.country_classification
				dynples.product_classification = self.product_classification
				dynples.complete_trade_network = self.complete_trade_network	
				return dynples
			# - Core Matrix - #
			elif data_name == 'self.matrix':                        # - Currently this is written for RCA MATRIX! - #
				matrix = self.get_matrix(rtype='long')
				selection = ['country', 'productcode']
				window = sum(smoother)
				matrix = matrix.groupby(level=selection).apply(lambda x: pd.rolling_mean(x, window=window, center=True))
				return DynamicProductLevelExportSystem().from_rca_df(data.reset_index().set_index(keys=['year']), rca_notes='Smoothed by Smoother: %s' % smoother) 				#Return New TradeSystem with Smoothed RCA Matrices
			else:
				raise ValueError("method has only been constructed for self.data and self.matrix")
		else:
			raise ValueError("Only method implimented leverages pandas. Specify method='pandas'")

	def compute_intertemporal_fill(self, interpolate=True, ffill=True, ffill_limit=1, bfill=True, bfill_limit=1, verbose=False):
		"""
			Compute Data with Intertemporal Fill and Return a New DynamicProductLevelExportSystem with new data
			
			** Status: NEEDS TESTING **

			Options:
			-------
				interpolate 	: 	Interpolate between gaps within the time series
				ffill 			: 	Forward fill the end of time-series gaps with the last value
				ffill_limit 	: 	Limit the number of periods for ffill
				bfill 			: 	Backward fill the begining of the time-series with the first value
				bfill_limit 	: 	Limit the number of periods for bfill

			Note: 
			-----
				[1] This is useful when using the current compute_smoothed_data() function as it requires all cells to include data to compute which creates intertemporal gaps
				[2] Return basic DynamicProductLevelExportSystem based only on the Default 'DataFrame' data structure. If network representations desired in the new DynPLES then will need to construct them using appropriate constructor methods
		"""
		# - Obtain Long Form Data to Leverage Pandas - #
		data = self.get_data(rtype='long', sort=True)
		if interpolate:
			data.apply(pd.Series.interpolate)
		if ffill == True:
			data.fillna(method='ffill', limit=ffill_limit)
		if bfill == True:
			data.fillna(method='bfill', limit=bfill_limit)
		# - Construct DynPLES - #
		DynPLES = DynamicProductLevelExportSystem()
		DynPLES.from_df(data)
		return DynPLES

	# - Return Averaged Data - #
	############################

	def compute_average(self, data, start_year=None, end_year=None, step=1, verbose=False):
		"""
			Compute and Return Some Averaged Data as a pd.DataFrame
			[Important: This method is sensative to np.nan (i.e. 1 + np.nan = np.nan)]

			Options:
			-------
				start_year 		: 	year to start averaging from 	[Default: Start Year Found in Data]
				end_year 		: 	year to end averaging on 		[Default: End Year Found in Data]
				step 			: 	step in years
			
			Notes:
			-----
				[1] df_data (i.e. A.data or A.proximity etc) will come in as a Dict(year : Data to Average)
		"""
		years = sorted(data.keys())
		# - Set Defaults - #
		if type(start_year) != int: start_year = years[0]
		if type(end_year) != int: end_year = years[-1]
		# - Check Valid Year Request - #
		if start_year not in years: raise ValueError("Start Year: %s is not valid. Earliest Year in Data is: %s" % (start_year, years[0]))
		if end_year not in years: raise ValueError("End Year: %s is not valid. Latest Year in Data is %s" % (end_year, years[-1]))
		if verbose: print "Computing Averaging for Passed Data between Year: %s and %s" % (start_year, end_year)
		# - Averaging - #
		averaging_years = range(start_year, end_year+1, step)
		num_years = len(averaging_years)
		avg_data = data[averaging_years[0]]
		for year in averaging_years[1:]:
			avg_data = avg_data + data[year]
		avg_data = avg_data / num_years
		return avg_data

	##########################
	## -- Change Methods -- ##
	##########################

	## -- Products -- ##
	####################

	def compute_product_changes(self, verbose=False):
		""" Compute Changes in Products Year to Year """
		# - Check Required Data is Computed - #
		if type(self.mcp) != dict:
			print "[NOTICE] Mcp matrix at (self.mcp) is currently not available. Computing Mcp with default kwargs"
			self.mcp_matrices()
		# - Construct Return Containers - #
		BothYears = dict()
		NewProducts = dict()
		DieProducts = dict()
		# - Comptue t and t+1 dynamics - #
		for base_year in self.years:
			if base_year == self.years[-1]: break 													#Last Year
			change_key = str(base_year) + '-' + str(base_year + 1)
			BothYears[change_key] = self[base_year].mcp * self[base_year + 1].mcp
			BothYears[change_key].name = 'ProductsBothYears'
			NewProducts[change_key] = self[base_year + 1].mcp - BothYears[change_key] 				#Could also use self[base_year + 1].mcp - self[base_year].mcp
			NewProducts[change_key].name = 'NewProducts'
			DieProducts[change_key] = self[base_year].mcp - BothYears[change_key] 					#Could also use self[base_year].mcp - self[base_year + 1].mcp
			DieProducts[change_key].name = 'DieProducts'
		return BothYears, NewProducts, DieProducts


	#########################################
	## -- Research Analytical Functions -- ##
	## -- Perhaps these Functions should be in their respective papers as functions? - #
	#########################################

	def compute_probable_improbable_emergence(self, prox_cutoff='median', style='average', output='summary', verbose=False):
		"""
		Compute the Probable and Improbable Emergence of Products

		Parameters
		----------
		prox_cutoff : 	str or numeric, optioanl(default='median')
						Specify a proximity cutoff value (i.e. 0.24 if desired), other options are 'mean' or 'median'
		style 		: 	str, optional(default='average')
						Specify how to treat proximity ('average': average of both years, 'base': use Base Year, 'next': use Next Year)
		output 		: 	str, optional(default='summary')
						Specify what type of output is to be returned
						'summary': Mc_ProbableProducts, Mc_ImProbableProducts
						'reduced': Mcp_ProbableProducts, Mcp_ImProbableProducts
						'extended': Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts, Mcp_ProbableProducts, Mcp_ImProbableProducts		
		
		Dependancies
		------------
		1. compute_product_changes()

		Notes
		-----
			1. These should probably be kept as separate functions as research specific functions? Include in the Class for now
			2. No Persistence is Modelled - Kept as a separate Function (compute_persistence())

		.. 	Future Work        
			------------
			1. Refactor Code into Smaller Functions
			2. Compare Results using NBER ONLY DataFile with Previously Computed Results

		"""
		# - Check Required Data - #
		if not self.global_panel:
			raise ValueError("DynamicProductLevelExportSystem needs to be a Global Dynamic Panel (with the same c x p matrix sizes)")
		if type(self.proximity) != dict: 																								#Assuming filled with pd.DataFrames
			print "[NOTICE] Proximity matrix at (self.proximity) is currently not available. Computing Proximity with default kwargs"
			self.proximity_matrices()

		## -- Proximity matrices for a Balanced Panel NOT WORKING! -- ##

		# - Parse Options (Preserve Non-Float State of Prox_Cutoff Income to Compute Mean/Median per Year Panel) - #
		if type(prox_cutoff) == str:
			prox_cutoff_option = prox_cutoff        #Save Incoming result of prox_cutoff
		else:
			prox_cutoff_option = ''

		# - Compute Dynamics (Two-Period) - #
		Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts = self.compute_product_changes(verbose=verbose)  
		
		# - Classify New Products - #
		year_transitions = Mcp_NewProducts.keys()
		# - Return Containers - #
		Mcp_ProbableProducts = dict()
		Mcp_ImProbableProducts = dict()
		if str(output).lower() == 'summary':
			Mc_ProbableProducts = dict()
			Mc_ImProbableProducts = dict()
		
		# - Compute Data - #
		for years in sorted(year_transitions):                        #Need to run through the ordered pairs
			# - Initialise Variables - #
			NewProducts = Mcp_NewProducts[years]
			base_year, next_year = [int(x) for x in years.split('-')]
			Prox_BaseYear = self.proximity[base_year]
			Prox_NextYear = self.proximity[next_year]
			if verbose: print "Years: %s; Base_Year: %s, Next_Year: %s" % (years, base_year, next_year)
			countries = Mcp_BothYears[years].index
			products = Mcp_BothYears[years].columns
			# - Decide prox_cutoff - #
			if str(prox_cutoff_option).lower() == 'median':
				prox_series1 = Prox_BaseYear.unstack()
				prox_series2 = Prox_NextYear.unstack()
				joint_series = prox_series1.append(prox_series2)
				prox_cutoff = (joint_series[joint_series != 0.0].median())      #Remove Null Information. Perhaps Should have been NAN when computing Proximity!
			elif str(prox_cutoff_option).lower() == 'mean':
				prox_series1 = Prox_BaseYear.unstack()
				prox_series2 = Prox_NextYear.unstack()
				joint_series = prox_series1.append(prox_series2)
				prox_cutoff = (joint_series[joint_series != 0.0].mean())        #Remove Null Information. Perhaps Should have been NAN when computing Proximity!
			elif type(prox_cutoff) == float:
				pass
			else:
				raise ValueError("ERROR: prox_cutoff needs to be Float, or 'Mean' / 'Median'")
			if verbose: print "Using Prox_Cutoff Value: %s" % prox_cutoff
			# - Initialise Return Structures - #
			ProbProducts = pd.DataFrame(np.zeros((len(countries), len(products))),  index=countries, columns=products)
			ImProbProducts = pd.DataFrame(np.zeros((len(countries), len(products))), index=countries, columns=products)
			for country in countries:                                   #Loop through Countries
				new_country_products = NewProducts.ix[country]
				base_country_products = self.mcp[base_year].ix[country]
				connections = dict()
				# - Find Product Relationships - #
				for productcode, value in new_country_products.iteritems():                     #Loop through Products
					product_pairs = list()
					if value == 1.0:                                                            #Succesful New Product
						for base_code, base_value in base_country_products.iteritems():         #Find Connections with all previous_year products
							if base_value == 1.0: 
								product_pairs.append(base_code)
					if product_pairs == []:                                                       #Don't Catalogue Blank Connections List
						continue
					connections[productcode] = product_pairs
				# - Find Proximity Values associated with ProductPairs and Classify Product - #
				for new_export in connections.keys():                       #Go through New Country Exports
					num_improbable_connections = 0
					num_probable_connections = 0
					for pair in connections[new_export]:                      #List of Pairs
						# - Decide what proximity to use  - #
						if style.lower() == 'base':
							prox = Prox_BaseYear.get_value(index=new_export, col=pair)
						elif style.lower() == 'next':
							prox = Prox_NextYear.get_value(index=new_export, col=pair)
						elif style.lower() == 'average':
							prox = (Prox_BaseYear.get_value(index=new_export, col=pair) + Prox_NextYear.get_value(index=new_export, col=pair)) / 2.0
						else:
							raise ValueError("ERROR: Need to Specify style as: 'base', 'next', or 'average'")
						# - Decide How to Classify product_pair - #
						if prox <= prox_cutoff:                        # Product Emergence is Improbable, This needs to consider List of Conections and if ANY are above then assign to Vector; 12/06/2013 changed from <= to <
							num_improbable_connections += 1
						else:
							num_probable_connections += 1
					# - For new_export classify based on aggregate of connection strength - #
					if num_probable_connections > 0:                                        # If ANY Connections are Probable, New_Export emerges near current production; Could improve this by returning b/c only need 1 connections close                
						ProbProducts.set_value(index=country, col=new_export, value=1.0)
					else:                                                                   # If None of the Connections are Probable then Classify New_Product as Improbable
						ImProbProducts.set_value(index=country, col=new_export, value=1.0)
		
			# - Add Results into Dict() - #
			Mcp_ProbableProducts[next_year] = ProbProducts                   #Dict(Probable Product in 'cp' layout), Taged to next_year rather than transition. 
			Mcp_ProbableProducts[next_year].name = 'ProbProducts'           
			Mcp_ImProbableProducts[next_year] = ImProbProducts               #Dict(ImProbable Product in 'cp' layout)
			Mcp_ImProbableProducts[next_year].name = 'ImProbProducts'        
			# - Return Options - #
			if str(output).lower() == 'summary':
				Mc_ProbableProducts[next_year] = ProbProducts.sum(axis=1)
				Mc_ProbableProducts[next_year].name = 'c_ProbProducts'
				Mc_ImProbableProducts[next_year] = ImProbProducts.sum(axis=1)
				Mc_ImProbableProducts[next_year].name = 'c_ImProbProducts'
		if str(output).lower() == 'summary':
			return Mc_ProbableProducts, Mc_ImProbableProducts
		elif str(output).lower() == 'reduced':
			return Mcp_ProbableProducts, Mcp_ImProbableProducts
		else:
			return Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts, Mcp_ProbableProducts, Mcp_ImProbableProducts

### --- WORKING HERE --- ####

 # def compute_probable_improbable_emergence_nx(mcp, proximity, prox_cutoff='median', style='average', output='summary', verbose=False):
#     """ Generate Probable and Improbable Emergence using the Networkx library
#         Status: Validated  [But Slow compared to Pandas Implimentation]
#         Options:    style       ->      Considers how to treat proximity ('average': average of both years, 'base': use Base Year, 'next': use Next Year)
#                     output      ->      'reduced':  returns only Prob_cp and ImProb_cp
#                                         'extended': returns additional measures of Mcp_BothYears, Mcp_NewProducts, and McpDieProducts
#                                         'summary':  returns only Prob_c and ImProb_c
#                                         'list':     returns list of products in Mcp Format.  
#                     prox_cutoff ->      Can specify a value (i.e. 0.24 if desired), other options are 'mean' or 'median'
#         Input: Dict(mcp), and Dict(proximity) + Options
#         Notes:      This routine could be greatly improved by returning if ANY close proximity item is found
#                     07/07/2013 -> This function and compute_probable_improbable_emergence() now agree! Pearson's of 1 for both Vectors
#     """
#     #Parse Option Flags
#     if type(prox_cutoff) == str:
#         prox_cutoff_option = prox_cutoff        #Save Incoming result of prox_cutoff
#     else:
#         prox_cutoff_option = ''
#     # Initialise Return Containers
#     prob_emerg = dict()                 #For 'list' option
#     improb_emerg = dict()
#     Mcp_ProbableProducts = dict()        #For all other output options
#     Mcp_ImProbableProducts = dict()
#     Mc_ProbableProducts = dict()
#     Mc_ImProbableProducts = dict()
#     # Compute Difference Matrices #
#     Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts = compute_product_changes(mcp)
#     # Classify New Products into Probable and Improbable
#     for years in sorted(Mcp_NewProducts.keys()):
#         # Define Base Year and Next Year
#         base_year, next_year = [int(x) for x in years.split('-')]
#         if verbose: print "Years: %s; Base_Year: %s, Next_Year: %s" % (years, base_year, next_year)
#         Prox_BaseYear = proximity[base_year]
#         Prox_NextYear = proximity[next_year]
#         #Init Return Dict() for 'list' option
#         prob_emerg[next_year] = dict()
#         improb_emerg[next_year] = dict()
#         #Initalise Return DataFrame structures (for other options)
#         countries = Mcp_BothYears[years].index
#         products = Mcp_BothYears[years].columns
#         ProbProducts = pd.DataFrame(np.zeros((len(countries), len(products))),  index=countries, columns=products)
#         ImProbProducts = pd.DataFrame(np.zeros((len(countries), len(products))), index=countries, columns=products)
#         #Decide prox_cutoff
#         if str(prox_cutoff_option).lower() == 'median':
#             prox_series1 = Prox_BaseYear.unstack()
#             prox_series2 = Prox_NextYear.unstack()
#             joint_series = prox_series1.append(prox_series2)
#             prox_cutoff = (joint_series[joint_series != 0.0].median())      #Remove Null Information. Perhaps Should have been NAN when computing Proximity!
#         elif str(prox_cutoff_option).lower() == 'mean':
#             prox_series1 = Prox_BaseYear.unstack()
#             prox_series2 = Prox_NextYear.unstack()
#             joint_series = prox_series1.append(prox_series2)
#             prox_cutoff = (joint_series[joint_series != 0.0].mean())        #Remove Null Information. Perhaps Should have been NAN when computing Proximity!
#         elif type(prox_cutoff) == float:
#             pass
#         else:
#             print "ERROR: prox_cutoff needs to be Float, or 'Mean' / 'Median'"
#             return None
#         if verbose: print "Using Prox_Cutoff Value: %s" % prox_cutoff
#         # Construct Network From Proximity
#         #Decide what proximity to use                                                                   #Added 06/07/2013 to allow selection of proximity. CHECK (DataFrame * DataFrame / 2.0) works as expected.
#         if style.lower() == 'base':
#             prox = Prox_BaseYear
#         elif style.lower() == 'next':
#             prox = Prox_NextYear
#         elif style.lower() == 'average':
#             prox = (Prox_BaseYear + Prox_NextYear) / 2.0
#         else:
#             print "ERROR: Need to Specify style as: 'base', 'next', or 'average'"
#             return None
#         prox.name = 'proximity'
#         network = construct_network_from_adjacency_df(prox)   #Currently chose base_year but need to create the right dataframe during the prox_cutoff stage; Changed proximity[base_year] to prox from style selection
#         #Classify Products Based on Network
#         for country in Mcp_NewProducts[years].index:            #Iterate through Countries in NewProducts
#             #Initialise Country Data List (for 'list') return
#             prob_emerg[next_year][country] = []
#             improb_emerg[next_year][country] = []
#             last_year_exports = pd.DataFrame(mcp[base_year].ix[country])  #DataFrame Vector of Last Years Exports
#             for product in Mcp_NewProducts[years].columns:      #Iterate through Products  in NewProducts
#                 if Mcp_NewProducts[years].get_value(index=country, col=product) == 1:
#                     # Initialise Counters
#                     num_probable_connections = 0
#                     num_improb_connections = 0
#                     if last_year_exports.sum() == 0:    #Don't Classify if last years export vector all = 0 (didn't export anything)
#                         continue
#                     for prev_product in last_year_exports.iterrows():
#                         if prev_product[1][country] == 1.0:                         #Need to only connect with those that it exported!
#                             proximity_val = network.get_edge_data(product, prev_product[0])['weight']  #Provides Proximity Between New Product and Previous Product
#                             if proximity_val > prox_cutoff:    #12/06/2013 -> Changed from >= to > to match stata and other implimentation
#                                 num_probable_connections += 1
#                             else:
#                                 num_improb_connections += 1 
#                     #Classify based on aggregate connections
#                     if num_probable_connections > 0:                                      #Swapped these so that if a country dosesn't export anything in previous year it won't be classified as improbale b/c both improb and prob == 0. In this case they can classified as probable
#                         prob_emerg[next_year][country].append(product)
#                         ProbProducts.set_value(index=country, col=product, value=1)                       
#                     else:
#                         improb_emerg[next_year][country].append(product)
#                         ImProbProducts.set_value(index=country, col=product, value=1) 
#         #Add Results into Dict()
#         Mcp_ProbableProducts[next_year] = ProbProducts                   #Dict(Probable Product in 'cp' layout), Taged to next_year rather than transition. 
#         Mcp_ProbableProducts[next_year].name = 'ProbProducts'            #Name the DF -> Added: 31/05/2013
#         Mcp_ImProbableProducts[next_year] = ImProbProducts               #Dict(ImProbable Product in 'cp' layout)
#         Mcp_ImProbableProducts[next_year].name = 'ImProbProducts'
#         Mc_ProbableProducts[next_year] = ProbProducts.sum(axis=1)
#         Mc_ProbableProducts[next_year].name = 'c_ProbProducts'
#         Mc_ImProbableProducts[next_year] = ImProbProducts.sum(axis=1)
#         Mc_ImProbableProducts[next_year].name = 'c_ImProbProducts' 
#     #Create DataFrame from Dict() of Dict()
#     if str(output).lower() == 'list':
#         probable_products = pd.DataFrame(prob_emerg)  #Add Label Values
#         improbable_products = pd.DataFrame(improb_emerg)
#         return probable_products, improbable_products
#     elif str(output).lower() == 'summary':
#         return Mc_ProbableProducts, Mc_ImProbableProducts
#     elif str(output).lower() == 'reduced':
#         return Mcp_ProbableProducts, Mcp_ImProbableProducts
#     else:
#         return Mcp_BothYears, Mcp_NewProducts, Mcp_DieProducts, Mcp_ProbableProducts, Mcp_ImProbableProducts

### --- END WORKING HERE --- ####

	###############################################
	## -- Network Diffusion Analysis Methods  -- ##
	## -- Should this be in DynPLES.Diffusion -- ##
	###############################################

	## -- Analytical Constructors -- ##
	###################################

### --- WORKING HERE --- ####

	def construct_cp_emergence_graph(self, verbose=False):
		"""
			Construct a Country x Product Emergence Graphs

			Algorithm Details:
			-----------------
				[1] A Product can only emerge once and the first emergent event is recorded!

			Options:
			-------
				[1] Allow Construction for a Single Country and then build off that function for other countries!


		"""
		raise NotImplementedError


 # def generate_country_product_emergence_graph(df, tradesystem='', country_filter=[], base_year=-1, end_year=-1, verbose=False, allow_repeats=True):
 #    """ Function: Generate Dynamic Edge Graph's
 #        Return: tradesystem
 #        Notes:      08/07/2013  -> Moved SubFunctions to be within the function so that future graph generators may also use the name first_year() for subfunctions 
 #    """

 #    """ Sub-Function Space """
 #    def first_year(df_country_year, tradesystem, country):
 #        """ Function:   Create BaseYear Node in Network
 #            Return:     Tradesystem with node 'S'->Export Basket, and list_of_products added
 #        """ 
 #        list_of_products = []    
 #        for row in df_country_year.iterrows():
 #            productcode, data = row
 #            try:                        #Allow Data to be inputed as rca
 #                rca = data['rca']
 #                if rca >= 1: 
 #                    mcp = 1
 #                else:
 #                    mcp = 0
 #            except:
 #                mcp = data['mcp']
 #            if int(mcp) == 1:
 #                tradesystem[country].add_node('S', key=0)
 #                tradesystem[country].add_node(productcode, key=1)
 #                tradesystem[country].add_edge('S', productcode, key=0)
 #                list_of_products.append(productcode)
 #        return tradesystem, list_of_products


 #    def add_year(df_country_year, tradesystem, base_year, current_year, list_of_products, country, allow_repeats=True):
 #        """ Function: Add additional year to Network
 #            Returns: updated tradesystem, and list of new_products added
 #        """
 #        new_list_of_products = []    
 #        new_set_of_products = set()
 #        time_delta = int(current_year) - int(base_year)
 #        for row in df_country_year.iterrows():
 #            productcode, data = row
 #            try:                        #Allow Data to be inputed as rca
 #                rca = data['rca']
 #                if rca >= 1.0: 
 #                    mcp = 1
 #                else:
 #                    mcp = 0
 #            except:
 #                mcp = data['mcp']
 #            if int(mcp) == 1:
 #                if allow_repeats == True:                                                                   #This produces a network that has loopbacks!
 #                    for source_product in list_of_products:
 #                        tradesystem[country].add_edge(source_product, productcode, key=time_delta)
 #                        new_set_of_products.add(productcode)                    
 #                else:
 #                    if tradesystem[country].has_node(productcode) == False:                                 #Must be New Product and ASSUMPTION IS - ONLY EMERGES ONCE!
 #                        for source_product in list_of_products:
 #                            tradesystem[country].add_node(productcode, key=time_delta+1)
 #                            tradesystem[country].add_edge(source_product, productcode, key=time_delta)
 #                            new_set_of_products.add(productcode)
 #        if new_set_of_products == set():                                    
 #            new_list_of_products = list_of_products             # So that Newtork doesn't become disconnected
 #        else: 
 #            new_list_of_products = list(new_set_of_products)
 #        return tradesystem, new_list_of_products

	  
 #    def final_year(tradesystem, base_year, end_year, list_of_products, country):
 #        """ Function: Close Network from Final Year Cross-Section
 #            Return: tradesystem with final cross section linked to Node: 'F'
 #        """   
 #        time_delta = int(end_year) - int(base_year) + 1       
 #        for source_product in list_of_products:
 #            tradesystem[country].add_node('F', key=time_delta+1)
 #            tradesystem[country].add_edge(source_product, 'F', key=time_delta)
 #        return tradesystem

 #    """ Main Function Space """
 #    #Check Data Inputs
 #    if type(df) != pd.DataFrame:                                                            
 #        print "ERROR: Pass DataFrame with MultiIndex (country, year, productcode)!"
 #        return None
 #    if type(df.index) != pd.MultiIndex:
 #        print "ERROR: Pass DataFrame with MultiIndex (country, year, productcode)!"
 #        return None
 #    #Ensure MultiLevel Index is correctly sorted
 #    if df.index.names != ['country', 'year', 'productcode']:                               
 #        df = df.reorder_levels(order=['country', 'year', 'productcode']).sort_index()      
 #    #Parse remaining Option Flags
 #    if base_year == -1:                                                                    
 #        base_year = min(df.index.levels[2])
 #    if end_year == -1:
 #        end_year = max(df.index.levels[2])
 #    if tradesystem == '':
 #        tradesystem, country_list, year_list, product_list = setup_country_product_emergence_graph(df, country_filter)
 #    if country_filter != []:
 #        country_list = country_filter
 #    """ Generate Graph """
 #    for country in country_list:                                                           
 #        if verbose: print "Adding Country: %s" % country        
 #        #Initialise tradesystem with Base Year Data 'S' -> 'ProductCodes'
 #        if verbose: print "Initial Year: %s" % base_year
 #        tradesystem, list_of_products = first_year(df.ix[country].ix[base_year], tradesystem, country)
 #        #Add Subsequent Years
 #        for year in range(base_year+1, end_year+1):
 #            if verbose: print "Adding Year: %s" % year
 #            tradesystem, list_of_products = add_year(df.ix[country].ix[year], tradesystem, base_year, year, list_of_products, country, allow_repeats=allow_repeats)
 #        #Add Final Year 'ProductCodes' -> 'F'
 #        if verbose: print "Final Year: %s" % end_year
 #        tradesystem = final_year(tradesystem, base_year, end_year, list_of_products, country)
 #    return tradesystem


### --- END WORKING HERE --- ####

	###########################################
	## -- Drawing & Visualisation Methods -- ##
	###########################################

	def draw_network(self, year):
		"""
			Plot Network for a Certain Year
			To Do: Impliment other graph types
		"""
		return self.network[year].draw_network()


	def plot_ts_simple(self, series, sort_data=True, verbose=True):
		"""
			Plot Time Series

			Future Work:
			-----------
				[1] Need to work on using Pandas datetime objects to get more contextually relevant graphs and charts
		"""
		if sort_data:				
			series = series.order(ascending=True, na_last=True)
		fig = plt.figure()
		ax = fig.add_subplot(1,1,1)
		ax = series.plot()
		ax.set_ylabel(series.name)
		return fig


### -- Main For Library File: DynamicProductLevelExportSystem --- ###
### -- Note: These have been migrated to tests/ but remain here as demonstrations on how to use the class -- ###

if __name__ == '__main__':
	print "Library File for DynamicProductLevelExportSystem Class"
	print "Following is a Demonstration of Some Features of the Class"
	print 

	from pyeconlab.util import package_folder

	### --- Options --- ###
	graphics = True

	### --- Test Data --- ###
	TEST_DATA_DIR = package_folder(__file__, "tests/data") 
	data = TEST_DATA_DIR + 'export-testdata.csv'
	
	#####################################
	### --- Test Simple Networks  --- ###
	#####################################
	
	A = DynamicProductLevelExportSystem()
	# - Import Test Data - #
	# - Funtions Tested: from_csv, from_df
	A.from_csv(data, dtypes=['DataFrame', 'BiPartiteGraph'], verbose=True)
	print
	print "Object A:"
	print A
	print "Object A.data => Should be the Same due to __repr__"
	print A.ples
	print "Object A[2000] Data:"
	print A[2000].data
	print "Object A[2000] Network Nodes:"
	print A[2000].network.nodes()
	print "Object A[2000] Network Edges:"
	print A[2000].network.edges()

	################
	#Test Functions#
	################

	if graphics: 
		print "Drawing Network"
		A[2000].draw_network()

	print "Test .data property"
	print A.data
	print "Test get_data() Extended method"
	print "Default Settings"
	print A.get_data()
	print "Specific Year Data"
	print A.get_data(year=2000)
	print "Long Panel of Data"
	print A.get_data(rtype='long')
	print "Wide Panel of Data"
	print A.get_data(rtype='wide')
	print "Panel of Data"
	print A.get_data(rtype='panel')

	print "Test as_cp_matrices()"
	A.as_cp_matrices(verbose=True)
	print A[2000].cp_matrix
	print A[2001].cp_matrix

	print "Testing .cp_matrix Property"
	print A.cp_matrix

	print "Testing rca_matrices()"
	A.complete_trade_network = True
	A.rca_matrices(verbose=True)
	print A[2000].rca
	print A[2001].rca
	print "Testing .rca Property"
	print A.rca

	print "Testing Mcp_matrices()"
	A.mcp_matrices(fillna=True, verbose=True)
	print A[2000].mcp
	print A[2001].mcp
	print "Testing .mcp Property"
	print A.mcp

	print "Testing proximity_matrices()"
	A.proximity_matrices(verbose=True)
	print A[2000].proximity
	print A[2001].proximity
	print "Testing .proximity Property"
	print A.proximity

	print "Testing proximity_matrices(matrix_type='asymmetric')"
	A.proximity_matrices(matrix_type='asymmetric', verbose=True)
	print A[2000].proximity
	print A[2001].proximity
	print "Testing .proximity Property"
	print A.proximity

	print "Testing Product Ubiquity Method"
	A.compute_ubiquity(verbose=True)
	print A[2000].ubiquity
	print A[2001].ubiquity
	print "Testing .ubiquity Property"
	print A.ubiquity

	print "Testing Country Diversity Method"
	A.compute_diversity(verbose=True)
	print A[2000].diversity
	print A[2001].diversity
	print "Testing .diversity Property"
	print A.diversity

	print "--------"
	print "TESTING COMPLETED SUCCESFULLY!"
	print "--------"

	sys.exit()

	#################################################
	## Alternative Ways of Working with the Object ##
	#################################################

	print "\nObject B:"
	B = DynamicProductLevelExportSystem()
	# - Import Test Data - #
	B.from_csv(data, dtypes=['DataFrame'], verbose=True)
	B.construct_bipartite(verbose=True)
	print B[2000].network.nodes()

	if graphics: B[2000].draw_network()


	####################################
	### --- Test Object Networks --- ###
	####################################
	# - Country Data from WDI - #
	W = wdi.WDI('WDI_Data.csv',source_ds='d1352f394ef8e7519797214f52ccd7cc', hash_file_sep=r' ', verbose=True)
	C = Countries() 																								#Investigate a Pickle Here
	C.build_from_wdi(W)

	A = DynamicProductLevelExportSystem()
	# - Import Test Data - #
	# - Funtions Tested: from_csv, from_df
	A.from_csv(data, cntry_obj=C, dtypes=['DataFrame', 'BiPartiteGraph'], verbose=True)
	print "Object A:"
	print A
	print "Object A.data => Should be the Same due to __repr__"
	print A.ples
	print "Object A[2000] Data:"
	print A[2000].data
	print "Object A[2000] Network Nodes:"
	print A[2000].network.nodes()
	print "Object A[2000] Network Edges:"
	print A[2000].network.edges()

	################
	#Test Functions#
	################

	if graphics: 
		print "\nDrawing Network"
		A[2000].draw_network()

	print "\nTest to_cp_matrices()"
	A.as_cp_matrices(verbose=True)
	print A[2000].cp_matrix
	print A[2001].cp_matrix

	print "\nTesting cp_matrices() Getter Method"
	print A.cp_matrices()