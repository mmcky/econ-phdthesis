# # -*- coding: utf-8 -*-
# """
# Project: 	PyEconLab
# Class: 		ProductLevelExportNetwork - Cross Section [NetworkX Data Core]
# Author: 	Matthew McKay <mamckay@gmail.com> 	

# **Status**: 	IN-WORK

# Current Work
# ------------
# [1] Convert to PyEconLab

# Purpose: 
# --------
# 	[1] Delivers a class suited for analysis of Product Level Exported Data.
# 	[2] Stores Product Level Data in a Structured Manner

# Internal Data Structures:
# ------------------------
# 	[1] **Default** => Data is stored in a pd.DataFrame with index (<country>, <productcode>), <export>
# 	[2] Other Data Structures => 'BiPartiteGraph', 'MultiPartiteGraph'

# Example:
# --------
# 	TBD

# Questions:
# ----------
# 	[1] How should I handle conversion to other trade data classification (HS) etc? Converter Class / Internal Methods?
# 	[2] How should I handle different data containers as some algorithms are much easier to generate based on a pre-computed data structure
# 		[Answer: So far, write algorithms based on the easiest computable underlying data structure]
# *** [3] Should Data be stored in pd.DataFrames? as the Base Data Structure? [This might be a more intuitive choice!] => However this won't be as memory efficient as data is stored twice! ***

# 		To reduce duplicity of data, allow functions to preserve current contents of self.data in the self.preserve dict
# 			[1] Check if current object exists in preserve and if it does then check if object is the same, if not raise Error!
# 			[2] def fill_obj_from_preserve()

# 		Q: How can I ensure that the data is the same in all the different 'formats'
# 			All data in an Instance is generated from self.data. Therefore, data integrity should be maintained unless users assign different data to self.data etc. 
# 			Should make internal objects immutable from external functions. 
# """

# ### --- Standard Library Imports --- ###

# from __future__ import division

# import sys
# import numpy as np
# import pandas as pd
# import networkx as nx
# from scipy import linalg
# from networkx.algorithms import bipartite
# import matplotlib.pyplot as plt
# import matplotlib.colors as colors
# from matplotlib import cm
# import cPickle as pickle
# import copy

# ### --- Accelerators --- ###
# # from numba import double
# # from numba.decorators import jit, autojit

# from numbapro import double
# from numbapro.decorators import jit, autojit

# ### -- Project Imports --- ###
# import pyeconlab.wdi as wdi
# from Countries import Country, Countries
# from Products import Product
# from Classification import WITSSITCR2L4

# ### --- ProductLevelExport Network --- ###

# class ProductLevelExportNetwork(object):
# 	'''
# 		Cross-Country Network of Exporters to the World

# 		Base Data Structures:
# 		---------------------
# 		  	[1] DataFrame 																	[Based on an instance of pd.DataFrame()]
# 		  	[2] BiPartiteGraph('country' -> 'product', edge = export value) 				[Based on an instance of nx.Graph()]
# 			[3] MultiDiGraph('country' -> 'world', key=productcode, edge = export value) 	[Based on an instance of nx.MultiDiGraph()]
# 				[**Convention**: Incoming Edge = Import]

# 		Notes & Questions:
# 		------------------
# 			[1] Should self.data be an immutable object that is always formated as a long indexed DataFrame?
# 			[2] Class Needs to Extend object for Properties to work correctly
# 			[3] Should Visualisations and Plotting be in a Separate Class File?

# 		**Default Structure** => pd.DataFrame, but some methods may require BiPartiteGraph or MultiDiGraph as they can be easier to compute from

# 		Architecture Updates: (RAM currently not binding constraing, focusing of computation for now = Cython)
# 		--------------------
# 			[1] Impliment the following change to the Architecture (Using Property Constructor)
				
# 				self._matrix = None 								#Last Computed Matrix
				
# 				Note using _matrix.name or _matrix.notes as Pandas Operations can remove these attributes from the underlying object!
# 				self._matrix_name = ''
# 				self._matrix_notes = '' 							#Last Computed Matrix_Notes

# 				@def matrix():
# 				    doc = "The matrix property."
# 				    def fget(self):
# 				        return self._matrix
# 				    def fset(self, value):
# 				        if self._matrix != None:
# 				        	#Pop Previously Computed Matrix into Preserve#
# 				        	self._preserved_matrix[self._matrix_name] = self._matrix
# 				        	self._preserved_matrix_notes[self._matrix_name] = self._matrix_notes
# 				        self._matrix = value
# 				    def fdel(self):
# 				        del self._matrix
# 				    return locals()
# 				matrix = property(**matrix())

# 				def get_matrix(self, matrix_name):
# 					#Check self._matrix_name First then look in _preserved_matrix
# 					# i.e. A.get_matrix('eci') will return eci matrix

# 				def load_matrix(self, matrix_name):
# 					#Swap out self._matrix for desired matrix

# 				Init Option: InMemory or OnDisk [__init__(self, in_memory=True)]
# 				--------------------------
# 				self._preserved_matrix = dict() OR HDFStore
# 				self._preserved_matrix_notes = dict() OR HDFStore

# 			Provide Backward Compatability:
# 			-------------------------------
# 				def populate_matrix_objects(self)
# 					self.eci = self.get_matrix('eci')
# 					etc.

# 		Code Reorganisation Updates:
# 		---------------------------
# 			ProductLevelExport.py {Core => Majority of this Code}
# 				ProductSpace => ProductSpace Analysis 
# 				Plotting => Plotting [Are these really PLES specific? or should this be on it's own level where .plot_mcp() is imported into objects?]
# 	'''

# 	##########################
# 	## -- Setup Routines -- ##
# 	##########################
	
# 	def __init__(self):
# 		## -- Indicator of Core Data Type -- ##
# 		self.core_type = 'network'	 				# Core Data Type {Used for __getitem__}
# 		self.year = None

# 		## -- Core Data -- ##
# 		self.notes  		= ''
# 		self.data 			= None					# In-Memory Core Data {Future Work: Make this immutable from external operations self._data}
# 		self.data_type  	= 'Pandas'				# {'Pandas' Long Table of (Country, ProductCode) -> <Export>}
# 		self.data_notes 	= ''
# 		self.data_file 		= None
# 		self.data_preserve 	= dict() 				# Preserve Dictionary {**Future Work:** Impliment On-Disk Storage}
# 		self.complete_trade_network = False 		# If loaded data is a complete trade network
# 		self.supp_data 		= dict() 				# Data Store for Supplimentary Data {if trade network is incomplete}
# 		self.country_classification = None 			# Country Classification (i.e. 'ISO3C')
# 		self.product_classification = None 			# Product Classification (i.e 'SITCR2L4')
		 
# 		## -- Network Models -- ##
# 		## -- Note: Should this be Migrated to self.data for better memory management at the expense of checking data types? -- ##
# 		## -- 		Answer: **No** Keep self.data as raw data which will be useful as a base for many other operations. If space becomes a problem, then use self.data_preserve to write to HDFStore or Shelf -- ##		
# 		self.network = None 						# Network Representation of Product Level Export System
# 		self.network_type = None 		
# 		self.use_objects = None
# 		self.network_attrs = dict() 				#Useful when not using Objects on the Core Network Structure to have attribute tables coded by 'ISO3C' or 'SITCR2' etc
# 		self.network_preserve = dict()
# 		# Optional Pointers to data_preserve to hold in-memory copies
# 		self.pandas = None
# 		self.bipartitegraph = None
# 		self.multidigraph = None

# 		## -- Country & Product Objects -- ##  		# A Dictionary or List of Countries and Products used within the Data Structure
# 		self.countries 	= None 
# 		self.products 	= None

# 		## -- Matrix Attributes -- ##
# 		self.matrix = None
# 		self.matrix_type = None
# 		self.matrix_preserve = dict()
# 		# OR #
# 		self.cp_matrix = None 						# Matrix of Values (c x p)
# 		self.adj_matrix = None 						# Adjacency Matrix (node x node)
		
# 		## -- ProductSpace Attributes -- ##
# 		self.productspace = None 					#ProductSpace(ProductLevelExportSystem) Object
# 		## OR ##
# 		self.rca = None
# 		self.rca_notes = '' 						# Place for writing any notes about the construction of self.rca (i.e. 3yrma-smoothing)
# 		self.rca_num = None 						
# 		self.rca_den = None
# 		self.mcp = None
# 		self.mcp_notes = ''
# 		self.proximity = None
# 		self.proximity_notes = ''
# 		self.ubiquity = None
# 		self.ubiquity_notes = ''
# 		self.diversity = None
# 		self.diversity_notes = ''
# 		self.kcn = None
# 		self.kcn_notes = ''
# 		self.kpn = None
# 		self.kpn_notes = ''
# 		self.mcc = None
# 		self.mcc_notes = ''
# 		self.eci = None
# 		self.eci_notes = ''
# 		self.mpp = None
# 		self.mpp_notes = ''
# 		self.pci = None
# 		self.pci_notes = ''

# 		## -- Temporary Objects -- ##
# 		self.temp = dict()

# 	def complete_setup(self, verbose=False):
# 		'''
# 			Complete Setup of DataShapes and Computables within Object
# 		'''
# 		self.setup_data(verbose=verbose)
# 		self.setup_computables(verbose=verbose)

# 	def setup_data(self, verobose=False):
# 		'''
# 			Completely Populate the Object with Useable DataShapes and Networks
# 			Future Work:
# 			------
# 				[1] Refactor from_df() to import data into self.data ONLY and split out the network functions into setup_bipartitegraph(), setup_multidigraph()
# 		'''
# 		## -- This is Current Handled by from_df() -- ##
# 		raise NotImplementedError

# 	def setup_computables(self, verbose=False):
# 		'''
# 			Completely Populate the Object with Computables (i.e. ProductSpace Values)
# 		'''
# 		self.rca_matrix(verbose=verbose)
# 		self.mcp_matrix(fillna=True, verbose=verbose)
# 		self.proximity_matrix(verbose=verbose)


# 	## -- Class Python Routines -- ##

# 	def network_uses_objects(self):
# 		'''
# 			Test if using Objects as Nodes
# 		'''
# 		if self.use_objects == True:
# 			return True
# 		elif isinstance(self.network.nodes()[0], Country) or isinstance(self.network.nodes()[0], Product): 			#BiPartite Networks have Country() and Product() as Nodes
# 			self.use_objects = True
# 			return True
# 		else:
# 			return False

# 	def __getitem__(self, val, verbose=False):
# 		'''
# 			Get Item for Core Type
# 			Notes:
# 			------
# 				[1] This current returns a dict() object of edges. **Q: What should it return?**
# 		'''	
# 		if verbose: print "__getitem__ for core type = %s" % self.core_type
# 		## -- Base Object Structure is network -- ##
# 		if self.core_type == 'network':
# 			if isinstance(val, Country) or isinstance(val, Product) and self.use_objects==True:  		
# 				return self.network[val]
# 			if type(val) == str:
# 				# - Test if Network Contains Objects - #
# 				if self.use_objects == True:															
# 					try: 
# 						c = self.countries[val]
# 						return self.network[c]
# 					except:
# 						c = False
# 						if verbose: print "Item: %s is not a country object within the network!"
# 					try:
# 						p = self.products[val]
# 						return self.network[p]
# 					except:
# 						p = False
# 						if verbose: print "Item: %s is not a product object within the network!"
# 					if not c or not p:
# 						raise ValueError("Item: %s is not either a Country or a Product within the network!" % val)
# 				# - Network must be simple network - #
# 				else:
# 					return self.network[val] 	
# 		else:
# 			raise ValueError('Core Type!')																			

# 	## -- Getter Methods -- ##

# 	def get_self_data(self):
# 		'''
# 			Check self.data and raise Value Error if doesn't contain any data
# 			Future Work:
# 			-----------
# 				[1] Make this the getter method for immutable data object _data
# 		'''
# 		if self.data == None:
# 			raise ValueError("self.data does not contain any data")
# 		else:
# 			return self.data

# 	def get_countries(self, verbose=False):
# 		return self.get_countries_from_df(df=self.data, verbose=verbose)

# 	def get_countries_from_df(self, df, verbose=False):
# 		'''
# 			Obtain Sorted List of Countries from DataFrame
			
# 			Incoming DataFrame:
# 			-------------------
# 										'export'
# 			<country>, <productcode> 	 value

# 			Future Work:
# 			-----------
# 				[1] Detect Index with name country
# 		'''
# 		countries = sorted(df.index.levels[df.index.names.index('country')])
# 		if verbose: print "Countries: %s" % countries
# 		return countries

# 	def get_products(self, verbose=False):
# 		return self.get_products_from_df(df=self.data, verbose=verbose)

# 	def get_products_from_df(self, df, verbose=False):
# 		'''
# 			Obtain Sorted List of Products from DataFrame

# 			Incoming DataFrame:
# 			-------------------
# 										'export'
# 			<country>, <productcode> 	 value

# 			Future Work:
# 			-----------
# 				[1] Detect Index with name productcode
# 		'''
# 		products = sorted(df.index.levels[df.index.names.index('productcode')])
# 		if verbose: print "ProductCodes: %s" % products
# 		return products

# 	##############
# 	## -- IO -- ##
# 	##############

# 	def data_from_df(self, df, country_classification, product_classification, df_notes='', verbose=False):
# 		'''
# 			Simple Load self.data with DataFrame Data
# 			Object can then be populated with other data structures as required

# 			Future Work:
# 			-----------
# 				[1] Check Conformance of incoming dataframe to self.data structure. 
# 		'''
# 		if verbose: print "Compiling Data Structure: DataFrame"
# 		# - Meta-Data from Cross-Section DF - #
# 		self.country_classification = country_classification
# 		self.product_classification = product_classification
# 		# - Save Incoming Data to Internal self.data - #
# 		self.data = df
# 		self.data_type = 'DataFrame'
# 		self.data_notes = df_notes
# 		self.countries = self.get_countries_from_df(df, verbose)									
# 		self.products = self.get_products_from_df(df, verbose)
# 		#return True

# 	def from_df(self, df, country_classification, product_classification, compile_dtypes, year, use_objects=False, cntry_obj=None, prod_obj=None, df_notes='', verbose=False):
# 		''' 
# 			Create Base Objects (self.data and self.network) From Pandas DataFrame
			
# 			Incoming DataFrame:
# 			-------------------
# 										'export'
# 			<country>, <productcode> 	 value

# 			Input:
# 			------
# 				[1] compile_dtypes 	: 	['DataFrame', 'BiPartiteGraph', 'MultiPartiteGraph']
# 										** The Last Specified Network is Constructed **

# 			Notes:
# 			------ 
# 				[1] Only ONE network type is produced even if more are specified in this list. The last computed network will be assigned to self.network
# 					Future Work: Detect Collisions and Move self.network to self.network_preserve to preserve computed structure
# 				[2] Currently no implimented option to inject Product Objects. They are currently always created from scratch and filled from self.classification etc.

# 			Future Work:
# 			-----------
# 				[1] Current implimentation only allows prefilled country objects and creates instances of Products. Allow for prefilled productcodes if required!
# 		'''
# 		# - Set Year - #
# 		self.year = year

# 		# - Attach DF to self.data by Default - #
# 		self.data_from_df(df, country_classification, product_classification, df_notes, verbose)

# 		# - Compute Following Data Structures - #
# 		for dtype in compile_dtypes:
# 			if dtype == 'DataFrame':
# 				# - Already Done By Default - #
# 				pass
# 			elif dtype == 'BiPartiteGraph':
# 				if verbose: print "\nCompiling Network Data Structure: %s" % dtype
# 				# - Check for Network Collision - #
# 				self.prepare_self_network(verbose)
# 				# - Compute Network - #
# 				self.network_type = dtype
# 				if isinstance(cntry_obj, Countries):
# 					# - Country Objects - #
# 					self.use_objects = True 											#Useful for Algorithms wanting to Know if objects are used in network structure
# 					self.network = self.construct_bipartite_with_cobjects(cntry_obj, verbose)
# 				else:
# 					# - Simple Network Layout of countrynames as nodes and productcode, values as edges - #
# 					self.network = self.construct_bipartite(verbose)
# 			elif dtype == 'MultiDiGraph':
# 				if verbose: print "\nCompiling Network Data Structure: %s" % dtype
# 				# - Check for Object Collision - #
# 				self.prepare_self_network(verbose)
# 				# - Compute Network - #
# 				self.network_type = dtype
# 				if isinstance(cntry_obj, Countries):
# 					# - Countries as Objects - #  												
# 					self.use_objects = True 											#Useful for Algorithms wanting to Know if objects are used in network structure
# 					self.network = self.construct_multidi_with_cobjects(cntry_obj, verbose)
# 				else:
# 					# - Simple Network Layout of countrycodes as nodes and productcode, values as edges - #
# 					self.network = self.construct_multidi(verbose)
# 			## -- Exception Handling -- ##
# 			else:
# 				raise ValueError("Unknown Data Structure Type! ['DataFrame', 'BiPartiteGraph', 'MultiDiGraph']")

# 	def from_rca_df(self, df, country_classification, product_classification, year, df_notes='', verbose=False):
# 		'''
# 			Populate PLES Object from RCA Data
			
# 			Notes:
# 			-----
# 				[1] Not implimenting country or product object options ... leaving that to construct_network() method
# 		'''
# 		# - Set Year - #
# 		self.year = year
# 		# - Classification - #
# 		self.country_classification = country_classification
# 		self.product_classification = product_classification
# 		# - Populate RCA Matrix - #
# 		if verbose: print "Constructing RCA Matrix for Year: %s" % year
# 		self.rca = df
# 		return self.rca

# 	def prepare_self_network(self, verbose=False):
# 		'''
# 			Prepare self.network by detecting if Network is Already Computed.
# 			If a computed network is found then it will move self.network to self.network_preserve['network-type' : network-object]
# 			Notes:
# 			------
# 				[1] self.network_preserve = tuple(self.network, self.network_type, self.use_objects) [Important when restoring from self.network_preserve]
# 		'''	
# 		# - Type Testing and Preserving - #
# 		if type(self.network) == type(None):
# 			return True
# 		elif type(self.network) == nx.Graph and self.network_type == 'BiPartiteGraph':
# 			self.network_preserve['BiPartiteGraph'] = (self.network, self.network_type, self.use_objects)
# 		elif type(self.network) == nx.MultiDiGraph:
# 			self.network_preserve['MultiDiGraph'] = (self.network, self.network_type, self.use_objects)
# 		else:
# 			raise ValueError("Network Type is an Unknown Type (%s)" % type(self.network))
# 		# - Clear Core Network Attributes - #
# 		self.network = None
# 		self.network_type = None
# 		self.use_objects = None

# 	def from_csv(self):
# 		'''
# 			Load a Single Cross Section of Export Data from CSV
# 			Notes:
# 			------
# 				[1] Not implimented yet as mainly will be using a panel in DynProductLevelExportSystem
# 		'''
# 		raise NotImplementedError("Method: from_csv() => Not Yet Implimented ... !")
	
# 	def load_csv(self, fn, summary=True, verbose=False):
# 		'''
# 			Load a CSV file and return a Pandas DataFrame Object
# 			Helper function for load_supp_data but may not be required. 
# 		'''
# 		if verbose: print "Loading DataFrame from file: %s" % fn
# 		df = pd.read_csv(fn)
# 		if summary == True:
# 			print df
# 		return df

# 	def load_supp_data(self, df, series_name, verbose=False):
# 		'''
# 			Method for Loading Object with Supplimentary Data
# 			Options:
# 			-------
# 				[1] series_name 	=> 	specify
# 		'''
# 		self.supp_data[series_name] = df


# 	########################
# 	## -- Constructors -- ##
# 	########################

# 	## -- Network Object Constructors -- ##
# 	#######################################

# 	def construct_network(self, ntype='bipartitegraph', c=None, p=None, verbose=False):
# 		'''
# 			Construct a Requested Network of ntype
			
# 			Notes:
# 			------
# 				[1] Generating Networks with Prefilled P objects (Products) is currently not supported!
# 		'''
# 		if verbose: print "Constructing a Network of Type: %s" % ntype
# 		# - No Country Objects Provided - #
# 		if c == None: 																	#IS this going to cause comparison isues when c != None? Better to test isinstance(c, Country())
# 			if ntype == 'bipartitegraph':
# 				self.construct_bipartite(verbose)
# 			elif ntype == 'multidigraph':
# 				self.construct_multidi(verbose)
# 			else:
# 				raise ValueError("'ntype' must be a bipartitegraph or multidigraph")
# 		# - Generate Network with Country() Objects
# 		else:
# 			if ntype == 'bipartitegraph':
# 				self.construct_bipartite_with_cobjects(c, verbose)
# 			elif ntype == 'multidigraph':
# 				self.construct_multidi_with_cobjects(c, verbose)
# 			else:
# 				raise ValueError("'ntype' must be a bipartitegraph or multidigraph")
# 		# - Return Reference to Network - #
# 		return self.network


# 	# - Bipartite Graphs - #
# 	########################

# 	def construct_bipartite(self, verbose=False, verbose_limit=10):
# 		'''
# 			Construct a Simple BiPartiteGraph Network
			
# 			Options:
# 			-------
# 				[1] verbose_limit 	: 	number of lines to show as example computation
# 		'''
# 		if verbose: print "Constructing BiPartiteGraph: %s (with verbose_limit: %s)" % (self.year, verbose_limit)
# 		n = nx.Graph()
# 		n.add_nodes_from(self.countries, bipartite='countries')
# 		n.add_nodes_from(self.products, bipartite='productcodes')
# 		verbose_counter = 0
# 		for idx, val in self.data.iterrows():
# 			(country, productcode) = idx
# 			export = val['export']
# 			n.add_edge(country, productcode, export=export)
# 			if verbose_counter <= verbose_limit:
# 				if verbose: print "Adding %s from %s to %s" % (export, country, productcode)
# 			verbose_counter += 1
# 		self.network = n
# 		return n

# 	def construct_bipartite_with_cobjects(self, c, verbose=False, verbose_limit=10):
# 		'''
# 			Construct a BiPartiteGraph with Objects [Nodes: Country() or Product() belonging to sets 'countries', 'productcodes']

# 			Notes:
# 			------
# 				[1] In BiPartiteGraph 	=> 	product() are node objects (Knowing this allows for __repr__ to correctly represent the object as a code rather tham a value)
# 				[2] Added a verbose_limit for large datasets

# 			Future Work:
# 			-----------
# 				[1] Improve Reporting to be Number of Country Objects Loaded and Number of Simple Objects Loaded
# 		'''
# 		if verbose: print "Constructing BiPartiteGraph with Country Objects (from cobjects) and Product Objects (generated): %s (with verbose_limit: %s)" % (self.year, verbose_limit)
# 		# - Construct Internal Dict of Country Objects - #
# 		countries = self.countries 								#Save List of Countries
# 		self.countries = dict() 			
# 		products = self.products  								#Save List of Countries
# 		self.products = dict()
# 		n = nx.Graph()
# 		#Declare Country Nodes
# 		for country in countries:  							#Generate a list within the object?
# 			try:
# 				if verbose: print "Adding Country %s and is an instance of Country(): %s" % (country, isinstance(c[country], Country))
# 				self.countries[country] = c[country] 												#Build Local Dict of Used Country Objects
# 				n.add_node(self.countries[country], bipartite='countries')
# 			except:
# 				# - Construct Heterogenous Network - #
# 				print "---"
# 				print "Adding Country (%s) as a Simple str() object" % country
# 				print "---"
# 				self.countries[country] = country
# 				n.add_node(country, bipartite='countries')
# 				# - Raise ValueError - #
# 				#raise ValueError("Country (%s) is not found in countries object" % country) 			#Probably should create empty country? OR return simple object str
# 		#Declare Product Nodes
# 		for productcode in products:
# 			self.products[productcode] = Product(code=productcode, classification=self.product_classification, ntype='node')
# 			n.add_node(self.products[productcode], bipartite='productcodes')
# 		#Declare Edges
# 		verbose_counter = 0
# 		for idx, val in self.data.iterrows():
# 			(country, productcode) = idx
# 			export = val['export'] 	
# 			n.add_edge(self.countries[country], self.products[productcode], export=export)
# 			if verbose_counter <= verbose_limit:
# 				if verbose: print "Adding %s from %s to %s" % (export, self.countries[country], self.products[productcode]) 			#removed  self.products[productcode].code as __repr__ should handle this
# 			verbose_counter += 1
# 		self.network = n
# 		return n 

# 	#- MultiPartite Graphs - #
# 	##########################

# 	def construct_multidi(self, verbose=False, verbose_limit=10):
# 		'''
# 			Construct a Simple MultiDiGraph
# 		'''
# 		if verbose: print "Constructing MultiDiGraph: %s (with verbose_limit: %s)" % (self.year, verbose_limit)
# 		n = nx.MultiDiGraph()
# 		n.add_nodes_from(self.countries)
# 		# - Add World Node - #
# 		if 'WLD' not in self.countries: 
# 			n.add_node('WLD')
# 		verbose_counter = 0
# 		for idx, val in self.data.iterrows():
# 			(country, productcode) = idx
# 			export = val['export']
# 			n.add_edge(country, 'WLD', key=productcode, export=export)
# 			if verbose: 
# 				if verbose_counter <= verbose_limit:
# 					print "Adding %s (%s) from %s to %s" % (export, productcode, country, productcode)
# 				verbose_counter += 1
# 		self.network = n
# 		return n

# 	def construct_multidi_with_cobjects(self, c, verbose=False, verbose_limit=10):
# 		'''	Construct a MultiDi Graph with Country Objects (from c) and Product Objects (Generated)
# 			Notes:
# 				[1] In MultiDiGraph 	=> 	product() are edge objects
# 		'''
# 		if verbose: print "Constructing MultiDiGraph using Country Objects as Nodes and Product Objects as Edges: %s (with verbose_limit: %s)" % (self.year, verbose_limit)
# 		# - Construct Internal Dict of Country Objects - #
# 		countries = self.countries 				#Save list of Countries
# 		self.countries = dict() 				#Rather than rewriting self.countries this should be written to self.countries_obj for Object Catelog (Only and Issue if Run More than One Network Constructor)
# 		n = nx.MultiDiGraph()	
# 		#Declare Country Nodes
# 		for country in countries:  							#Generate a list within the object?
# 			try:
# 				if verbose: print "Adding Country %s and is an instance of Country(): %s" % (country, isinstance(c[country], Country))
# 				self.countries[country] = c[country] 												#Build Local Dict of Used Country Objects
# 				n.add_node(self.countries[country])
# 			except:
# 				# - Construct Heterogenous Network - #
# 				print "---"
# 				print "Adding Country (%s) as a Simple str() object" % country
# 				print "---"
# 				self.countries[country] = country
# 				n.add_node(country)
# 				# - Raise ValueError - #
# 				#raise ValueError("Country (%s) is not found in countries object" % country) 			#Probably should create empty country? OR return simple object str
# 		#Declare World Node
# 		if verbose: print "Adding World Node"
# 		try:
# 			self.countries['WLD'] = c['WLD']
# 		except:
# 			raise ValueError('WLD Country() object not found!') 									#Could Create an Empty Instance?
# 		n.add_node(self.countries['WLD'])
# 		#Declare Edges
# 		verbose_counter = 0
# 		for idx, val in self.data.iterrows():
# 			(country, productcode) = idx
# 			export = val['export']
# 			# - Create Edge Object - #
# 			p = Product(ntype='edge')
# 			p.code = productcode
# 			p.value = export 
# 			n.add_edge(self.countries[country], self.countries['WLD'], key=productcode, export=p)	 								#From Country to World - Export Value
# 			if verbose: 
# 				if verbose_counter <= verbose_limit:
# 					print "Adding %s (%s) from %s to %s" % (p, p.code, self.countries[country], self.countries['WLD'])
# 				verbose_counter += 1
# 		self.network = n
# 		return n


# 	###########################################
# 	## -- International Trade Computables -- ##
# 	###########################################

# 	def rca_matrix(self, series_name='export', fill_na=False, clear_temp=True, complete_data=False, decomposition=False, verbose=False):
# 		'''
# 			Generate Revealed Comparative Advantage (RCA) Matrix (Shape: Country x Product)
# 			Measure: Balassa (1965) Trade Liberalisation and Revealed Comparative Advantage

# 			Options:
# 			--------
# 				[1] series_name 	=> 	Allow specification of the series_name [Default: 'export']
# 				[2] fill_na 		=> 	True/False [Default: False]
# 				[3] clear_temp 		=> 	Delete Data generated in temp
# 				[4] complete_data 	=> 	Allows for ALL RCA values to be computed using a complete trade system. (i.e. TotWorldTrade is represented by the sample)
# 				[5] decomposition 	=> 	Saves Numerator (self.rca_num) and Denominator (self.rca_den) Values [Default: False]

# 			Notes:
# 			-----
# 				[1] Should I have a self.rca or use self.matrix and self.matrix_type for more efficient memory use?
# 				[2] self.rca_notes = Is this a good idea or burden?
# 				[3] Data in self.supp_data needs to be a pd.Series Object or Value. pd.read_csv() can return DataFrame. Could export this to a function returning a pd.Series

# 			Future Work:
# 			-----------
# 				[1] Compute RCA using a NUMBA method to improve performance
# 		'''
# 		if complete_data == True:
# 				self.complete_trade_network = True
# 		if self.complete_trade_network == True:
# 			## -- Endogenously Compute RCA -- ##
# 			if verbose: print "Endogenously computing: TotalWorldExport, TotalProductExport, and TotalCountryExport"
# 			self.supp_data['TotalWorldExport'] = self.data[series_name].sum()
# 			self.supp_data['TotalProductExport'] = self.data.groupby(level=['productcode'])[series_name].transform(np.sum)
# 			self.supp_data['TotalCountryExport'] = self.data.groupby(level=['country'])[series_name].transform(np.sum)
# 			self.rca_notes = 'Simple RCA computed from self.data {Assumption: Complete Trade Network}'
# 			self.temp['rca_num'] = (self.data[series_name] / self.supp_data['TotalCountryExport'])
# 			self.temp['rca_num'].name = 'rca_num'
# 			self.temp['rca_den'] = (self.supp_data['TotalProductExport'] / self.supp_data['TotalWorldExport'])
# 			self.temp['rca_den'].name = 'rca_den'
# 			# - Parse decomposition option - #
# 			if decomposition:
# 				self.rca_num = self.temp['rca_num'].unstack(level='productcode')
# 				self.rca_num.name = 'rca_num'
# 				self.rca_den = self.temp['rca_den'].unstack(level='productcode')
# 				self.rca_den.name = 'rca_den'
# 			# - Compute RCA - #
# 			self.temp['rca'] =  self.temp['rca_num'] / self.temp['rca_den']  
# 			tmp_used = ['rca_num', 'rca_den', 'rca']
# 		else:
# 			## -- Require self.supp_data -- ##
# 			if verbose: print "Using TotalWorldExport, TotalProductExport, and TotalCountryExport from self.supp_data"
# 			dnames = self.supp_data.keys()
# 			# - Check Data is Available - #
# 			if 'TotalWorldExport' not in dnames: raise ValueError("Total World Export ('TotalWorldExport') required in self.supp_data to compute RCA")
# 			if 'TotalProductExport' not in dnames: raise ValueError("Total Product Export ('TotalProductExport') required in self.supp_data to compute RCA")
# 			if 'TotalCountryExport' not in dnames: raise ValueError("Total Country Export ('TotalCountryExport') required in self.supp_data to compute RCA")
# 			self.rca_notes = 'Simple RCA computed from self.supp_data {Assumption: Incomplete Trade Network}'
# 			# - Type Checking - #
# 			if type(self.supp_data['TotalCountryExport']) == pd.DataFrame:
# 				if verbose: print "Data in self.supp is a DataFrame. Converting to pd.Series for 'TotalCountryExport'"
# 				self.supp_data['TotalCountryExport'] = self.supp_data['TotalCountryExport']['TotalCountryExport']
# 			# - Compute Numerator - #
# 			self.temp['rca_num'] = self.data[series_name].div(self.supp_data['TotalCountryExport'], level='country') 		#Note TotalCountryExport Needs to be a pd.Series
# 			# - Type Checking - #
# 			if type(self.supp_data['TotalProductExport']) == pd.DataFrame:
# 				if verbose: print "Data in self.supp is a DataFrame. Converting to pd.Series for 'TotalProductExport'"
# 				self.supp_data['TotalProductExport'] = self.supp_data['TotalProductExport']['TotalProductExport']
# 			if type(self.supp_data['TotalWorldExport']) == pd.Series: 	#Should this be pd.DataFrame?
# 				if verbose: print "Data in self.supp is a Series. Converting to Value for 'TotalWorldExport'"
# 				self.supp_data['TotalWorldExport'] = self.supp_data['TotalWorldExport']['TotalWorldExport']
# 			# - Computing Denominator - #
# 			self.temp['rca_den'] = self.supp_data['TotalProductExport'].div(self.supp_data['TotalWorldExport'])
# 			# - Compute RCA - #
# 			self.temp['rca'] = self.temp['rca_num'].div(self.temp['rca_den'], level='productcode')
# 			# - Parse decomposition option - #
# 			if decomposition:
# 				self.rca_num = self.temp['rca_num'].unstack(level='productcode')
# 				self.rca_num.name = 'rca_num'
# 				self.rca_den = self.temp['rca_den'].unstack(level='productcode')
# 				self.rca_den.name = 'rca_den'
# 			tmp_used = ['rca_num', 'rca_den', 'rca']
# 		## -- Rearrange Data -- ##
# 		rca = self.temp['rca'].unstack(level=['productcode']).sort_index(axis=0).sort_index(axis=1)   #Added pd.DataFrame() so that this returns a DF rather than a Series
#    		if fill_na: rca = rca.fillna(0.0)   
# 		# - Flush Used Items in Temp File - #
# 		if clear_temp: 
# 			for item in tmp_used:
# 				del self.temp[item]
# 		# self.matrix = rca
# 		# self.matrix_type == 'rca'
# 		self.rca = rca
# 		self.rca.name = 'rca'
# 		return self.rca

# 	def rca_decomposition_table(self, fresh_rca=True, series_name='export', verbose=False):
# 		'''
# 			Return Decomposition of the RCA Calculations
# 			Measure: Balassa (1965) Trade Liberalisation and Revealed Comparative Advantage

# 			Notes:
# 			-----
# 				[1] Might be more helpful to write functions .rca_matrix_denominator(), .rca_matrix_numerator()
# 		'''
# 		# - Freshly Compute the RCA Values - #
# 		if fresh_rca == True:
# 			self.rca_matrix(series_name=series_name, clear_temp=False, verbose=verbose)
# 		# - Fraction Components - #
# 		a = self.rca.unstack().reorder_levels(order=['country', 'productcode'])
# 		b = self.temp['rca_num'] 												#Usually deleted but is contained in supp data due to clear_temp=False
# 		c = self.temp['rca_den']
# 		d = self.data['export']
# 		e = self.supp_data['TotalCountryExport']
# 		f = self.supp_data['TotalProductExport']
# 		g = self.supp_data['TotalWorldExport']
# 		# - Build Single Pandas DataFrame - #
# 		rca_table = pd.concat([a,b,c,d,e,f], axis=1)
# 		rca_table.columns = ['RCA', 'RCA_NUM', 'RCA_DEN', 'Export', 'TotalCountryExport', 'TotalProductExport']
# 		rca_table['TotalWorldExport'] = g		
# 		rca_table.name = 'RCA-DecompositionTable'
# 		return rca_table

# 	def symmetric_rca_matrix(self, series_name='export', fill_na=False, clear_temp=True, verbose=False):
# 		return self.rsca_matrix(series_name=series_name, fill_na=fill_na, clear_temp=clear_temp, verbose=verbose)

# 	def srca_matrix(self, series_name='export', fill_na=False, clear_temp=True, verbose=False):
# 		'''
# 			Computes the Symmetric RCA Measure (RCA - 1)/(RCA + 1) where -1.0 < RSCA < +1.0. 
# 			Ref:  (Dalum, Laursen, Villumsen, 1998) "Structural Change in OECD Export Specialisation Patterns: dec-specialisation and 'stickiness'"
# 		'''
# 		# - Check RCA Data is Available - #
# 		if type(self.rca) != pd.DataFrame:
# 			print "RCA Matrix not found. Compute RCA using default kwargs"
# 			self.rca_matrix()
# 		# - Compute RSCA - #
# 		rsca = self.rca.applymap(lambda x: (x-1)/(x+1))
# 		return rsca

# 	def country_shares(self, series_name='export'):
# 		'''
# 			Compute Country Export Share
# 		'''
# 		cshare = (self.data.sum(level='country') / self.data.sum())[series_name]
# 		cshare.name = 'country' + series_name + 'share'
# 		return cshare

# 	def product_shares(self, series_name='export'):
# 		'''
# 			Compute Product Export Share
# 		'''
# 		pshare = (self.data.sum(level='productcode') / self.data.sum())[series_name]
# 		pshare.name = 'product' + series_name + 'share'
# 		return pshare

# 	######################################
# 	### --- ProductSpace Functions --- ###
# 	######################################

# 	### --- Mcp Methods --- ##
# 	##########################

# 	def mcp_matrix(self, cutoff=1.0, fillna=True, verbose=False):
# 		'''
# 			ProductSpace Function for Generating Mcp Matrix {1,0} Export Indicators 'rca' >= 1
# 		'''
# 		# - Map of Values - #
# 		def mapping(x, cutoff):
# 			if np.isnan(x):
# 				return np.nan
# 			elif x < cutoff:
# 				return 0
# 			else:
# 				return 1

# 		## -- Check Required Inputs -- ##
# 		if type(self.rca) != pd.DataFrame:
# 			if verbose: print "RCA Matrix at (self.rca) is currently not available ... running self.rca_matrix()"
# 			self.rca_matrix()
# 		self.mcp = self.rca.applymap(lambda x: mapping(x, cutoff)) 	 		
# 		if fillna:
# 			self.mcp = self.mcp.fillna(0) 											
# 		self.mcp.name = 'Mcp'
# 		return self.mcp


# 	### --- Proximity Matrix Functions --- ###
# 	##########################################

# 	def proximity_matrix(self, fillna=False, clear_temp=True, verbose=False):
# 		'''
# 			ProductSpace Function for Computing Proximity Matrix
			
# 			Notes:
# 			------
# 				[1] Numpy Implimentation: timeit results: 1000 loops, best of 3: 857 µs per loop
# 				[2] Pandas Implimentation: 100 loops, best of 3: 2.84 ms per loop
# 				[3] compute_proximity() method allows for non-standard proximity matrices to be computed ('asymmetric', 'minmax' etc.)
# 			  **[4] Converted to Using a NUMBA accelerated version (Big Improvement in Performance 49 x Faster than Numpy)
# 		'''
# 		return self.proximity_matrix_numba(fillna=fillna, clear_temp=clear_temp, verbose=verbose)


# 	def proximity_matrix_pandas(self, fillna=False, clear_temp=True, verbose=False):
# 		'''
# 			ProductSpace Funtion for Computing Proximity Matrix
# 			Notes:
# 			-----
# 				[1] Pandas Method for Computing a Symmetrix Proximity Matrix
# 				[2] 100 loops, best of 3: 2.84 ms per loop (4 x 3 Example)
# 		'''
# 		## - Check Mcp State - ##
# 		if type(self.mcp) != pd.DataFrame:
# 			if verbose: print "Mcp matrix at (self.mcp) is currently not available. Computing Mcp with default kwargs"
# 			self.mcp = self.mcp_matrix()
# 		## - Compute Symmetric Proximity - ##
# 		self.proximity = pd.DataFrame(index=self.mcp.columns, columns=self.mcp.columns) 	 #np.nan initialised matrix
# 		products = self.mcp.columns
# 		self.temp['column_sums'] = self.mcp.sum()
# 		for product1 in products:
# 			for product2 in products:
# 				cond_prob = (self.mcp[product1] * self.mcp[product2]).sum() / max(self.temp['column_sums'][product1], self.temp['column_sums'][product2])
# 				self.proximity.set_value(index=product1, col=product2, value=cond_prob)
# 		self.proximity_notes = 'symetric'
# 		## - Fill Na Option - ##
# 		if fillna:
# 			self.proximity = self.proximity.fillna(0.0)
# 		## - Remove Temp Data - ##
# 		if clear_temp:
# 			del self.temp['column_sums']
# 		self.proximity.name = 'Proximity'
# 		return self.proximity

# 	def proximity_matrix_numpy(self, fillna=False, clear_temp=True, verbose=False):
# 		'''
# 			ProductSpace Function for Computing Proximity Matrix
			
# 			Notes:
# 			-----
# 				[1] Numpy Method for Computing a Symmetrix Proximity Matrix [Depricated: proximity_matrix_nump_numba()]
# 				[2] timeit results: 1000 loops, best of 3: 857 µs per loop (3 x 4 Example)
# 				[3] timeit results: 1 loops, best of 3: 13.5 s per loop (176 x 646)

# 		'''
# 		## - Check Mcp State - ##
# 		if type(self.mcp) != pd.DataFrame:
# 			if verbose: print "Mcp matrix at (self.mcp) is currently not available. Computing Mcp"
# 			self.mcp = self.mcp_matrix()
# 		## - Compute Symmetric Proximity - ##
# 		num_products = len(self.products)
# 		self.proximity = np.zeros((num_products, num_products))
# 		self.temp['col_sums'] = self.mcp.sum()	
# 		self.temp['data'] = self.mcp.T.as_matrix()          		#This generates a c x p numpy array
# 		for index1 in range(0,num_products):
# 			for index2 in range(0,num_products):
# 				cond_prob = (self.temp['data'][index1] * self.temp['data'][index2]).sum() / max(self.temp['col_sums'][index1], self.temp['col_sums'][index2])
# 				self.proximity[index1][index2] = cond_prob
# 		# Return DataFrame Representation #
# 		self.proximity = pd.DataFrame(self.proximity, index=self.products, columns=self.products)
# 		self.proximity.index.name = 'productcode1'
# 		self.proximity.columns.name = 'productcode2'
# 		# self.proximity.index.set_names(names=['productcode1'], inplace=True)
# 		# self.proximity.columns.set_names(names=['productcode2'], inplace=True)
# 		self.proximity_notes = 'symmetric'
# 		if verbose: print "Index: %s (%s); Columns: %s (%s)" % (self.proximity.index.name, len(self.proximity.index), self.proximity.columns.name, len(self.proximity.columns))
# 		## - Fill Na Option - ##
# 		if fillna:
# 			self.proximity = self.proximity.fillna(0.0)
# 		## - Remove Temp Data - ##
# 		if clear_temp:
# 			del self.temp['col_sums']
# 			del self.temp['data']
# 		self.proximity.name = 'Proximity' 				#Good to name Objects last due to some Pandas behaviour through things like fillna(0) methods creating a new DF
# 		return self.proximity

# 	def proximity_matrix_numba(self, fillna=False, clear_temp=True, verbose=False):
# 		'''
# 			ProductSpace Function for Computing Proximity Matrix
			
# 			Notes:
# 			-----
# 				[1] Simple Method for Computing a Symmetrix Proximity Matrix
# 				[2] timeit results: 1 loops, best of 3: 275 ms per loop (176 x 646) [49 x Faster than Numpy Solution]
# 				[3] Updated to minimise computation making use of the symmetry condition 

# 			Future Work:
# 			-----------
# 				[1] Needs to handle exceptions and bad data better (Remove Irrelevant Rows and Columns prior to using coexport_probability())
			
# 		'''
# 		# - Computational Helper Functions - #

# 		# from numba import double
# 		# from numba.decorators import jit, autojit

# 		from numbapro import double
# 		from numbapro.decorators import jit, autojit

# 		@autojit
# 		def coexport_probability(X):
# 			C = X.shape[0]
# 			P = X.shape[1]      
# 			# - Column Sums - #
# 			CS = np.zeros((P,), dtype=np.float)
# 			for p in range(P):
# 				for c in range(C):
# 					CS[p] += X[c,p]
# 			D = np.empty((P, P), dtype=np.float)    #Return Matrix
# 			for i in range(P):
# 				for j in range(P):
# 					# - Symmetry Condition - #
# 					if j <= i: 			
# 					# - Compute Elemental Pairwise Sums over each Product Vector - #
# 						pws = 0
# 						for c in range(C):
# 							pws += (X[c,i] * X[c,j])
# 						coexprob = pws / max(CS[i], CS[j])
# 						D[i,j] = coexprob
# 						D[j,i] = coexprob
# 			return D 

# 		## - Check Mcp State - ##
# 		if type(self.mcp) != pd.DataFrame:
# 			if verbose: print "Mcp matrix at (self.mcp) is currently not available. Computing Mcp (with default KWARGS)"
# 			self.mcp = self.mcp_matrix()
# 		## - Compute Symmetric Proximity - ##
# 		num_products = len(self.products)
# 		## - Prepare Data -- ##
# 		self.temp['data'] = self.remove_zero_relationships_matrix(self.mcp)  		#Remove np.nan relationships for iteration
# 		product_index = copy.deepcopy(self.temp['data'].columns)
# 		self.temp['data'] = self.temp['data'].as_matrix()       					#This generates a c x p numpy array

# 		## -- Speed Up Loops Using NUMBA -- ##
# 		self.proximity = coexport_probability(self.temp['data'])
# 		## -- END Speed Up Loops Using NUMBA -- ##

# 		# Return DataFrame Representation #
# 		self.proximity = pd.DataFrame(self.proximity, index=copy.deepcopy(product_index), columns=copy.deepcopy(product_index))

# 		# - Reinstate Removed Rows and Columns - #
# 		self.proximity = self.proximity.reindex_axis(labels=copy.deepcopy(self.mcp.columns), axis=0).reindex_axis(labels=copy.deepcopy(self.mcp.columns), axis=1)

# 		if verbose: print "Index: %s (%s); Columns: %s (%s)" % (self.proximity.index.name, len(self.proximity.index), self.proximity.columns.name, len(self.proximity.columns))
# 		## - Fill Na Option - ##
# 		if fillna:
# 			self.proximity = self.proximity.fillna(0.0)
# 		## - Remove Temp Data - ##
# 		if clear_temp:
# 			del self.temp['data']
# 			#del self.temp['pindex']
# 		self.proximity.name = 'Proximity' 				#Good to name Objects last due to some Pandas behaviour through things like fillna(0) methods creating a new DF
# 		self.proximity.index.set_names(names=['productcode1'], inplace=True)
# 		self.proximity.columns.set_names(names=['productcode2'], inplace=True)
# 		self.proximity_notes = 'symmetric'
# 		return self.proximity

# 	## - Cython Version of the Computational Helper Function: coexport_probability_cy() - ##
# 	########################################################################################

# 		# %%load_ext cythonmagic
# 		# %%cython
# 		# import numpy as np
# 		# cimport numpy as np

# 		# def coexport_probability_cy(double[:,:] X):
# 		# 	'''
# 		# 		IPython Method Using Cython Magic
# 		# 		Notes:
# 		# 		-----
# 		# 			[1] timeit results: 1 loops, best of 3: 231 ms per loop (176 x 646) [Fairly Similar to Numba Solution]
# 		# 				Sticking with Numba as it is easier to integrate using autojit()
# 		# 	'''
# 		# 	cdef int C, P, i, j, k
# 		# 	C = X.shape[0]
# 		# 	P = X.shape[1]
# 		# 	cdef double pws
# 		# 	cdef double [:] CS = np.sum(X, axis=0)
# 		# 	cdef double [:,:] D = np.empty((P,P), dtype=np.float)

# 		# 	for i in range(P):
# 		# 		for j in range(P):
# 		# 			pws = 0.0
# 		# 			for c in range(C):
# 		# 				pws += (X[c, i] * X[c, j])
# 		# 			D[i,j] = pws / max(CS[i], CS[j]) 
# 		# 	return D


# 	def compute_proximity(self, matrix_type='symmetric', clear_temp=True, fillna=False, verbose=False):
# 		'''
# 			ProductSpace Funtion for Generating Different Proximity Matrix Types ('Symmetric', 'Assymetric', 'MinMax')
			
# 			Options:
# 			-------
# 				[1] type 	=> 	'symmetric', 'assymetric', 'minmax'
# 			Notes:
# 			------
# 				[1] A more complex funtion for working with symmetric and assymetric matrices
# 				[2] Should I keep the Helper functions external for the option of direct use? [Current Decision: No -> Use one method]
# 		'''
# 		## -- Helper Functions -- ##
# 		def proximity_matrix_symmetric(self):
# 			'''
# 				Compute Symmetric Proximity Matrix 
# 				Note: This is the same as using the STANDARD proximity_matrix() method
# 			'''
# 			return self.proximity_matrix(fillna=fillna, clear_temp=clear_temp, verbose=verbose) 		 			#Calling an External Function, Pass Parameters							

# 		def proximity_matrix_asymmetric(self):
# 			'''
# 				ProductSpace Function to Compute an Assymetric Proximity Matrix
# 				Notes:
# 				-----
# 					[1] This have not been optimised with NUMBA
# 			'''
# 			num_products = len(self.products)
# 			# Initialise NumPy Array as results collector    
# 			self.proximity = np.zeros((num_products, num_products))
# 			self.temp['data'] = self.mcp.T.as_matrix()           
# 			self.temp['col_sums'] = self.mcp.sum().values        				#Row Vector#
# 			for index1 in xrange(0,num_products):
# 			    for index2 in xrange(0,num_products):                      
# 			        cond_prob = (self.temp['data'][index1] * self.temp['data'][index2]).sum() / self.temp['col_sums'][index2]   	# ProductCode 1 by convention (PP')/P [Nb: This is index2 in Numpy]
# 			        self.proximity[index1][index2] = cond_prob
# 			# Return DataFrame Representation		
# 			self.proximity = pd.DataFrame(self.proximity, index=self.products, columns=self.products)
# 			self.proximity.index.name = 'productcode1'
# 			self.proximity.columns.name = 'productcode2'
# 			self.proximity_notes = 'assymetric'
# 			if verbose: print "Index: %s (%s); Columns: %s (%s)" % (self.proximity.index.name, len(self.proximity.index), self.proximity.columns.name, len(self.proximity.columns))
# 			## - Fill Na Option - ##
# 			if fillna:
# 				self.proximity.fillna(0.0)
# 			## - Remove Temp Data - ##
# 			if clear_temp:
# 				del self.temp['col_sums']
# 				del self.temp['data']
# 			self.proximity.name = 'Proximity'
# 			return self.proximity

# 		def proximity_matrix_minmax(self):
# 			'''
# 				ProductSpace Function to Compute a MinMax Proximity Matrix
# 				Notes:
# 				-----
# 					[1] This have not been optimised with NUMBA
# 			'''
# 			num_products = len(self.products)
# 			# Initialise NumPy Array as results collector    
# 			self.proximity = np.zeros((num_products, num_products))
# 			self.temp['data'] = self.mcp.T.as_matrix()           
# 			self.temp['col_sums'] = self.mcp.sum().values        			#Row Vector#
# 			for index1 in xrange(0,num_products):
# 			    for index2 in xrange(0,num_products):
# 			        joint_export_num = (self.temp['data'][index1] * self.temp['data'][index2]).sum()
# 			        if index2 < index1: 													#Placing Max Values in Top Right Diagonal Quadrant
# 			            max_exports_num  = max(self.temp['col_sums'][index1], self.temp['col_sums'][index2])
# 			            cond_prob = joint_export_num / max_exports_num
# 			        else: 																	#Placing Min Values in Top Right Diagonal Quadrant
# 			            min_exports_num = min(self.temp['col_sums'][index1], self.temp['col_sums'][index2])
# 			            cond_prob = joint_export_num / min_exports_num
# 			        self.proximity[index1][index2] = cond_prob
# 			# Return DataFrame Representation
# 			self.proximity = pd.DataFrame(self.proximity, index=self.products, columns=self.products)
# 			self.proximity.index.name = 'productcode1'
# 			self.proximity.columns.name = 'productcode2'
# 			self.proximity_notes = 'minmax'
# 			if verbose: print "Index: %s (%s); Columns: %s (%s)" % (self.proximity.index.name, len(self.proximity.index), self.proximity.columns.name, len(self.proximity.columns))
# 			## - Fill Na Option - ##
# 			if fillna:
# 				self.proximity.fillna(0.0)
# 			## - Remove Temp Data - ##
# 			if clear_temp:
# 				del self.temp['col_sums']
# 				del self.temp['data']
# 			self.proximity.name = 'Proximity'
# 			return self.proximity

# 		## -- Funtion Code -- ##
# 		if type(self.mcp) != pd.DataFrame:
# 			if verbose: print "Mcp matrix at (self.mcp) is currently not available. Computing Mcp (with default KWARGS)"
# 			self.mcp = self.mcp_matrix()
# 		# - Output - #
# 		if matrix_type == 'symmetric':
# 			return proximity_matrix_symmetric(self)
# 		elif matrix_type == 'asymmetric':
# 			return proximity_matrix_asymmetric(self)
# 		elif matrix_type == 'minmax':
# 			return proximity_matrix_minmax(self)
# 		else:
# 			raise ValueError("Proximity type must be either symmetric, asymmetric, or minmax")

# 	### --- Ubiquity and Diversity --- ###
# 	######################################

# 	def compute_ubiquity(self, verbose=False):
# 		'''
# 			Compute Ubiquity from Mcp Matrix (self.mcp)
# 		'''
# 		if type(self.mcp) != pd.DataFrame: 																			#Assume Mcp has been computed. Improve this
# 			if verbose: print "No Mcp Matrix at self.mcp. Running mcp_matrix() method with default kwargs"
# 			self.mcp = self.mcp_matrix()
# 		self.ubiquity = self.mcp.sum()
# 		self.ubiquity.name = 'ubiquity'
# 		self.ubiquity.index.name = 'productcode'
# 		return self.ubiquity

# 	def compute_diversity(self, verbose=False):
# 		'''
# 			Compute Diversity from Mcp Matrix (self.mcp)
# 		'''
# 		if type(self.mcp) != pd.DataFrame:
# 			if verbose: print "No Mcp Matrix at self.mcp. Running mcp_matrix() method with default kwargs"
# 			self.mcp = self.mcp_matrix()
# 		self.diversity = self.mcp.sum(axis=1)
# 		self.diversity.name = 'diversity'
# 		self.diversity.index.name = 'country'
# 		return self.diversity

# 	### --- ECI, PCI, Kcn, Kpn Complexity Measures --- ###
# 	######################################################

# 	## -- Using Matrix Structures -- ##
# 	###################################


# 	def compute_mcc(self, verbose=False):
# 		'''
# 			Header Function for Computing Mcc Matrices
# 			Notes:
# 			------
# 				[1] Current Options are Pandas, Numba 

# 		'''
# 		return self.compute_mcc_numba(verbose)


# 	def compute_mcc_numba(self, clear_temp=True, verbose=False):
# 		'''
# 			Compute Mcc Matrix
# 			Notes:
# 			------
# 				[1] If Diversity and Ubiquity then there exists an Mcp and Product List so no need to check self.mcp and self.products
# 		'''

# 		# - Helper Functions - #

# 		@autojit
# 		def mcc_numba(mcp):
# 			'''
# 				Compute Computational Expensive Loops for Mcc Calculations

# 				Notes:
# 				-----
# 					[1] This could have been implimented as an Mxx routine, but for clarity I have opted to have separate mcc and mpp numba routines
# 					[2] Why do the explicit loops slow NUMBA down a lot? [Maybe NumbaPro is parallising the .sum() methods etc.]
# 			'''
# 			C = mcp.shape[0]
# 			P = mcp.shape[1]
# 			# - Row Sums - #								
# 			RS = mcp.sum(axis=1) 							# - sum() seems to work faster - #
# 				# RS = np.zeros((C,), dtype=np.int) 		# - Row Sum - #
# 				# for c in range(C):
# 				# 	for p in range(P):
# 				# 		RS[c] += mcp[c,p]
# 			# - Column Sum - # 
# 			CS = mcp.sum(axis=0) 							# - sum() seems to work faster - #
# 				# CS = np.zeros((P,), dtype=np.int) 		# - Column Sum - #
# 				# for p in range(P):
# 				# 	for c in range(C):
# 				# 		CS[p] += mcp[c,p]

# 			Mcc = np.empty((C, C), dtype=np.float)
# 			for c in range(C): 								#Compute Combinations
# 				for c_prime in range(C):
# 					num = mcp[c] * mcp[c_prime] 				# Vector Multiply
# 					den = RS[c] * CS 							# Row Sum for Item / Vector of Column Sums 
# 					Mcc[c][c_prime] = (num / den).sum()
			
# 			####################################################
# 			# - Why does this slow down NUMBA significantly? - #
# 			####################################################
# 			# Mcc = np.empty((C,C), dtype=np.float)
# 			# for c in range(C):
# 			# 	for c_prime in range(C):
# 			# 		# - Numerator Vector Multiplication - #
# 			# 		num = np.empty((P,), dtype=np.int)
# 			# 		for p in range(P):
# 			# 			num[p] = mcp[c,p] * mcp[c_prime, p]
# 			# 		# - Denominator Vector Multiplication - #
# 			# 		den = np.empty((P,), dtype=np.int)
# 			# 		for p in range(P):
# 			# 			den[p] = RS[c] * CS[p]
# 			# 		# - Numerator / Denominator Vector Multiplications - #
# 			# 		num_div_den = np.empty((P,), dtype=np.float)
# 			# 		for p in range(P):
# 			# 			num_div_den[p] = num[p] / den[p]
# 			# 		# - Result Sum over Vector - #
# 			# 		result = 0
# 			# 		for p in range(P):
# 			# 			result += num_div_den[p]
# 			# 		Mcc[c][c_prime] = result
# 			#####################################################
# 			return Mcc

# 		# - Function Code - #
# 		# - Parse Required Data - #
# 		if type(self.diversity) != pd.Series:
# 			if verbose: print "self.diversity doesn't contain a pd.Series ... running self.compute_diversity() with default kwargs" # Currently these aren't used #
# 			self.compute_diversity()
# 		if type(self.ubiquity) != pd.Series:
# 			if verbose: print "self.ubiquity doesn't contain a pd.Series ... running self.compute_ubiquity() with default kwargs"
# 			self.compute_ubiquity()
# 		## -- Compute Mcc -- ##
# 		if verbose: print "Computing: Mcc"
		
# 		# - Prepare Data for Numba - # 														#-Preparation Required to Handle np.Nan (Alternative would be to fill np.nan with 0.0)-#
# 		self.temp['data'] = self.remove_zero_relationships_matrix(self.mcp)
# 		self.temp['cindex'] = self.temp['data'].index

# 		## --- NUMBA CALL --- ###
# 		Mcc = mcc_numba(self.temp['data'].as_matrix()) 										
# 		## --- END NUMBA CALL --- ###
	
# 		# Return DataFrame Representation #
# 		Mcc = pd.DataFrame(Mcc, index=copy.deepcopy(self.temp['cindex']), columns=copy.deepcopy(self.temp['cindex'])) 
# 		Mcc.index.name = 'country'
# 		Mcc.columns.name = 'country_prime'

# 		# - Reinstate Removed Rows and Columns - #
# 		Mcc = Mcc.reindex_axis(labels=copy.deepcopy(self.mcp.index), axis=0).reindex_axis(labels=copy.deepcopy(self.mcp.index), axis=1)  # - Restore np.nan cases for Global Dynamic Panels - #

# 		# - Clear Temp - #
# 		if clear_temp:
# 			del self.temp['data']
# 			del self.temp['cindex']

# 		self.mcc = Mcc
# 		return Mcc

	
# 	def compute_mcc_pandas(self, verbose=False):
# 		'''
# 			Compute Mcc Matrix
# 			Notes:
# 			------
# 				[1] If Diversity and Ubiquity then there exists an Mcp and Product List so no need to check self.mcp and self.products

# 		'''
# 		## -- Parse Data Requirements: ubiquity, diversity, self.countries, self.mcp -- ##
# 		if type(self.diversity) != pd.Series:
# 			if verbose: print "self.diversity doesn't contain a pd.Series ... running self.compute_diversity() with default kwargs"
# 			self.compute_diversity()
# 		if type(self.ubiquity) != pd.Series:
# 			if verbose: print "self.ubiquity doesn't contain a pd.Series ... running self.compute_ubiquity() with default kwargs"
# 			self.compute_ubiquity()
# 		## -- Compute Mcc -- ##
# 		if verbose: print "Computing: Mcc"
# 		country_list = self.countries
# 		Mcc = pd.DataFrame(np.empty([len(self.countries), len(self.countries)]), index=self.countries, columns=self.countries)  #Start with Zero's in DataFrame
# 		for country in self.countries:                           #Country
# 			for country_prime in self.countries:                 #Country Prime
# 				numerator = self.mcp.ix[country].mul(self.mcp.ix[country_prime])
# 				denominator = self.diversity[country] * (self.ubiquity)                   # Check This #
# 				Mcc.set_value(index=country, col=country_prime, value=numerator.div(denominator).sum())
# 				#Mcc[country_prime].ix[country] = numerator.div(denominator).sum() 		  # Is this Efficient? => Changed to set_value method#
# 		self.mcc = Mcc
# 		return Mcc


# 	def compute_eci(self, use_scipy=True, verbose=False):
# 		'''	
# 			Compute Country (Economic) Complexity (EigenVector, EigenValues Method)
# 			Notes:
# 			------
# 				[1] Convention of ECI => High ECI == High Complexity [TODO: Check this during error testing then delete comment]
# 				[2] Not finding a big difference between %timeit results between numpy and scipy
# 		'''
# 		## -- Check Required Data -- ##
# 		if type(self.mcc) != pd.DataFrame:
# 			if verbose: print "self.mcc is not a DataFrame ... running self.compute_mcc() with default kwargs"
# 			self.compute_mcc()
# 		## -- Compute ECI -- ##
# 		if verbose: print "Computing: ECI"
# 		Mcc = self.mcc.fillna(0.0) 												
# 		if use_scipy:
# 			eig_val, eig_vect = linalg.eig(Mcc.as_matrix())
# 		else:
# 			eig_val, eig_vect = np.linalg.eig(Mcc.as_matrix())                  #POSSIBLE ISSUE: NEED TO CONFIRM RESULT RETURNED IN SORTED ORDER?
# 																				#eig_val_sorted = np.sort(eig_val)
# 																				#eig_vect_sorted = eig_vect[eig_val.argsort()]
# 		ECI = (eig_vect[:,1] - eig_vect[:,1].mean())/(eig_vect[:,1].std())
# 																				#ECI = -1 * ECI                                                              
# 		ECI = pd.DataFrame(ECI, index=self.countries, columns=['ECI'])
# 		if verbose: 
# 			print "Test EigValue and EigVector"
# 			lh = np.dot(Mcc.as_matrix(), eig_vect)
# 			rh = eig_val * eig_vect
# 			test_result = (lh == rh)
# 			print "Left Hand: %s" % lh
# 			print "Right Hand: %s" % rh
# 			print "Test Result: %s" % test_result
# 		self.eci = ECI['ECI']								#Store as Pd.Series
# 		return self.eci

# 	def compute_mpp(self, verbose=False):
# 		'''
# 			Compute Mpp Matrix
# 			Notes:
# 			------
# 				[1] If Diversity and Ubiquity then there exists an Mcp and Product List so no need to check self.mcp and self.products
# 		'''
# 		return self.compute_mpp_numba(verbose)


# 	def compute_mpp_numba(self, clear_temp=True, verbose=False):
# 		'''
# 			Compute Mpp Matrix (Numba Function)
			
# 			Notes:
# 			------
# 				[1] If Diversity and Ubiquity then there exists an Mcp and Product List so no need to check self.mcp and self.products
# 		'''

# 		# - Helper Functions - #

# 		@autojit
# 		def mpp_numba(mpc):
# 			'''
# 				Compute Computational Expensive Loops for Mpp Calculations

# 				Notes:
# 				-----
# 					[1] This could have been implimented as an Mxx routine, but for clarity I have opted to have separate mcc and mpp numba routines
# 			'''
# 			P = mpc.shape[0]
# 			C = mpc.shape[1]
# 			# - Row Sums - #								
# 			RS = mpc.sum(axis=1) 							# - sum() seems to work faster - #
# 				# RS = np.zeros((P,), dtype=np.int) 		# - Row Sum - #
# 				# for p in range(P):
# 				# 	for c in range(C):
# 				# 		RS[p] += mpc[p,c]
# 			# - Column Sums - #
# 			CS = mpc.sum(axis=0) 							# - sum() seems to work faster - #
# 				# CS = np.zeros((C,), dtype=np.int) 		# - Column Sum - #
# 				# for c in range(C):
# 				# 	for p in range(P):
# 				# 		CS[c] += mpc[p,c]
# 			Mpp = np.empty((P, P), dtype=np.float)
# 			for p in range(P): 								#Compute Combinations
# 				for p_prime in range(P):
# 					num = mpc[p] * mpc[p_prime] 				# Vector Multiply
# 					den = RS[p] * CS 							# Row Sum for Item / Vector of Column Sums 
# 					Mpp[p][p_prime] = (num / den).sum()
# 			return Mpp

# 		## -- Parse Data Requirements: ubiquity, diversity-- ##
# 		if type(self.diversity) != pd.Series:
# 			if verbose: print "self.diversity doesn't contain a pd.Series ... running self.compute_diversity() with default kwargs"
# 			self.compute_diversity()
# 		if type(self.ubiquity) != pd.Series:
# 			if verbose: print "self.ubiquity doesn't contain a pd.Series ... running self.compute_ubiquity() with default kwargs"
# 			self.compute_diversity()
# 		## -- Compute Mpp -- ##
# 		if verbose: print "Computing: Mpp"
# 		# - Prepare Data for Numba - # 														#-Preparation Required to Handle np.Nan (Alternative would be to fill np.nan with 0.0)-#
# 		self.temp['data'] = self.remove_zero_relationships_matrix(self.mcp)
# 		self.temp['pindex'] = self.temp['data'].columns
		
# 		## -- NUMBA FUNCTION -- ##
# 		Mpp = mpp_numba(self.temp['data'].T.as_matrix())
# 		## -- END NUMBA -- ##

# 		# Return DataFrame Representation #
# 		Mpp = pd.DataFrame(Mpp, index=copy.deepcopy(self.temp['pindex']), columns=copy.deepcopy(self.temp['pindex'])) 
# 		Mpp.index.name = 'productcode'
# 		Mpp.columns.name = 'productcode_prime'

# 		# - Reinstate Removed Rows and Columns - #
# 		Mpp = Mpp.reindex_axis(labels=copy.deepcopy(self.mcp.columns), axis=0).reindex_axis(labels=copy.deepcopy(self.mcp.columns), axis=1)  # - Restore np.nan cases for Global Dynamic Panels - #

# 		# - Clear Temp - #
# 		if clear_temp:
# 			del self.temp['data']
# 			del self.temp['pindex']

# 		self.mpp = Mpp
# 		return Mpp	


# 	def compute_mpp_pandas(self, verbose=False):
# 		'''
# 			Compute Mpp Matrix
# 			Notes:
# 			------
# 				[1] If Diversity and Ubiquity then there exists an Mcp and Product List so no need to check self.mcp and self.products

# 			Future Work:
# 			-----------
# 				The loops could be optimised with Numba
# 		'''
# 		## -- Parse Data Requirements: ubiquity, diversity-- ##
# 		if type(self.diversity) != pd.Series:
# 			if verbose: print "self.diversity doesn't contain a pd.Series ... running self.compute_diversity() with default kwargs"
# 			self.compute_diversity()
# 		if type(self.ubiquity) != pd.Series:
# 			if verbose: print "self.ubiquity doesn't contain a pd.Series ... running self.compute_ubiquity() with default kwargs"
# 			self.compute_ubiquity()
# 		## -- Compute Mpp -- ##
# 		if verbose: print "Computing: Mpp"
# 		Mpp = pd.DataFrame(np.empty([len(self.products), len(self.products)]), index=self.products, columns=self.products)  #Start with Zero's in DataFrame
# 		Mpc = self.mcp.T                                            																#Need to turn original Mcp such that Products are Index's
# 		for product in self.products:                           #Product
# 			for product_prime in self.products:                 #Product Prime
# 				numerator = Mpc.ix[product].mul(Mpc.ix[product_prime])         
# 				denominator = self.ubiquity[product] * self.diversity                   
# 				Mpp.set_value(index=product, col=product_prime, value=numerator.div(denominator).sum())
# 				#Mpp[product_prime].ix[product] = numerator.div(denominator).sum() 									#Improved Efficiency using set_value() method
# 		self.mpp = Mpp
# 		return Mpp		

# 	def compute_pci(self, use_scipy=True, verbose=False):
# 		'''	
# 			Compute Product Complexity (EigenVector, EigenValues Method)
# 			Notes:
# 			------
# 				[1] Convention of PCI => High PCI == High Complexity [TODO: Check this during error testing then delete comment]
# 				[2] Not finding a big difference between %timeit results between numpy and scipy
# 		'''
# 		## -- Check Required Data -- ##
# 		if type(self.mpp) != pd.DataFrame:
# 			if verbose: print "self.mpp is not a DataFrame ... running self.compute_mpp() with default kwargs"
# 			self.compute_mpp()
# 		## -- Compute PCI -- ##
# 		if verbose: print "Computing: PCI"
# 		Mpp = self.mpp.fillna(0.0)                                   #Note: Compare with Hidalgo
# 		if use_scipy:
# 			eig_val, eig_vect = linalg.eig(Mpp.as_matrix())
# 		else:	
# 			eig_val, eig_vect = np.linalg.eig(Mpp.as_matrix())                      #POSSIBLE ISSUE: NEED TO CONFIRM RESULT RETURNED IN SORTED ORDER?
# 		PCI = (eig_vect[:,1] - eig_vect[:,1].mean())/(eig_vect[:,1].std())
# 																				#PCI = -1 * PCI                                                       
# 		#WORKING HERE#
# 		#Keep its Magnitude (Or Real Component?)
# 		PCI_real = PCI.real   
# 		PCI_mag = abs(PCI)
# 		if verbose: print "Testing PCI_real vs PCI_mag: %s" % (PCI_real == PCI_mag)
# 		#FINISH WORKING HERE#
# 		PCI = pd.DataFrame(PCI_real, index=self.products, columns=['PCI'])
# 		if verbose: 
# 			print "Test EigValue and EigVector"
# 			lh = np.dot(Mpp.as_matrix(), eig_vect)
# 			rh = eig_val * eig_vect
# 			test_result = (lh == rh)
# 			print "Left Hand: %s" % lh
# 			print "Right Hand: %s" % rh
# 			print "Test Result: %s" % test_result
# 		self.pci = PCI['PCI'] 											#Save as Pd.Series
# 		return self.pci


# 	def compute_iterated_countryproduct_complexity(self, cpweights=(None,None), iterations=20, max_iterations=50, verbose=False):
# 		'''
# 			Compute Country Complexity and Product Complexity by Iterating (Method of Reflections) using Ubiquity and Diversity

# 			Options:
# 			-------
# 				[1] iterations 	:  	Int() 				number of iterations 	[Default Value = 20]
# 									'rank_stability' 	iterate until Rank Stability is Achieved in Countries (Kcn) or ProductSpace (Kpn)
# 									'rank_stability_country' 	iterate until Rank Stability is Achieved in Countries (Kcn)
# 									'rank_stability_product' 	iterate until Rank Stability is Achieved in Products (Kpn)
# 				[2] cpweights 	: 	Allows Specification of Weight Matrix (i.e. Export Shares)

# 			Notes:
# 			-----
# 				[1] Stop Condition is when the RANK of country complexity OR product complexity doesn't change {Should this be AND?}
# 				[2] Method of Reflections - this will store each step in the iterated process in a DataFrame
# 				[3] For SITC4 data, ~19 iterations is what drives stability in Hidalgo (2008?). This is the default value for the method
# 			  **[4] I check previous iteration K(n-1) of each Kpn and Kcn as they are two seperate series I don't need to check 2 series ago (as is done in Hidalgo with a mixed matrix)!

# 			Future Work:
# 			-----------
# 				[1] Move Helper Functions (i.e. check_index_ranking_series()) to a DataFrame Class with Helper Functions and call df.check_rank_index_ranking_series(). This way all functions can use them and they appropriately organised. 
# 				[2] Incorporate Weighted Iteration. {Should this be normalised or should I weight the initial distribtuion only?} 
# 				[3] Country Share Filter (cutoff = 1/num_products) filter products that are > than a uniform distribution over all products as an indicator of specializer
# 		'''
# 		## -- Helper Function -- ##

# 		def check_index_ranking_series(series_one, series_two):
# 		    ''' Check Ranking of Two Independent Series (Commonly indexed) to see if they are the same ordering after sorting
# 		        Status: In-TESTING
# 		        Return True if Index Ranking is the SAME, False if Index Ranking is Different
# 		        Note: Could turn this into recieving a list of series [Future Work]
# 		    '''
# 		    sorted_s1 = series_one.copy().order()
# 		    sorted_s2 = series_two.copy().order()
# 		    return sorted_s1.index.equals(sorted_s2.index)

# 		## -- Function Code -- ##

# 		## -- Check Data is Available -- ##
# 		if type(self.diversity) != pd.Series:
# 			if verbose: print "self.diversity not found ... executing self.compute_diversity()"
# 			self.compute_diversity(verbose=verbose) 	# DataFrame of Diversity
# 		if type(self.ubiquity) != pd.Series:
# 			if verbose: print "self.ubiquity not found ... executing self.compute_diversity()"
# 			self.compute_ubiquity(verbose=verbose) 		# DataFrame of Ubiquity
# 		if type(cpweights) == tuple:
# 			cweights, pweights = cpweights
# 		## -- Construct Kcn, and Kpn with Starting Values -- ##
# 		Kcn = pd.DataFrame(self.diversity)
# 		Kcn.columns = pd.Index(['Kc0'])  		
# 		Kpn = pd.DataFrame(self.ubiquity)
# 		Kpn.columns = pd.Index(['Kp0'])
# 		for num in range(1,max_iterations+1,1):
# 			if verbose: print "[MethodOfReflections] Iteration: %s" % num
# 			# - Weighted Option - #	
# 			# - Global Weights - #
# 			if type(cpweights) == pd.DataFrame:
# 				if verbose: print "[Info] Computed Global Weighted Method of Reflections"
# 				if cpweights.shape != self.mcp.shape:
# 					raise ValueError("[Error] Weights Matrix is not the same shape as the Mcp Matrix")
# 				Kcn['Kc'+ str(num)] = self.mcp.mul(Kpn['Kp'+str(num-1)]).mul(cpweights).sum(axis=1).div(Kcn['Kc0']) 		#.mul(cweights)
# 				Kpn['Kp'+ str(num)] = self.mcp.T.mul(Kcn['Kc'+str(num-1)]).mul(cpweights.T).sum(axis=1).div(Kpn['Kp0'])  		
# 			# - Country and Product Weights - #
# 			# More Work Needed here to establish if this is the correct weighting regime - #
# 			elif type(cweights) == pd.Series and type(pweights) == pd.Series:
# 				if verbose: print "[Info] Computing Country, Product Weighted Method of Reflections"
# 				if len(cweights) != len(self.mcp.index) or len(pweights) != len(self.mcp.columns):
# 					raise ValueError("[Error] Weights Matrices are not the same shape as Mcp Matrix")
# 				### ---> IN WORK <--- ###
# 				Kcn['Kc'+ str(num)] = self.mcp.mul(Kpn['Kp'+str(num-1)]).sum(axis=1).mul(cweights).div(Kcn['Kc0']) 		#.mul(cweights)
# 				Kpn['Kp'+ str(num)] = self.mcp.T.mul(Kcn['Kc'+str(num-1)]).sum(axis=1).mul(pweights.T).div(Kpn['Kp0'])
# 				# Kcn['Kc'+ str(num)] = self.mcp.mul(Kpn['Kp'+str(num-1)]).mul(pweights).sum(axis=1).div(Kcn['Kc0']) 		#.mul(cweights)
# 				# Kpn['Kp'+ str(num)] = self.mcp.T.mul(Kcn['Kc'+str(num-1)]).mul(cweights.T).sum(axis=1).div(Kpn['Kp0'])  #.mul(pweights.T) # Transpose to get Products as Rows, could also use axis
# 				### ----------------- ###
# 			# - Unweighted Iteration - #
# 			else:
# 				Kcn['Kc'+ str(num)] = self.mcp.mul(Kpn['Kp'+str(num-1)]).sum(axis=1).div(Kcn['Kc0'])
# 				Kpn['Kp'+ str(num)] = self.mcp.T.mul(Kcn['Kc'+str(num-1)]).sum(axis=1).div(Kpn['Kp0'])           #mcp_year.T to get Products down the Index Axis in DataFrame
# 			if num < 2:    		
# 				continue
# 			# - Iterator Types - #
# 			if iterations == 'rank_stability':
# 				## -- Iterate Until Rank Stability is Achieved in Kcn or Kpn -- ##
# 				if check_index_ranking_series(Kcn['Kc'+str(num-2)], Kcn['Kc'+str(num)]) and check_index_ranking_series(Kpn['Kp'+str(num-2)], Kpn['Kp'+str(num)]):     #Need to Use two panels apart due to Method of Reflections
# 					if verbose: print "Rank Convergence Achieved after %s iterations with Rank Stability in Kcn and Kpn" % num
# 					break
# 			elif iterations == 'rank_stability_country':
# 				## -- Iterate Until Rank Stability in Kcn -- ##
# 				if check_index_ranking_series(Kcn['Kc'+str(num-2)], Kcn['Kc'+str(num)]):
# 					if verbose: print "Rank Convergence Achieved after %s iterations with Rank Stability in Kcn" % num
# 					break
# 			elif iterations == 'rank_stability_product': 
# 				## -- Iterate Until Rank Stability in Kcp -- ##
# 				if check_index_ranking_series(Kpn['Kp'+str(num-2)], Kpn['Kp'+str(num)]):
# 					if verbose: print "Rank Convergence Achieved after %s iterations with Rank Stability in Kpn" % num
# 					break
# 			else:
# 				## -- Integer Iteration -- ##
# 				if num == iterations: 				#This Iteration has already been computed
# 					break
# 		self.kcn = Kcn
# 		self.kpn = Kpn
# 		return Kcn, Kpn
	

# 	### -- ProductSpace: Network Functions -- ##
# 	############################################

# 	def network_iterated_countryproduct_complexity(self, verbose=False):
# 		'''
# 			A Method that operates on a BiPartiteGraph (C-P Mapping)
# 			Benefits: Allow Weighted Method of Reflections through Edge Data (i.e. Export Shares in Country Export Vector)
# 		'''
# 		raise NotImplementedError




# 	##################################
# 	## -- Data Reshape Functions -- ##
# 	##################################

# 	def as_cp_matrix(self, matrix_type='pandas', value_name='export', verbose=False):
# 		'''
# 			Construct and Return an Value Matrix that is Country x Product (CP)
			
# 			Options:
# 			--------
# 				[1] matrix_type  	Output Matrix Type (Pandas / Numpy)
# 				[2] value 			Edge Attribute (Default: 'export')

# 			Future Work:
# 			------------
# 				[1] Add source for generating an mcp of different types [source = 'rca' or self.rca]
# 				[2] Add Filter to Allow Masking of the Matrix Values
# 		'''
# 		# - Construct from self.data - #
# 		if type(self.data) == pd.DataFrame:
# 			if verbose: print "Computing cp matrix from self.data"
# 			self.cp_matrix = self.data.unstack()
# 			if matrix_type == 'pandas':
# 				return self.cp_matrix
# 			else:
# 				raise NotImplementedError
# 		# - Construct from Network data Structure - #
# 		elif self.network_type == 'BiPartiteGraph':
# 			if verbose: print "Computing cp matrix from self.network"
# 			cntrys = set(n for n,d in self.network.nodes(data=True) if d['bipartite'] == 'countries') 									#Could Rely on self.countries and self.products?
# 			prods = set(self.network) - cntrys
# 			if matrix_type == 'pandas':
# 				m = pd.DataFrame(np.zeros((len(cntrys), len(prods))), index = sorted(list(cntrys)), columns=sorted(list(prods)))
# 				## Note: Can this be vectorized? ##
# 				for c in cntrys:
# 					for p in self.network.neighbors(c):
# 						m.set_value(index=c, col=p, value=self.network[c][p][value_name])  #Value Should be Edge Value
# 				# self.matrix = m
# 				# self.matrix_type = 'cp'
# 				self.cp_matrix = m
# 				return self.cp_matrix
# 			else:
# 				raise NotImplementedError
# 				pass
# 		elif self.network_type == 'MultiDiGraph':
# 			raise NotImplementedError
# 		else:
# 			raise ValueError("self.data must be Long DataFrame or Network Type must be BiPartiteGraph or MultiDiGraph")

# 	def as_adj_matrix(self, matrix_type='pandas', verbose=False):
# 		'''
# 			Convert Network into Adjacency Matrix 
# 			Types: Numpy Matrix or Pandas DataFrame
# 		'''
# 		if self.network == None:
# 			raise ValueError("Network Representation of Data must be computed")
# 		if verbose: print "Computing Adjacency Matrix for Network in self.network"
# 		m = nx.to_numpy_matrix(self.network) 					#Ordering based on G.nodes()
# 		if matrix_type == 'numpy':
# 			# self.matrix = m
# 			# self.matrix_type = 'adjacency'
# 			self.adj_matrix = m
# 			return m
# 		elif matrix_type == 'pandas':
# 			nds = self.network.nodes()
# 			m = pd.DataFrame(m, columns=[nds], index=[nds]).sort_index(axis=0).sort_index(axis=1)
# 			# self.matrix = m
# 			# self.matrix_type = 'adjacency'
# 			self.adj_matrix = m
# 			return m
# 		else:
# 			raise ValueError("matrix_type -> must be pandas or numpy")		

# 	def sorted_matrix(self, df_matrix, row_sortby=None, row_ascending=True, column_sortby=None, column_ascending=True, strict_index=False, verbose=False):
# 		''' 
# 			Sort a Pandas Indexed Matrix
# 			Sort Incoming DataFrame by row or column by passing a series to sort on (Default: Ascending Sort)

# 			Notes: 
# 			------
# 				[1] Default behaviour is 'LEFT' Index Joining. Mcp matrix will be preserved ... if want matched between the two series then need to change default flag 'how' to 'inner' (intersection)
# 				[2] df_matrix can be MultiIndex if there are concordances attached. Keep this function simple and make another method sorted_multiindex_matrix()
# 				[3] This will Filter the results based on that found in the index of row_sortby and column_sortby series
			
# 			Future Work:
# 			-----------
# 			 	[1] This could be made more robust by using REGEXR to see if index1 and index2 contain the same basic info "country" vs "countrycode" - Currently issue's advice due to limited REGEXR exploration
# 				[2] This Function could be re-written with new knowledge of index object creation (but not time urgent)
# 				[3] Incorporate strict_index: where name of index is the same as the name of the incoming sortby series
# 				[4] Reduce Code Complexity by removing column test for row_sortby vector. Could copy df_matrix to sorted_df_matrix and then operate on that item. 
# 				[5] Allow row_sortby and column_sortby to be pd.DataFrame objects with series_name?
# 				[6] Split Function into sorted_matrix_rows() and sorted_matrix_columns() and then get sorted_matrix() to parse the options and call relevant methods to simplify code?
# 		'''
# 		## -- Sort Rows by Index -- ##
# 		if type(row_sortby) == pd.Series:       
# 			InMatrixRowIndex = set(df_matrix.index)
# 			InRowSortbyIndex = set(row_sortby.index)
# 			RowItemsDropped = InMatrixRowIndex - InRowSortbyIndex
# 			if RowItemsDropped: print "[WARNING] Items dropped from Matrix Row Index: %s (Not in row_sortby)" % RowItemsDropped
# 			row_sortby.sort(ascending=row_ascending)
# 			sorted_df_matrix = df_matrix.reindex(index=row_sortby.index)
# 		elif row_sortby == None: 																#Pass if Nothing to Do [Default Value == None]; This will throw a ValueError if DataFrame is passed
# 			pass
# 		else:
# 			raise ValueError("row_sortby must be a pd.Series")
# 		## -- Sort Columns by Index -- ##
# 		if type(column_sortby) == pd.Series:
# 			InMatrixColIndex = set(df_matrix.columns)
# 			InColSortbyIndex = set(column_sortby.index)
# 			ColItemsDropped = (InMatrixColIndex - InColSortbyIndex)
# 			if ColItemsDropped: print "[WARNING] Items dropped from Matrix Column Index: %s (Not in column_sortby)" % ColItemsDropped
# 			column_sortby.sort(ascending=column_ascending)
# 			if type(row_sortby) == pd.Series:  													
# 				sorted_df_matrix = sorted_df_matrix.reindex(columns=column_sortby.index) 		#Sort Already Sorted Matrix by row_matrix 
# 			else: 
# 				sorted_df_matrix = df_matrix.reindex(columns=column_sortby.index) 				#Sort the column index
# 		elif column_sortby == None:
# 			pass 																				#Pass if Nothing to Do [Default Value == None]; This will throw a ValueError if DataFrame is passed
# 		else:
# 			raise ValueError("column_sortby must be a pd.Series")
# 		## -- Return Sorted Data -- ##
# 		if type(row_sortby) != pd.Series and type(column_sortby) != pd.Series:					#Check Null input Condition
# 			raise ValueError("Need to specify a row_sortby series or column_sortby series or both")
# 		return sorted_df_matrix


# 	def sorted_multiindex_matrix(self, df_matrix, row_sortby=None, row_ascending=True, column_sortby=None, column_ascending=True, strict_index=True, verbose=False):
# 		'''
# 			Sort a Pandas Hierarchically Indexed Matrix

# 			row_sortby, column_sortby 		: 	pd.Series or [pd.Series]

# 		  **Status: This Function is currently UNTESTED**

# 			Notes:
# 			-----
# 				[1] To match to relevant level of Index the Incoming Series needs to have the same name as the level in the index!

# 			Future Work:
# 			------------
# 				[1] Construct a general sorted_matrix() that makes use of the current sorted_matrix() for simple matrices and sorted_multiindex_matrix for more complex matrices
# 				[2] Allow row_sortby to be a DataFrame with specified series_name?
# 				[3] Split Function into sorted_multiindex_matrix_rows() and sorted_multiindex_matrix_columns() that is called by sorted_multiindex_matrix() to simplify code?
# 		'''
# 		## -- Helper Functions -- ##
# 		def row_sorter(df_matrix, row_sortby, row_ascending):
# 			## -- Check Input -- ##
# 			if type(row_sortby) != pd.Series: raise ValueError("row_sortby must be a pd.Series") 																#Ensures all Passed List Items are Checked.
# 			if row_sortby.name not in df_matrix.index.levels: raise ValueError("row_sortby.name doesn't match any of the Row index level names")
# 			row_sortby.sort(ascending=row_ascending)
# 			### CHECK THIS ###
# 			sorted_df_matrix = df_matrix.reindex(index=row_sortby.index, level=row_sortby.name)
# 			### END ###
# 			return sorted_df_matrix

# 		def column_sorter(df_matrix, column_sortby, column_ascending):
# 			## - Check Input -- ##
# 			if type(column_sortby) != pd.Series: raise ValueError("column_sortby must be a pd.Series") 															#Ensures all Passed List Items are Checked.
# 			if column_sortby.name not in df_matrix.columns.levels: raise ValueError("column_sortby.name doesn't match any of the Column index level names")
# 			column_sortby.sort(ascending=column_ascending)
# 			### CHECK THIS ###
# 			sorted_df_matrix = df_matrix.reindex(columns=column_sortby.index, level=column_sortby.name)
# 			### END ###
# 			return sorted_df_matrix	

# 		## -- Parse Types -- ##
# 		if df_matrix.index != pd.MultiIndex:
# 			raise ValueError("This Method Requires df_matrix to have a MultiIndex")
# 		## -- Sort Rows -- ##
# 		sorted_df_matrix = df_matrix.copy(deep=True) 												#Prepare a Copy of df_matrix
# 		if type(row_sortby) == pd.Series: 															#Sort One Level
# 			sorted_df_matrix = row_sorter(sorted_df_matrix, row_sortby, row_ascending)
# 		elif type(row_sortby) == list: 																#Sort Multiple Levels Sorting First by Item 1 and Last by Item n
# 			for row_sortby_item in row_sortby:
# 				sorted_df_matrix = row_sortby(sorted_df_matrix, row_sortby_item, row_ascending)
# 		## -- Sort Columns -- ##
# 		if type(column_sortby) == pd.Series:
# 			sorted_df_matrix = column_sorter(sorted_df_matrix, column_sortby, column_ascending)
# 		elif type(column_sortby) == list:
# 			for column_sortby_item in column_sortby: 												#Sort Multiple Levels Sorting First by Item 1 and Last by Item n
# 				sorted_df_matrix = column_sorter(sorted_df_matrix, column_sortby, column_ascending)
# 		## -- Return Sorted Data -- ##
# 		try:  																						#Check Zero Sorting Series Condition
# 			if row_sortby==None and column_sortby == None: raise ValueError("Must specify row_sortby or column_sortby or both")
# 		except:
# 			pass
# 		return sorted_df_matrix

# 	#######################
# 	## Filtering Methods ##
# 	#######################

# 	def supp_data_from_complete_tn(self, series_name='export', verbose=False):
# 		'''
# 			Prepare Supplimentary Data from a complete trade network
# 			Useful for filtering methods so that RCA can still be computed correctly

# 			Future Work:
# 			-----------
# 				[1] Update rca_matrix() to use this method for generating supp_data in object to remove code duplicity
# 		'''
# 		if self.complete_trade_network != True:
# 			raise ValueError("This is Not a Complete Trade Network!")
# 		TotalWorldExport = self.data[series_name].sum()
# 		TotalProductExport = self.data.groupby(level=['productcode']).sum()[series_name] 
# 		TotalCountryExport = self.data.groupby(level=['country']).sum()[series_name]
# 		return TotalWorldExport, TotalProductExport, TotalCountryExport

# 	def filter_for_countries(self, countries, verbose=False):
# 		'''
# 			Filter Cross Section for Specific Countries

# 			Future Work:
# 			-----------
# 				[1] Check if Cntry and Prod Objects will Pass Through This Filter
# 		'''
# 		## -- Reporting Vars -- ##
# 		CntryInDataset = []
# 		## -- Copy of DataFrame -- ##
# 		data = self.data.copy(deep=True)
# 		for cntry in countries:
# 			if cntry in self.data.index.levels[self.data.index.names.index('country')]:          
# 				CntryInDataset.append(cntry)
# 		CntryInDatasetButExcluded = list(set(self.countries) - set(CntryInDataset))
# 		if len(CntryInDataset) == 0: raise ValueError("Country List has resulted in a dataset of 0 countries")
# 		note = "Filtered Dataset of Countries:\n.. Countries in New Dataset: %s\n.. Excluding Countries from Current Dataset: %s\n.. Countries Requested but not in Current Dataset: %s" % (CntryInDataset, CntryInDatasetButExcluded, set(countries) - set(CntryInDatasetButExcluded) - set(CntryInDataset))
# 		if verbose: 
# 			print note
# 		ples = ProductLevelExportSystem()
# 		ples.from_df(data.ix[CntryInDataset], self.country_classification, self.product_classification, ['DataFrame'], self.year, verbose=verbose)
# 		ples.notes = note 
# 		ples.complete_trade_network = False  				# This will always be False by Definition
# 		## -- Load Supp Data for Computing RCA -- ##
# 		TotalWorldExport, TotalProductExport, TotalCountryExport = self.supp_data_from_complete_tn(verbose=verbose)
# 		ples.supp_data['TotalWorldExport'] = TotalWorldExport
# 		ples.supp_data['TotalProductExport'] = TotalProductExport
# 		ples.supp_data['TotalCountryExport'] = TotalCountryExport
# 		return ples

# 	def filter_for_products(self, products, verbose=False):
# 		'''
# 			Filter Cross Section for Specific Products

# 			Future Work:
# 			-----------
# 				[1] Check if Cntry and Prod Objects will Pass Through This Filter
# 		'''
# 		## -- Reporting Vars -- ##
# 		ProdInDataset = []
# 		## -- Copy of DataFrame -- ##
# 		data = self.data.copy(deep=True)
# 		for prod in products:
# 			if prod in self.data.index.levels[self.data.index.names.index('productcode')]:          
# 				ProdInDataset.append(prod)
# 		ProdInDatasetButExcluded = list(set(self.products) - set(ProdInDataset))
# 		if len(ProdInDataset) == 0: raise ValueError("Product List has resulted in a dataset of 0 countries")
# 		note = "Filtered Dataset of Products:\n.. Products in New Dataset: %s\n.. Excluding Products from Current Dataset: %s\n.. Products Requested but not in Current Dataset: %s" % (ProdInDataset, ProdInDatasetButExcluded, set(products) - set(ProdInDatasetButExcluded) - set(ProdInDataset))
# 		if verbose: 
# 			print note
# 		ples = ProductLevelExportSystem()
# 		data = data.reorder_levels(order=['productcode', 'country']).ix[ProdInDataset] 		#Reorder for access through .ix
# 		ples.from_df(data.reorder_levels(order=['country', 'productcode']), self.country_classification, self.product_classification, ['DataFrame'], self.year, verbose=verbose)
# 		ples.notes = note 
# 		ples.complete_trade_network = False  	# This will always be False by Definition
# 		## -- Load Supp Data for Computing RCA -- ##
# 		TotalWorldExport, TotalProductExport, TotalCountryExport = self.supp_data_from_complete_tn(verbose=verbose)
# 		ples.supp_data['TotalWorldExport'] = TotalWorldExport
# 		ples.supp_data['TotalProductExport'] = TotalProductExport
# 		ples.supp_data['TotalCountryExport'] = TotalCountryExport
# 		return ples
	
# 	def remove_zero_relationships_matrix(self, df_matrix, over_index='both', new_df=True, verbose=False):
# 		'''
# 			Remove Zero Relationships from any Matrix DataFrame

# 			Options:
# 			--------
# 				[1] over_index 		: 		'rows', 'columns', or 'both' [Default = 'both']
# 				[2] new_df 			: 		return a Copy of the passed Matrix (So as not to replace internal matrices) [Default = True]

# 		'''
# 		if new_df == True:
# 			df_matrix = df_matrix.copy()
# 		if verbose: print "Removing Zero Relationships over: %s" % over_index
# 		if over_index == 'rows' or over_index == 'both':
# 			for idx, data in df_matrix.iterrows():
# 				if data.sum() == 0: 
# 					df_matrix.drop(labels=[idx], inplace=True)
# 		if over_index == 'columns' or over_index == 'both':
# 			for idx, data in df_matrix.iteritems():
# 				if data.sum() == 0: 
# 					del df_matrix[idx]
# 		if over_index != 'columns' and over_index != 'rows' and over_index != 'both':
# 			raise ValueError("over_index must be specified as both, columns or rows")
# 		return df_matrix

# 	###############################
# 	## -- Concordance Methods -- ##
# 	###############################

# 	def concord_data(data, concord, match_on='productcode', nw_code='community', agg_series='export', aggregate=True, merge_report=True, verbose=False):
# 		''' 
# 			Concord Data and Aggregate to new Concordance Level
		
# 		    Options:
# 		    -------    
# 		    	match_on    : 	Defines the Column or Index Level to match on
# 		    	nw_code     :	Specifies which column is to be the new aggregated index
# 		    	agg_series  :	Specifies which series or list of series to aggregate
# 		    	aggregate 	: 	Aggregate Data or Return MultiIndex
# 		    	merge_report:	Specifies if return a merge report

# 		    Returns: 
# 		    --------
# 		    	DataFrame of Aggregated Series (agg_series) under new code definition (nw_code)

# 		    Notes:
# 		    ------
# 		    	[1] MultiIndex DataFrames? Previous Code Had concord_multiindex_data()
# 		'''
# 		raise NotImplementedError

# ### --- WORKING HERE --- ###

# # def concord_data(data, concord, match_on='productcode', nw_code='community', agg_series='export', report=True, verbose=False):                  #Does this require LONG Data?
# #     ''' Function: concord_data (Concord Data and Aggregate to new Concordance Level)
# #         Status: IN-TESTING
# #         Options:    match_on    -> Defines the Column or Index Level to match on
# #                     nw_code     -> Specifies which column is to be the new aggregated index
# #                     agg_series  -> Specifies which series or list of series to aggregate
# #                     report      -> Specifies if return a merge report
# #         Returns: DataFrame of Aggregated Series (agg_series) under new code definition (nw_code)
# #     '''
# #     #Merge
# #     nw_data = data.reset_index().merge(concord.reset_index(), on=[match_on], how='left').sort(columns=[match_on])    #Merge Concordance to DataSet. [.reset_index() in case data is already indexed]
# #     if report == True:
# #         report_df = pd.DataFrame(data.reset_index()[match_on].describe(), columns=['left'])
# #         report_df = report_df.merge(pd.DataFrame(concord[match_on].describe(), columns=['right']), left_index=True, right_index=True)
# #         set_left = set(data.reset_index()[match_on].unique())
# #         set_right = set(concord[match_on].unique())
# #         set_info = pd.DataFrame({'left':set_left.issubset(set_right), 'right':set_right.issubset(set_left)}, index=['subset'])
# #         report_df = report_df.append(set_info)
# #         set_info = pd.DataFrame({'left':len(set_left.difference(set_right)), 'right':len(set_right.difference(set_left))}, index=['differences'])
# #         report_df = report_df.append(set_info)
# #         left_diff = list(set_left.difference(set_right))[0:5]
# #         if len(left_diff) < 5:
# #             left_diff = (left_diff + [np.nan]*5)[0:5]
# #         right_diff = list(set_right.difference(set_left))[0:5]
# #         if len(right_diff) < 5:
# #             right_diff = (right_diff + [np.nan]*5)[0:5]
# #         set_info = pd.DataFrame({'left': left_diff, 'right': right_diff}, index=['diff1', 'diff2', 'diff3', 'diff4', 'diff5'])
# #         report_df = report_df.append(set_info)
# #     #Aggregation
# #     group = ['year', 'country', nw_code]
# #     nw_data = pd.DataFrame(nw_data.groupby(by=group)[agg_series].sum())  #DataFrame as I want to add Series to it. 
# #     nw_data.index.names = ['year', 'country', 'productcode'] #reset naming convetion back to generic productcode
# #     #Returns
# #     if report == True:
# #         if verbose: print "Returning: new_data and report"
# #         return nw_data, report_df
# #     else:
# #         if verbose: print "Returning: new_data only"
# #         return nw_data

# # def concord_data_multiindex(data, matrix_shape='cp', row_concord='', col_concord='', chapter_level='', how='left', report=True, verbose=False):
# #     ''' Function: Concord Data (But Return Disaggregated Data with MultiIndex)
# #         Status: IN-DEVELOPMENT
# #         Inputs:     data            -> DataFrame containing Data
# #                     row_concord     -> Concordance Series to match to Rows Index
# #                     col_concord     -> Concordance Series to match to Columns Index2
# #                     chapter_level   -> Chapter Level to Merge concordance to [Default is '', chapter_level will be infered by the productcode] It is the Chapter Level of the data, or a higher aggregation
# #         Options:    how         -> Join Style
# #                     report      -> Identifies unmatched Concordance and Data Items
# #         Returns: DataFrame in Original Form with new MultiIndex
# #         Assume: Income Data and Concordances are indexed by the same Codeing System (i.e. Data in the Concordance are the new Classification Codes)
# #                 This only works for two levels.
# #     '''
# #     #Infer Chapter Level to MATCH ON
# #     if str(chapter_level) == '':                                    
# #         if matrix_shape == 'cp' or matrix_shape == 'pp':
# #             if type(data.columns[0] != str):
# #                 print "Warning: Using the Length of the First Element can be dangerous IF the productcode is NOT a string! (Due to Leading Zero's Issue)"
# #             chapter_level = len(data.columns[0])
# #         elif matrix_shape == 'p':
# #             chapter_level = len(data.index[0])                   
# #     elif type(chapter_level) == int:
# #         pass
# #     else:
# #         print "ERROR: Chapter Level is either '' (to infer) or some integer"
# #     #Update row_concord, col_concord to dataframe's (as required below)
# #     if type(row_concord) == pd.Series:
# #         row_concord = pd.DataFrame(row_concord)
# #     if type(col_concord) == pd.Series:
# #         col_concord = pd.DataFrame(col_concord)    
# #     #Concord to New Product Code Scheme
# #     if str(row_concord) != '':
# #         pass
# #         #IMPLIMENT HERE#
# #     if str(col_concord) != '':
# #         #Perform Column Concordance
# #         col_index = pd.DataFrame(data.columns, columns=[data.columns.name])
# #         if len(data.columns[0]) != chapter_level:
# #             col_index['match_on'] = col_index.applymap(lambda x: x[0:chapter_level])
# #         else:
# #             col_index['match_on'] = col_index[data.columns.name]
# #         #Merge
# #         col_index = col_index.merge(col_concord, how=how, left_on=['match_on'], right_index=True)
# #         col_index = col_index.drop(['match_on'], axis=1)
# #         #construct MultiIndex
# #         level1 = []
# #         level2 = []
# #         for row in col_index.iterrows():
# #             level1.append(row[1][col_concord.columns[0]])                                         
# #             level2.append(row[1][data.columns.name])
# #         tuple_list = zip(np.array(level1), np.array(level2))
# #         nw_index = pd.MultiIndex.from_tuples(tuple_list, names=[col_concord.columns[0], data.columns.name])         #COULD CHANGE CONCORD.COLUMNS[0] TO a named Series?
# #         #Create a new Copy of the Data
# #         nw_data = data.copy()
# #         nw_data.columns = nw_index
# #         return nw_data
# #     print "ERROR: Need to pass either a row_concordance or a col_concordance or both"
# #     return None     

# ### --- END WORKING HERE --- ###

# 	#############################
# 	## -- Drawing Functions -- ##
# 	#############################

# 	## -- Matrix Visualisations -- ##
# 	#################################

# 	def plot_mcp(self, row_sortby_label='', row_step=10, column_sortby_label='', column_step=20):
# 		'''
# 			Plot Mcp Matrix (Auto Generated Labels by Step Size)

# 			Options:
# 			--------
# 				row_sortby_label	: Text for Y-Axis Label
# 				row_step 			: Step Size for Automatic Data Label
# 				column_sortby_label : Text for X-Axis Label
# 				column_step 		: Step Size for Automatic Data Labels

# 			Future Work:
# 			------------
# 				[1] savefig option
# 		'''
# 		# - Setup Plot - #
# 		fig = plt.figure(1, figsize=(12,4))
# 		ax1 = fig.add_subplot(1,1,1)
# 		# - Formatting for Figure - #
# 		cmap = colors.ListedColormap(['white', 'black'])
# 		bounds=[0,1,1]
# 		norm = colors.BoundaryNorm(bounds, cmap.N)
# 		mat = ax1.matshow(self.mcp, origin='lower', cmap=cmap, norm=norm)
# 		# - Select Ticks - #
# 		num_countries = len(self.mcp.index)
# 		ytick_step = int(num_countries / row_step) 									
# 		if ytick_step == 0: ytick_step = 1 												#Low Dimensionality Case
# 		yticks_index = range(0,num_countries,ytick_step) 						
# 		num_products = len(self.mcp.columns)
# 		xtick_step = int(num_products / column_step)
# 		if xtick_step == 0: xtick_step = 1 												#Low Dimensionality Case
# 		xticks_index = range(0, num_products, xtick_step)
# 		ax1.set_yticks(yticks_index)
# 		ax1.set_yticklabels(self.mcp.index[yticks_index], fontsize='small')
# 		ax1.set_xticks(xticks_index)
# 		ax1.set_xticklabels(self.mcp.columns[xticks_index], fontsize='small')
# 		# - Construct Titles, Labels, and Notes - #
# 		title = 'Mcp Matrix [Year: ' + str(self.year) + ']'
# 		ax1.set_title(title)
# 		ylabel = 'Countries [Sorted by ' + row_sortby_label + ']'
# 		ax1.set_ylabel(ylabel)
# 		xlabel = 'SITC4 Products [Sorted by ' + column_sortby_label + ']'
# 		ax1.set_xlabel(xlabel)
# 		note = self.product_classification + ': ' + str(num_products) + ' , Countries: ' + str(num_countries)
# 		fig.text(0.72, 0.1, note) 																					#This formating is rather arbitrary!
# 		fig.tight_layout()
# 		return fig

# 	def plot_mcp_simple(self, sortby_row_title='', sortby_col_title=''):
# 		''' 
# 			Produce Simple Mcp Matrix Plots
# 			Notes:
# 			-----
# 				[1] plot_mcp() method is the main plotting function. This is for quick and simple views of the data patterns
# 				[2] Basically This is a matshow() with labels
# 		'''
# 		# Plot Generation    
# 		fig = plt.figure()
# 		ax = fig.add_subplot(1,1,1)
# 		ax.matshow(self.mcp)
# 		ax.set_title('Mcp Matrix')
# 		if sortby_row_title != '': 
# 			ax.set_ylabel('Countries [Sorted by: ' + sortby_row_title + ']')
# 		else:
# 			ax.set_ylabel('Countries')
# 		if sortby_col_title != '': 
# 			ax.set_xlabel('Products [Sorted by: ' + sortby_col_title + ']')
# 		else:
# 			ax.set_xlabel('Products')
# 		return fig

# 	### --- WORKING HERE --- ###

# 	# - Can there be a Default Settings dictionary based on the country and product classification? - #
# 	# - self.plot_settings['SITCR2L4'] = (default_products, default_countries...)

# 	default_cntrys = ['JPN', 'USA', 'DEU', 'KOR', 'ITA', 'AUS', 'BRA', 'THA', 'GHA', 'GBR', 'TWN', 'FRA', 'MEX']
# 	default_products = ['8421', '3330', '7922', '8748', '7810', '7442', '7781']
# 	def plot_mcp_square(self, row_sortby_label='', column_sortby_label='', step=20, label_cntrys=default_cntrys, xlabel_rot=90, label_products=default_products, axs_only=False):
# 		'''
# 			Plot Square Mcp Matrix
			
# 			Future Work:
# 			------------
# 				[1] Can this be integrated into the above plot_mcp() function with aspect option?
# 				[2] Make General for any matrix AND not just Mcp
# 		'''

# 		# - Helper Functions - #
		
# 		def forceAspect(ax,aspect=1):
# 			im = ax.get_images()
# 			extent =  im[0].get_extent()
# 			ax.set_aspect(abs((extent[1]-extent[0])/(extent[3]-extent[2]))/aspect)
		
# 		def index_array(labels, items):
# 			''' 
# 				Take np.array and return appropriate index positions for items
# 			'''
# 			index = []
# 			for item in items:
# 				try:
# 					idx = np.where(labels==item)
# 					index.append(idx[0][0])        #First [0] extracts from tuple, Second[0] extracts from ndarray
# 				except:
# 					print "[WARNING]: %s is not found in sortby or scaleby vectors. Ignoring!" % item
# 					continue 
# 			return np.array(index)

# 		# - Function Code - #
		
# 		if axs_only != False:
# 			ax1 = axs_only
# 		else:
# 			fig = plt.figure(1, figsize=(12,12))
# 		ax1 = fig.add_subplot(1,1,1)
# 		# - Formatting for Graph - #
# 		cmap = colors.ListedColormap(['white', 'black'])
# 		bounds=[0,1,1]
# 		norm = colors.BoundaryNorm(bounds, cmap.N)
# 		mat = ax1.matshow(self.mcp, origin='lower', cmap=cmap, norm=norm)
# 		# - Force Aspect - #
# 		forceAspect(ax1, aspect=1)
# 		# - Select Ticks - #
# 		num_countries = len(self.mcp.index)
# 		num_products = len(self.mcp.columns)
# 		if type(step) == int:
# 			ytick_step = int(num_countries / step)
# 			if ytick_step == 0: ytick_step = 1
# 			yticks_index = range(0,num_countries,ytick_step)
# 			xtick_step = int(num_products / step)
# 			if xtick_step == 0: xtick_step = 1
# 			xticks_index = range(0, num_products, xtick_step)
# 			ax1.set_yticks(yticks_index)
# 			ax1.set_yticklabels(self.mcp.index[yticks_index], fontsize='small')
# 			ax1.set_xticks(xticks_index)
# 			ax1.set_xticklabels(self.mcp.columns[xticks_index], fontsize='small', rotation=xlabel_rot)
# 		else:
# 			yticks_index = index_array(self.mcp.index, label_cntrys)
# 			ax1.set_yticks(yticks_index)
# 			ax1.set_yticklabels(self.mcp.index[yticks_index], fontsize='small')
# 			xticks_index = index_array(self.mcp.columns, label_prods)
# 			ax1.set_xticks(xticks_index)
# 			ax1.set_xticklabels(self.mcp.columns[xticks_index], fontsize='small', rotation=xlabel_rot)
# 		if axs_only != False:
# 			return ax1
# 		# - Add Graph Labels & Titles - #
# 		title = 'Mcp Matrix [Year: ' + str(self.year) + ']'
# 		ax1.set_title(title)
# 		if row_sortby_label == '':
# 			ylabel = 'Countries'
# 		else:
# 			ylabel = 'Countries [Sorted by ' + row_sortby_label + ']'
# 		ax1.set_ylabel(ylabel)
# 		if column_sortby_label == '':
# 			xlabel = 'Products'
# 		else:
# 			xlabel = self.product_classification + ' Products [Sorted by ' + column_sortby_label + ']'
# 		ax1.set_xlabel(xlabel)
# 		note = self.product_classification + ' Products: ' + str(num_products) + ' , Countries: ' + str(num_countries)
# 		fig.text(0.72, 0.05, note) 	# Rather Arbitrary but Works for SITC
# 		fig.tight_layout()
# 		return fig


# 	### --- WORKING HERE --- ###

# 	def plot_heatmap(self, data, row_sortby_label='', row_step=0, column_sortby_label='', column_step=0, data_cutoff=None, figsize=5, aspect=None, notes=''):
# 		'''
# 			Generalised Version of plot_mcp_heatmap and plot_scaled_mcp_heatmap
# 		'''
# 		raise NotImplementedError

# 	def plot_mcp_heatmap(self, cpdata, cpdata_name='Data', row_sortby_label='', row_step=0, column_sortby_label='', column_step=0, xrot=45, data_cutoff=None, \
# 							figsize=5, aspect=(False, False), notes='', rem_zero_rel=True, fillna=True):
# 		'''
# 			Plot Mcp Heatmap (Mcp = Country x Product Matrix)
			
# 				data 		: 		data in (c x p format) [i.e. RCA Matrix]

# 			Options:    
# 			--------
# 				cpdata_name 		: 	Name of Data being Passed in as cpdata
# 				row_sortby_label 	:	Description of Row Sortby for Y-Axis 		(Requires Pre-Sorted cpdata!)
# 				row_step   			: 	Graduations for the step size on the Y-Axis
# 				column_sortby   	: 	Description of Column Sortby for X-Axis 	(Requires Pre-Sorted cpdata!)
# 				column_step     	: 	Graduations for the step size on the X-Axis
# 				data_cutoff		 	: 	Specifies a cutoff for the Data Gradient. [NB: Some RCA VAlues are very large and that degree isn't very important]
# 				figsize     		: 	Specifies in inches the figure size [Default: -1 let's the aspect ratio be chosen by the data dimensions]
# 				aspect  			:  	Specify an Aspect Ratio (X, Y)
# 				notes 				: 	Add Notes to Figure
# 				rem_zero_rel 		: 	Remove Zero Relationships from both X and Y Axis [Default = True]
# 				fillna 				: 	Fill np.NaN values with 0 [Default = True]
			
# 			Notes:
# 			------ 
# 				[1] Allow cmap to be custom made for white 0 to 1 and gradient red from 1 to max()

# 			Future Work:
# 			-----------
# 				[1] Generalised Filter for ColorMap to Allow +/- Bounds (Currently Bounded by 0)
# 		'''
# 		from matplotlib import cm 			# Check if this is imported in the Header of file
# 		# - Pandas Work - #
# 		if fillna:
# 			cpdata = cpdata.fillna(0.0)
# 		if rem_zero_rel: 
# 			cpdata = self.remove_zero_relationships_matrix(cpdata)
# 		countries = cpdata.index
# 		products = cpdata.columns
# 		# - Apply Cutoff - #
# 		if type(data_cutoff) == int:
# 			matr = cpdata.applymap(lambda x: 0 if x < 1.0 else x).applymap(lambda x: data_cutoff if x >= data_cutoff else x).as_matrix()         	#One Sided (Build a Filter Method!)
# 			matr_note = 'Gradient Cutoff >= ' + str(data_cutoff)
# 		else:
# 			matr = cpdata.as_matrix() 
# 			matr_note = ''

# 		# - Numpy Work - #
# 		x = np.array(products)
# 		y = np.array(countries)
# 		xx, yy = np.meshgrid(np.arange(len(products)+1), np.arange(len(countries)+1))
# 		if aspect==(False, False): 																	# Natural Aspect of the Data is Plotted by Default
# 			aspect = ((len(products)/len(countries))*figsize, 1*figsize)
# 		# - Generate Figure - #
# 		fig = plt.figure(figsize=(aspect))
# 		axs = fig.add_subplot(1,1,1)
# 		plot = axs.pcolormesh(xx, yy, matr, cmap=cm.Reds)

# 		axs.set_ylim(0,len(y))
# 		if row_step == 0: row_step = 1														#Display ALL Data [Default]
# 		axs.set_yticks(np.arange(len(countries), step=row_step) + 0.5) 						#Center The Labels
# 		axs.set_yticklabels(y[np.arange(len(countries), step=row_step)])
# 		axs.set_xlim(0,len(x))
# 		axs.xaxis.set_ticks_position('bottom')
# 		if column_step == 0: column_step = 1
# 		axs.set_xticks(np.arange(len(products), step=column_step) + 0.5) 					#Center The Labels
# 		axs.set_xticklabels(x[np.arange(len(products), step=column_step)], rotation=xrot)
# 		# - Construct Colorbar - #
# 		cbar = plt.colorbar(plot, ticks=[0, matr.max()/2, matr.max()]) 			 			#This is bounded by 0 colorbar															
# 		cbar.set_ticklabels([cpdata_name + ' = 0', cpdata_name + ' = '+str(matr.max()/2), cpdata_name + ' >= '+str(int(matr.max()))])
# 		# - Titles and Notes - #
# 		figtitle = "Mcp [" + cpdata_name + " Values] Matrix [Yr: " + str(self.year) + "]"
# 		plt.title(figtitle)
# 		if notes != '':
# 			notes = matr_note + "; "+ notes
# 			plt.figtext(0.8, 0.01, notes) 							#Arbitary?
# 		else:
# 			plt.figtext(0.8, 0.01, matr_note) 						#Arbitary?
# 			plt.tight_layout()
# 		return fig


# ### --- WORKING HERE --- ###

# 	default_cntrys = ['JPN', 'USA', 'DEU', 'KOR', 'ITA', 'AUS', 'BRA', 'THA', 'GHA', 'GBR', 'TWN', 'FRA', 'MEX']
# 	default_prods = ['8421', '3330', '7922', '8748', '7810', '7442', '7781'] 
# 	default_row_labels = ('ECI', 'Export Value Share')
# 	default_column_labels = ('PCI', 'Product Value Share') 
# 	def plot_scaled_mcp_heatmap(self, sorted_cpdata, cpdata_name='Data', row_scaleby=None, column_scaleby=None, row_label=default_row_labels, label_cntrys=default_cntrys, \
# 									column_label=default_column_labels, label_prods=default_prods, low_value_cutoff=None, high_value_cutoff=None, \
# 										 gradient_cutoff=4, size=8, axs_only=False, cmap=cm.Reds, rem_zero_rel=True, fillna=True, verbose=False):
# 		'''
# 			Plot Sorted and Scaled Heatmaps of Any CxP Matrix (Mcp)
# 			[PreSort sorted_cpdata]

# 			Options:
# 			-------
# 				cpdata_name 	: 	Specify a Name for the Data (i.e. RCA) 	[Defualt: Data]
# 				row_scaleby     : 	Specify a row_scaleby series (i.e. GDP or Country Exports etc.)
# 				column_scaleby  : 	Specify a column_scaleby series
# 				row_label       : 	Text for Row (or Y Axis) Type('row_sortby', 'row_scaleby')
# 				label_cntrys    : 	Specify which countries to label
# 				column_label    : 	Text for Column (or X Axis) Type('col_sortby', 'col_scaleby')
# 				label_prods     : 	Specify which products to label
# 				low_value_cutoff: 	Specify a Low Value Cutoff for ColorBar
# 				high_value_cutoff: 	Specify a High Value Cutoff for ColorBar
# 				gradient_cutoff : 	Specify cutoff value for heatmap color variation (RCA => 4/5 seems to work well in this context) 
# 				size            : 	Size of Heatmap Figure
# 				axs_only        : 	Allows axs to be an input and return graph axs only. (Useful when compiling evolution of graph via Kpn and Kcn)
# 				cmap 			:  	Specify a colormap [Default: cm.Reds]
# 				rem_zero_rel 	: 	Remove Zero Data Relationships from the Data [Default: True]
# 				fillna 			: 	Fill Missing Values with 0 [Default = True]

# 			Notes:
# 			-----
# 				[1] Integrate the below three functions into one single ploting method. 
# 				[2] Make General for any matrix AND not just Mcp

# 			Future Work:
# 			-----------
# 				[1] Is there a more sensible way to Handle the default value vectors

# 			Current Known Issues:
# 			--------------------
# 				[1] ColorBar Logic Not quite Working
# 				[1] Setting lower_limit on data isn't constructing the correct colorbar
# 		'''
# 		# - Import Standard Library - #
# 		from matplotlib import cm

# 		# - Helper Functions - #
# 		def index_array(labels, items):           													## ==> CODE DUPLICATED: Move to Common File for Plotting Helper Functions <== ##
# 			''' 
# 				Take np.array and return appropriate index positions for items
# 			'''
# 			index = []
# 			for item in items:
# 				try:
# 					idx = np.where(labels==item)
# 					index.append(idx[0][0])        #First [0] extracts from tuple, Second[0] extracts from ndarray
# 				except:
# 					print "[WARNING]: %s is not found in sortby or scaleby vectors. Ignoring!" % item
# 					continue 
# 			return np.array(index)

# 		def prepare_scaling_vectors(cpdata, row_scaleby=None, column_scaleby=None):
# 			''' 
# 				Prepares scaleby vectors for plot_scaled_mcp_heatmap() function 
# 				[Basically generates a jointly indexed set of values for rowsortby and rowscale by vectors]
				
# 				Notes:
# 				-----
# 					[1] Only data matching between the original cpdata matrix and the scaleby series will be returned
# 						This allows one to use country_income vectors and any extra data is discarded in preparing the scalebys 

# 				Work:
# 				----
# 					[1] Where should this method live? Could be useful for other functions
# 			'''
# 			if type(row_scaleby) == pd.Series:
# 				rownames = cpdata.index
# 				row_scaleby = row_scaleby[rownames]        			#Select Appropriate Data
# 				row_scaleby = row_scaleby[row_scaleby.notnull()]
# 				sorted_df_year = cpdata.ix[row_scaleby.index]  		#Get the Matching Data
# 			if type(column_scaleby) == pd.Series:
# 				colnames = cpdata.columns
# 				column_scaleby = column_scaleby[colnames]
# 				column_scaleby = column_scaleby[column_scaleby.notnull()]
# 				sorted_df_year = cpdata[column_scaleby.index]
# 			# - Returns relevant scaleby vectors - #
# 			return cpdata, row_scaleby, column_scaleby

# 		# - User Reminders - #
# 		if type(row_scaleby) != pd.Series and type(col_scaleby) != pd.Series:
# 			raise ValueError("One of row_scaleby, col_scaleby or both need to be specified")
# 		print "[NOTICE]: Data contains data named: %s; Make Sure you have set appropriate low (%s) and high (%s) value cutoffs for optimal views" % (cpdata_name, low_value_cutoff, high_value_cutoff)
# 		print "[WARNING]: This function will name rows, Sortby: %s, Scaleby: %s" % row_label
# 		print "[WARNING]: This function will name columns, Sortby: %s, Scaleby: %s" % column_label
# 		if cpdata_name == 'Data':
# 			print "[NOTICE]: Using Generic Data Name: Data"

# 		# - Pandas Work - #
# 		if rem_zero_rel:
# 			sorted_cpdata = self.remove_zero_relationships_matrix(sorted_cpdata)
# 		if fillna:
# 			sorted_cpdata = sorted_cpdata.fillna(0.0)
# 		countries = sorted_cpdata.index
# 		products = sorted_cpdata.columns

# 		# - Prepare Scaling Vectors - #
# 		sorted_cpdata, row_scaleby, column_scaleby = prepare_scaling_vectors(sorted_cpdata, row_scaleby, column_scaleby)

# 		# - Adjust matr for Gradient Cutoffs - #
# 		value_note = ''
# 		if type(low_value_cutoff) in [int, float]:
# 			sorted_cpdata = sorted_cpdata.applymap(lambda x: low_value_cutoff if x < low_value_cutoff else x)
# 			value_note = value_note + "Applied Low Value Cutoff (%s) to cpdata\n" % low_value_cutoff
# 		if type(high_value_cutoff) in [int, float]:
# 			sorted_cpdata = sorted_cpdata.applymap(lambda x: high_value_cutoff if x > high_value_cutoff else x)
# 			value_note = value_note + "Applied Low Value Cutoff (%s) to cpdata\n" % low_value_cutoff

# 		# - Plot Data - #
# 		matr = sorted_cpdata.as_matrix()
# 		matr_note = value_note

# 		# - Scaling Vectors - #
# 		if type(row_scaleby) == pd.Series: 						
# 			yscale = np.array(row_scaleby)
# 			yscale = yscale / np.sum(yscale)                    #Normalise (0,1) 
# 			yscale = np.insert(yscale.cumsum(), 0, 0)           #Replace y with scaled vector
# 			ylabel = np.array(countries)
# 		else: 
# 			y = np.array(countries)
# 			yscale = np.arange(len(y))
# 			ylabel = np.array(countries)
# 		if type(column_scaleby) == pd.Series: 
# 			xscale = np.array(column_scaleby)
# 			xscale = xscale / np.sum(xscale)                    #Normalise (0,1)
# 			xscale = np.insert(xscale.cumsum(), 0, 0)       	#Replace x with scaled vector
# 			xlabel = np.array(products)              
# 		else:
# 			x = np.array(products)
# 			xscale = np.arange(len(x))
# 			xlabel = np.array(products)
# 		xx,yy = np.meshgrid(xscale,yscale)
# 		# - Matplotlib Work - #
# 		# - Prepare Figure - #
# 		if axs_only != False:
# 			axs = axs_only
# 		else:
# 			fig = plt.figure(figsize=(size*1.2,size))                           #The 1.2 multiple allows for the incorporation of a colorbar
# 			axs = fig.add_subplot(1,1,1)
# 		plot = axs.pcolormesh(xx, yy, matr, cmap=cmap)                         #Could Consider making cmap an option (but not implimented b/c Reds works well and all graphs will then be standardized)
# 		# - Set Axis Limits - #
# 		if type(row_scaleby) and type(column_scaleby) == pd.Series:
# 			axs.set_ylim(0,1)
# 			axs.set_xlim(0,1)
# 		elif type(row_scaleby) == pd.Series:
# 			axs.set_ylim(0,1)
# 			axs.set_xlim(0, len(x))
# 		elif type(column_scaleby) == pd.Series:
# 			axs.set_ylim(0, len(y))
# 			axs.set_xlim(0,1)
# 		else:
# 			axs.set_ylim(0, len(y))
# 			axs.set_xlim(0, len(x))
# 		# - Geneate Labeling for Axes - #
# 		idx = index_array(ylabel, label_cntrys)
# 		centre_adj = (yscale[idx+1] - yscale[idx]) / 2
# 		axs.set_yticks(yscale[idx] + centre_adj)
# 		axs.set_yticklabels(ylabel[idx])
# 		idx = index_array(xlabel, label_prods)
# 		centre_adj = (xscale[idx+1] - xscale[idx]) / 2
# 		axs.set_xticks(xscale[idx] + centre_adj)
# 		axs.set_xticklabels(xlabel[idx])
# 		if axs_only != False:
# 			return axs
# 		# - Colorbar - #
# 		if len(str(matr.max())) > 4:                       #For Long Values (like Export Values) Put Value on New Line, This is a fairly arbitrary decision re: length so may consider turning this into a flag. 
# 			# - Matr has Negative Values - #
# 			if matr.min() < 0:                             #Formatting for Lower Bounds that are Negative
# 				cbar = plt.colorbar(plot, ticks=[matr.min(), (matr.max() - abs(matr.min()))/2, matr.max()])
# 				cbar.set_ticklabels(['<= %0.0e' % (matr.min()), '%0.0e' % ((matr.max() - abs(matr.min()))/2), '>= %0.0e' % (matr.max())])     
# 				cbar.ax.set_ylabel(cpdata_name)
# 			# - Matr Only has Positive Values - #
# 			else:
# 				cbar = plt.colorbar(plot, ticks=[matr.min(), matr.min()+(matr.max() - matr.min())/2, matr.max()])
# 				cbar.set_ticklabels(['<= %0.0e' % matr.min(), '%0.0e' % (matr.min()+((matr.max() - matr.min())/2)), '>= %0.0e' % (matr.max())])    
# 				cbar.ax.set_ylabel(cpdata_name)
# 		else:
# 			if matr.min() < 0:
# 				cbar = plt.colorbar(plot, ticks=[matr.min(), (matr.max() - abs(matr.min()))/2, matr.max()])
# 				cbar.set_ticklabels(['<= %0.2f' % (matr.min()), '= %0.2f' % ((matr.max() - abs(matr.min()))/2), '>= %0.2f' % (matr.max())])     
# 				cbar.ax.set_ylabel(cpdata_name)
# 			else:
# 				cbar = plt.colorbar(plot, ticks=[matr.min(), matr.min()+(matr.max() - matr.min())/2, matr.max()])
# 				cbar.set_ticklabels(['<= %0.2f' % matr.min(), '= %0.2f' % (matr.min()+((matr.max() - matr.min())/2)), '>= %0.2f' % (matr.max())])     
# 				cbar.ax.set_ylabel(cpdata_name)
# 		# - Figure Titles and Labels - #
# 		figtitle = "Mcp [" + cpdata_name + " Values] Matrix [Yr: " + str(self.year) + "]"
# 		plt.title(figtitle)
# 		sortby, scaleby = row_label
# 		ylabel = 'Countries [Ordered by ' + sortby + ', Scaled by ' + scaleby + ']' 	# - Assuming sortby and scaleby are always used here - #
# 		axs.set_ylabel(ylabel)
# 		sortby, scaleby = column_label
# 		xlabel = 'Products [Ordered by ' + sortby + ', Scaled by ' + scaleby + ']'
# 		axs.set_xlabel(xlabel)
# 		# - Notes - #
# 		note = 'Countries: ' + str(len(countries)) + '; Products: ' + str(len(products))
# 		if value_note != '': 
# 			note + '\n' + value_note
# 		plt.figtext(0.7, 0.01, note)
# 		plt.tight_layout()
# 		return fig



# # default_cntrys = ['JPN', 'USA', 'DEU', 'KOR', 'ITA', 'AUS', 'BRA', 'THA', 'GHA', 'GBR', 'TWN', 'FRA', 'MEX']
# # default_prods = ['8421', '3330', '7922', '8748', '7810', '7442', '7781']                                       #May Need to Update for Dataset #3 if it becomes default dataset
# # default_row_labels = ('ECI', 'Export Value Share')
# # default_column_labels = ('PCI', 'Product Value Share')
# # def plot_scaled_mcp_heatmap(sorted_rca_year, row_scaleby='', column_scaleby='', year=2000, row_label=default_row_labels, label_cntrys=default_cntrys, column_label=default_column_labels, label_prods=default_prods, rca_grad_cutoff=4, size=8, axs_only=False):
# #     ''' Function: plot_scaled_mcp_heatmap (Designed for RCA and Mcp Values)
# #         Status: IN-USE
# #         Options:    row_scaleby     -> Specify a row_scaleby series (i.e. GDP or Country Exports etc.)
# #                     column_scaleby  -> Specify a column_scaleby series
# #                     year            -> Used in Title
# #                     row_label       -> Text for Row (or Y Axis) Type('row_sortby', 'row_scaleby')
# #                     label_cntrys    -> Specify which countries to label
# #                     column_label    -> Text for Column (or X Axis)
# #                     label_prods     -> Specify which products to label
# #                     rca_grad_cutoff -> Specify rca cutoff for heatmap color variation (4/5 seems to work well in this context) 
# #                     size            -> Size of Heatmap Figure
# #                     axs_only        -> Allows axs to be an input and return graph axs only. (Useful when compiling evolution of graph via Kpn and Kcn)
# #         Changes: row_step and column_step are less meaningful in this non-linear index - Replaced by country specific markers (i.e row_label=['DEU', 'AUS'])
# #         Dependancies: prepare_scaling_sortby_vectors()
# #         Notes:      1. Consider changing nomenclature in this function from rca_grad_cutoff to generic grad_cutoff that operations on sorted_data_year not just sorted_rca_year
# #                     However in the meantime -> can use this function to performe any sorted, scaled, pcolormap!
# #                     [Note: This option can also be used for ploting other data that isn't RCA like export Values but labels it incorectly]
# #                     2. Version 2 is Implimented (Consider Removing this version - currently here for backwards compatability) -> Version 2 Defaults should be set to RCA graphs anyway!
# #     '''
# #     from matplotlib import cm

# #     def index_array(labels, items):
# #         ''' Take np.array and return appropriate index positions for items'''
# #         index = []
# #         for item in items:
# #             try:
# #                 idx = np.where(labels==item)
# #                 index.append(idx[0][0])        #First [0] extracts from tuple, Second[0] extracts from ndarray
# #             except:
# #                 print "WARNING: %s is not found in sortby or scaleby vectors. Ignoring!" % item
# #                 continue 
# #         return np.array(index)

# #     #Print Reminder
# #     print "WARNING: This function defaults to naming rows, Sortby: %s, Scaleby: %s" % default_row_labels
# #     print "WARNING: This function defaults to naming columns, Sortby: %s, Scaleby: %s" % default_column_labels

# #     #Pandas Work
# #     rcamat = remove_zero_relationships(sorted_rca_year)
# #     countries = rcamat.index
# #     products = rcamat.columns
# #     if rca_grad_cutoff != -1:
# #         matr = rcamat.applymap(lambda x: 0 if x < 1.0 else x).applymap(lambda x: rca_grad_cutoff if x >= rca_grad_cutoff else x).as_matrix()        
# #         rca_note = 'RCA Gradient Cutoff >= ' + str(rca_grad_cutoff)
# #     else:
# #         matr = rcamat.as_matrix()
# #         rca_note = ''
# #     #Scaling Vectors
# #     if type(row_scaleby) == pd.Series:
# #         yscale = np.array(row_scaleby)
# #         yscale = yscale / np.sum(yscale)                    #Normalise (0,1) 
# #         yscale = np.insert(yscale.cumsum(), 0, 0)           #Replace y with scaled vector
# #         ylabel = np.array(countries)
# #     else: 
# #         y = np.array(countries)
# #         yscale = np.arange(len(y))
# #         ylabel = np.array(countries)
# #     if type(column_scaleby) == pd.Series: 
# #         xscale = np.array(column_scaleby)
# #         xscale = xscale / np.sum(xscale)                    #Normalise (0,1)
# #         xscale = np.insert(xscale.cumsum(), 0, 0)       #Replace x with scaled vector
# #         xlabel = np.array(products)                 
# #     else:
# #         x = np.array(products)
# #         xscale = np.arange(len(x))
# #         xlabel = np.array(products)
# #     xx,yy = np.meshgrid(xscale,yscale)
# #     #Matplotlib Work
# #     if axs_only != False:
# #         axs = axs_only
# #     else:
# #         fig = plt.figure(figsize=(size*1.2,size))                           #The 1.2 multiple allows for the incorporation of a colorbar
# #         axs = fig.add_subplot(1,1,1)
# #     plot = axs.pcolormesh(xx,yy,matr, cmap=cm.Reds)
# #     if type(row_scaleby) and type(column_scaleby) == pd.Series:
# #         axs.set_ylim(0,1)
# #         axs.set_xlim(0,1)
# #     elif type(row_scaleby) == pd.Series:
# #         axs.set_ylim(0,1)
# #         axs.set_xlim(0, len(x))
# #     elif type(column_scaleby) == pd.Series:
# #         axs.set_ylim(0, len(y))
# #         axs.set_xlim(0,1)
# #     else:
# #         axs.set_ylim(0, len(y))
# #         axs.set_xlim(0, len(x))
# #     #Labeling Axes
# #     idx = index_array(ylabel, label_cntrys)
# #     centre_adj = (yscale[idx+1] - yscale[idx]) /2
# #     axs.set_yticks(yscale[idx] + centre_adj)
# #     axs.set_yticklabels(ylabel[idx])
# #     idx = index_array(xlabel, label_prods)
# #     centre_adj = (xscale[idx+1] - xscale[idx]) /2
# #     axs.set_xticks(xscale[idx] + centre_adj)
# #     axs.set_xticklabels(xlabel[idx])
# #     if axs_only != False:
# #         return axs
# #     #Colorbar
# #     cbar = plt.colorbar(plot, ticks=[0,matr.max()/2,matr.max()])
# #     cbar.set_ticklabels(['RCA = 0', 'RCA = '+str(matr.max()/2), 'RCA >= '+str(int(matr.max()))])
# #     #Figure Text
# #     figtitle = "Mcp [RCA Values] Matrix [Yr: " + str(year) + "]"
# #     plt.title(figtitle)
# #     sortby, scaleby = row_label
# #     ylabel = 'Countries [Ordered by ' + sortby + ', Scaled by ' + scaleby + ']'
# #     axs.set_ylabel(ylabel)
# #     sortby, scaleby = column_label
# #     xlabel = 'Products [Ordered by ' + sortby + ', Scaled by ' + scaleby + ']'
# #     axs.set_xlabel(xlabel)
# #     #Notes
# #     note = 'Countries: ' + str(len(countries)) + '; Products: ' + str(len(products))
# #     plt.figtext(0.7, 0.01, note)
# #     plt.tight_layout()
# #     return fig


# # default_cntrys = ['JPN', 'USA', 'DEU', 'KOR', 'ITA', 'AUS', 'BRA', 'THA', 'GHA', 'GBR', 'TWN', 'FRA', 'MEX']
# # default_prods = ['8421', '3330', '7922', '8748', '7810', '7442', '7781']
# # default_row_labels = ('ECI', 'Export Value Share')
# # default_column_labels = ('PCI', 'Product Value Share')
# # def plot_scaled_mcp_heatmap_v2(sorted_data_year, row_scaleby='', column_scaleby='', year=2000, row_label=default_row_labels, label_cntrys=default_cntrys, column_label=default_column_labels, label_prods=default_prods, value_type='RCA', low_value_cutoff=-1, value_cutoff=-1, size=8, axs_only=False, verbose=False):
# #     ''' Function: plot_scaled_mcp_heatmap (Designed for Mcp, RCA, Export, ExportShare etc. to be graphed as a scaled heatmap pcolor chart!)
# #         Status:   IN-DEVELOPMENT
# #         Input:    Requires Pre-Sorted Matrix of Data
# #         Options:    row_scaleby     -> Specify a row_scaleby series (i.e. GDP or Country Exports etc.)
# #                     column_scaleby  -> Specify a column_scaleby series
# #                     year            -> Used in Title
# #                     row_label       -> Text for Row (or Y Axis) Type('row_sortby', 'row_scaleby')
# #                     label_cntrys    -> Specify which countries to label
# #                     column_label    -> Text for Column (or X Axis)
# #                     label_prods     -> Specify which products to label
# #                     low_value_cutoff -> Specify Low Value to Cutoff (Defaults to 0 if not specified explicitly) [Works in Most Use Cases Except Percent Share Data]
# #                     value_cutoff    -> Specify value cutoff for heatmap color variation 
# #                     size            -> Size of Heatmap Figure
# #                     axs_only        -> Allows axs to be an input and return graph axs only. (Useful when compiling evolution of graph via Kpn and Kcn)
# #         Dependancies: prepare_scaling_sortby_vectors()
# #     '''
# #     from matplotlib import cm

# #     def index_array(labels, items):
# #         ''' Take np.array and return appropriate index positions for items'''
# #         index = []
# #         for item in items:
# #             try:
# #                 idx = np.where(labels==item)
# #                 index.append(idx[0][0])        #First [0] extracts from tuple, Second[0] extracts from ndarray
# #             except:
# #                 print "WARNING: %s is not found in sortby or scaleby vectors. Ignoring!" % item
# #                 continue 
# #         return np.array(index)

# #     #Print Reminders
# #     print "Note: Value Type is: %s; Make Sure you have set appropriate value_cutoff: %s for optimal views" % (value_type, value_cutoff)
# #     print "WARNING: This function defaults to naming rows, Sortby: %s, Scaleby: %s" % default_row_labels
# #     print "WARNING: This function defaults to naming columns, Sortby: %s, Scaleby: %s" % default_column_labels

# #     #Pandas Work
# #     data = remove_zero_relationships(sorted_data_year)
# #     countries = data.index
# #     products = data.columns
# #     if low_value_cutoff == -1:                  #Default Set for RCA Filter
# #         low_value_cutoff = 1.0
# #     if value_cutoff != -1:
# #         matr = data.applymap(lambda x: 0 if x < low_value_cutoff else x).applymap(lambda x: value_cutoff if x >= value_cutoff else x).as_matrix()        
# #         value_note = value_type + ' Gradient Cutoff >= ' + str(value_cutoff)
# #     else:
# #         if verbose: print "All Plot Values will be shown. To make Graph clearer use: value_cutoff and low_value_cutoff options"
# #         matr = data.as_matrix()
# #         rca_note = ''
# #     #Scaling Vectors
# #     if type(row_scaleby) == pd.Series:
# #         yscale = np.array(row_scaleby)
# #         yscale = yscale / np.sum(yscale)                    #Normalise (0,1) 
# #         yscale = np.insert(yscale.cumsum(), 0, 0)           #Replace y with scaled vector
# #         ylabel = np.array(countries)
# #     else: 
# #         y = np.array(countries)
# #         yscale = np.arange(len(y))
# #         ylabel = np.array(countries)
# #     if type(column_scaleby) == pd.Series: 
# #         xscale = np.array(column_scaleby)
# #         xscale = xscale / np.sum(xscale)                    #Normalise (0,1)
# #         xscale = np.insert(xscale.cumsum(), 0, 0)       #Replace x with scaled vector
# #         xlabel = np.array(products)                 
# #     else:
# #         x = np.array(products)
# #         xscale = np.arange(len(x))
# #         xlabel = np.array(products)
# #     xx,yy = np.meshgrid(xscale,yscale)
# #     #Matplotlib Work
# #     if axs_only != False:
# #         axs = axs_only
# #     else:
# #         fig = plt.figure(figsize=(size*1.2,size))                           #The 1.2 multiple allows for the incorporation of a colorbar
# #         axs = fig.add_subplot(1,1,1)
# #     plot = axs.pcolormesh(xx,yy,matr, cmap=cm.Reds)                         #Could Consider making cmap an option (but not implimented b/c Reds works well and all graphs will then be standardized)
# #     if type(row_scaleby) and type(column_scaleby) == pd.Series:
# #         axs.set_ylim(0,1)
# #         axs.set_xlim(0,1)
# #     elif type(row_scaleby) == pd.Series:
# #         axs.set_ylim(0,1)
# #         axs.set_xlim(0, len(x))
# #     elif type(column_scaleby) == pd.Series:
# #         axs.set_ylim(0, len(y))
# #         axs.set_xlim(0,1)
# #     else:
# #         axs.set_ylim(0, len(y))
# #         axs.set_xlim(0, len(x))
# #     #Labeling Axes
# #     idx = index_array(ylabel, label_cntrys)
# #     centre_adj = (yscale[idx+1] - yscale[idx]) /2
# #     axs.set_yticks(yscale[idx] + centre_adj)
# #     axs.set_yticklabels(ylabel[idx])
# #     idx = index_array(xlabel, label_prods)
# #     centre_adj = (xscale[idx+1] - xscale[idx]) /2
# #     axs.set_xticks(xscale[idx] + centre_adj)
# #     axs.set_xticklabels(xlabel[idx])
# #     if axs_only != False:
# #         return axs
# #     #Colorbar
# #     if len(str(matr.max())) > 4:                       #For Long Values (like Exports) Put Value on New Line, This is a fairly arbitrary decision re: length so may consider turning this into a flag. 
# #         cbar = plt.colorbar(plot, ticks=[0,matr.max()/2,matr.max()])
# #         cbar.set_ticklabels(['<= %0.0e' % (matr.min()), '%0.0e' % (matr.max()/2), '>= %0.0e' % (matr.max())])     #Is This Formating Necessary? Changed: ' >= '+str(int(matr.max()))] to str(matr.max()))]
# #         cbar.ax.set_ylabel(value_type)
# #     else:
# #         cbar = plt.colorbar(plot, ticks=[0,matr.max()/2,matr.max()])
# #         cbar.set_ticklabels(['<= %0.2f' % (matr.min()), '= %0.2f' % (matr.max()/2), '>= %0.2f' % (matr.max())])     #Is This Formating Necessary? Changed: ' >= '+str(int(matr.max()))] to str(matr.max()))]
# #         cbar.ax.set_ylabel(value_type)
# #     #Figure Text
# #     figtitle = "Mcp [" + value_type + " Values] Matrix [Yr: " + str(year) + "]"
# #     plt.title(figtitle)
# #     sortby, scaleby = row_label
# #     ylabel = 'Countries [Ordered by ' + sortby + ', Scaled by ' + scaleby + ']'
# #     axs.set_ylabel(ylabel)
# #     sortby, scaleby = column_label
# #     xlabel = 'Products [Ordered by ' + sortby + ', Scaled by ' + scaleby + ']'
# #     axs.set_xlabel(xlabel)
# #     #Notes
# #     note = 'Countries: ' + str(len(countries)) + '; Products: ' + str(len(products))
# #     plt.figtext(0.7, 0.01, note)
# #     plt.tight_layout()
# #     return fig


# # default_cntrys = ['JPN', 'USA', 'DEU', 'KOR', 'ITA', 'AUS', 'BRA', 'THA', 'GHA', 'GBR', 'TWN', 'FRA', 'MEX']
# # default_prods = ['8421', '3330', '7922', '8748', '7810', '7442', '7781']
# # default_row_labels = ('ECI', 'Export Value Share')
# # default_column_labels = ('PCI', 'Product Value Share')
# # def plot_scaled_mcp_heatmap_v3(sorted_data_year, row_scaleby='', column_scaleby='', year=2000, row_label=default_row_labels, label_cntrys=default_cntrys, column_label=default_column_labels, label_prods=default_prods, value_type='RCA', low_value_cutoff=np.nan, high_value_cutoff=-1, size=8, axs_only=False, cmap=cm.Reds, verbose=False):
# #     ''' Function: plot_scaled_mcp_heatmap (Designed for Mcp, RCA, Export, ExportShare etc. to be graphed as a scaled heatmap pcolor chart!)
# #         Status:   IN-DEVELOPMENT
# #         Input:    Requires Pre-Sorted Matrix of Data
# #         Options:    row_scaleby     -> Specify a row_scaleby series (i.e. GDP or Country Exports etc.)
# #                     column_scaleby  -> Specify a column_scaleby series
# #                     year            -> Used in Title
# #                     row_label       -> Text for Row (or Y Axis) Type('row_sortby', 'row_scaleby')
# #                     label_cntrys    -> Specify which countries to label
# #                     column_label    -> Text for Column (or X Axis)
# #                     label_prods     -> Specify which products to label
# #                     low_value_cutoff -> Specify Low Value to Cutoff (Defaults to 0 if not specified explicitly) [Works in Most Use Cases Except Percent Share Data]
# #                     value_cutoff    -> Specify value cutoff for heatmap color variation 
# #                     size            -> Size of Heatmap Figure
# #                     axs_only        -> Allows axs to be an input and return graph axs only. (Useful when compiling evolution of graph via Kpn and Kcn)

# #         Dependancies: prepare_scaling_sortby_vectors()
# #         Differences: Version 3 incorporates option to change ColorMap and Allows for Negative Numbers
# #     '''
# #     from matplotlib import cm

# #     def index_array(labels, items):
# #         ''' Take np.array and return appropriate index positions for items'''
# #         index = []
# #         for item in items:
# #             try:
# #                 idx = np.where(labels==item)
# #                 index.append(idx[0][0])        #First [0] extracts from tuple, Second[0] extracts from ndarray
# #             except:
# #                 print "WARNING: %s is not found in sortby or scaleby vectors. Ignoring!" % item
# #                 continue 
# #         return np.array(index)

# #     #Print Reminders
# #     print "Note: Value Type is: %s; Make Sure you have set appropriate high_value_cutoff: %s for optimal views" % (value_type, high_value_cutoff)
# #     print "WARNING: This function defaults to naming rows, Sortby: %s, Scaleby: %s" % default_row_labels
# #     print "WARNING: This function defaults to naming columns, Sortby: %s, Scaleby: %s" % default_column_labels

# #     #Pandas Work
# #     data = remove_zero_relationships(sorted_data_year)
# #     countries = data.index
# #     products = data.columns
# #     '''
# #     if low_value_cutoff == np.nan:                  #Default Set for RCA Filter
# #         low_value_cutoff = 1.0
# #     if high_value_cutoff != -1:
# #         matr = data.applymap(lambda x: 0 if x < low_value_cutoff else x).applymap(lambda x: high_value_cutoff if x >= value_cutoff else x).as_matrix()        
# #         value_note = value_type + ' Gradient Cutoff >= ' + str(high_value_cutoff)
# #     else:
# #     '''
# #     if verbose: print "All Plot Values will be shown. To make Graph clearer use: value_cutoff and low_value_cutoff options"
# #     matr = data.as_matrix()
# #     rca_note = ''
# #     #Scaling Vectors
# #     if type(row_scaleby) == pd.Series:
# #         yscale = np.array(row_scaleby)
# #         yscale = yscale / np.sum(yscale)                    #Normalise (0,1) 
# #         yscale = np.insert(yscale.cumsum(), 0, 0)           #Replace y with scaled vector
# #         ylabel = np.array(countries)
# #     else: 
# #         y = np.array(countries)
# #         yscale = np.arange(len(y))
# #         ylabel = np.array(countries)
# #     if type(column_scaleby) == pd.Series: 
# #         xscale = np.array(column_scaleby)
# #         xscale = xscale / np.sum(xscale)                    #Normalise (0,1)
# #         xscale = np.insert(xscale.cumsum(), 0, 0)       #Replace x with scaled vector
# #         xlabel = np.array(products)                 
# #     else:
# #         x = np.array(products)
# #         xscale = np.arange(len(x))
# #         xlabel = np.array(products)
# #     xx,yy = np.meshgrid(xscale,yscale)
# #     #Matplotlib Work
# #     if axs_only != False:
# #         axs = axs_only
# #     else:
# #         fig = plt.figure(figsize=(size*1.2,size))                           #The 1.2 multiple allows for the incorporation of a colorbar
# #         axs = fig.add_subplot(1,1,1)
# #     plot = axs.pcolormesh(xx,yy,matr, cmap=cmap)                         #Could Consider making cmap an option (but not implimented b/c Reds works well and all graphs will then be standardized)
# #     if type(row_scaleby) and type(column_scaleby) == pd.Series:
# #         axs.set_ylim(0,1)
# #         axs.set_xlim(0,1)
# #     elif type(row_scaleby) == pd.Series:
# #         axs.set_ylim(0,1)
# #         axs.set_xlim(0, len(x))
# #     elif type(column_scaleby) == pd.Series:
# #         axs.set_ylim(0, len(y))
# #         axs.set_xlim(0,1)
# #     else:
# #         axs.set_ylim(0, len(y))
# #         axs.set_xlim(0, len(x))
# #     #Labeling Axes
# #     idx = index_array(ylabel, label_cntrys)
# #     centre_adj = (yscale[idx+1] - yscale[idx]) /2
# #     axs.set_yticks(yscale[idx] + centre_adj)
# #     axs.set_yticklabels(ylabel[idx])
# #     idx = index_array(xlabel, label_prods)
# #     centre_adj = (xscale[idx+1] - xscale[idx]) /2
# #     axs.set_xticks(xscale[idx] + centre_adj)
# #     axs.set_xticklabels(xlabel[idx])
# #     if axs_only != False:
# #         return axs
# #     #Colorbar
# #     if len(str(matr.max())) > 4:                       #For Long Values (like Exports) Put Value on New Line, This is a fairly arbitrary decision re: length so may consider turning this into a flag. 
# #         if matr.min() < 0:                             #Formatting for Lower Bounds that are Negative
# #             cbar = plt.colorbar(plot, ticks=[matr.min(), (matr.max() - abs(matr.min()))/2, matr.max()])
# #             cbar.set_ticklabels(['<= %0.0e' % (matr.min()), '%0.0e' % ((matr.max() - abs(matr.min()))/2), '>= %0.0e' % (matr.max())])     #Is This Formating Necessary? Changed: ' >= '+str(int(matr.max()))] to str(matr.max()))]
# #             cbar.ax.set_ylabel(value_type)
# #         else:
# #             cbar = plt.colorbar(plot, ticks=[0,matr.max()/2,matr.max()])
# #             cbar.set_ticklabels(['<= %0.0e' % ((matr.min()), '%0.0e' % (matr.max())/2), '>= %0.0e' % (matr.max())])     #Is This Formating Necessary? Changed: ' >= '+str(int(matr.max()))] to str(matr.max()))]
# #             cbar.ax.set_ylabel(value_type)
# #     else:
# #         cbar = plt.colorbar(plot, ticks=[0,matr.max()/2,matr.max()])
# #         cbar.set_ticklabels(['<= %0.2f' % (matr.min()), '= %0.2f' % (matr.max()/2), '>= %0.2f' % (matr.max())])     #Is This Formating Necessary? Changed: ' >= '+str(int(matr.max()))] to str(matr.max()))]
# #         cbar.ax.set_ylabel(value_type)
# #     #Figure Text
# #     figtitle = "Mcp [" + value_type + " Values] Matrix [Yr: " + str(year) + "]"
# #     plt.title(figtitle)
# #     sortby, scaleby = row_label
# #     ylabel = 'Countries [Ordered by ' + sortby + ', Scaled by ' + scaleby + ']'
# #     axs.set_ylabel(ylabel)
# #     sortby, scaleby = column_label
# #     xlabel = 'Products [Ordered by ' + sortby + ', Scaled by ' + scaleby + ']'
# #     axs.set_xlabel(xlabel)
# #     #Notes
# #     note = 'Countries: ' + str(len(countries)) + '; Products: ' + str(len(products))
# #     plt.figtext(0.7, 0.01, note)
# #     plt.tight_layout()
# #     return fig

# ### --- END WORKING HERE --- ###


# 	default_prods = ['8421', '3330', '7922', '8748', '7810', '7442', '7781']
# 	def plot_proximity(self, sortby=None, sortby_text='', scaleby=None, scaleby_text='', label_prods=default_prods, prox_cutoff=-1, step=0, xlabel_rot=0, size=6):
# 		'''
# 			Plot Proximity Matrix

# 			Options:
# 			-------
# 				sortby  		: 	SortBy Series
# 				sortby_text 	: 	Text to Add to the Labels
# 				scaleby 		: 	Scaling Vector (Heterogenous Row and Column Sizes)
# 				scaleby_text 	: 	Text to Add to the Labels
# 				label_prods 	: 	List of Products to Label
# 				prox_cutoff 	: 	Proximity Cutoff
# 				step    		:	Step for Row and Column Labels (If label_prods is not specified)
# 				xlabel_rot 		: 	Rotate xlabel by degrees
# 				size 			: 	Figure Size 

# 			Notes:
# 			------
# 				[1] SITC4 Products step = 10 is a good number

# 		'''
# 		# - Standard Library Imports - #
# 		from matplotlib import cm 						#Note: Check if it is in header as no need to import twice

# 		# - Helper Functions - #
# 		def index_array(labels, items):
# 			''' 
# 				Take np.array and return appropriate index positions for items
# 			'''
# 			index = []
# 			for item in items:
# 				try:
# 					idx = np.where(labels==item)
# 					index.append(idx[0][0])        #First [0] extracts from tuple, Second[0] extracts from ndarray
# 				except:
# 					print "[WARNING]: %s is not found in sortby or scaleby vectors. Ignoring!" % item
# 					continue 
# 			return np.array(index)

# 		# - Function Code - # 
		
# 		# - Sorting Work in Pandas - #
# 		if type(sortby) != pd.Series:             				# By Default this should be sorted by ProductCode
# 			sorted_proximity = self.proximity 
# 		else:
# 			sorted_proximity = self.sorted_matrix(self.proximity, row_sortby=sortby, column_sortby=sortby)
		
# 		# When scaleby is defined (default to specific labelled products) #
# 		if type(scaleby) == pd.Series:              
# 			step = None                    
		
# 		# - Generate Graph Data Matrix - #
# 		if prox_cutoff != -1:
# 			matr = sorted_proximity.applymap(lambda x: prox_cutoff if x >= prox_cutoff else x).as_matrix()     #Only One Sided Filter (Perhaps write a filter method)     
# 		else:
# 			matr = sorted_proximity.as_matrix()

# 		# - Plot Generation - #    
# 		fig = plt.figure(figsize=(1.2*size,size))       	#Allow space for colobar
# 		axs = fig.add_subplot(1,1,1)
# 		products = sorted_proximity.index
# 		# - Scaling Vectors - #
# 		if type(scaleby) == pd.Series: 
# 			xscale = np.array(scaleby)
# 			xscale = xscale / np.sum(xscale)                    #Normalise (0,1)
# 			xscale = np.insert(xscale.cumsum(), 0, 0)           #Replace x with scaled vector
# 			xlabel = np.array(products)
# 			yscale = xscale                                     #Redundant from Memory perspective but maybe easier to understand
# 			ylabel = xlabel                 
# 		else:
# 			xscale = np.arange(len(products)+1)
# 			xlabel = np.array(products)                         #Redundant as use sorted_proximity_year.index directly later on. 
# 			yscale = xscale                                     #Redundant from Memory perspective but maybe easier to understand
# 			ylabel = xlabel                                     #Redundant as use sorted_proximity_year.index directly later on. 
# 		xx,yy = np.meshgrid(xscale,yscale)
# 		plot = axs.pcolormesh(xx,yy,matr)

# 		# - Set Limits - #
# 		if type(scaleby) == pd.Series:
# 			axs.set_ylim(0,1)
# 			axs.set_xlim(0,1)
# 		else:
# 			axs.set_ylim(0, len(yscale)-1)
# 			axs.set_xlim(0, len(xscale)-1)
		
# 		# - Labeling Axes - #
# 		if type(step) == int:
# 			num_products = len(products)
# 			if step == 0:  										#Case where all products should be labelled [Default]
# 				step = num_products
# 			tick_step = int(num_products / step)
# 			ticks_index = np.array(range(0, num_products, tick_step))
# 			axs.set_yticks(ticks_index + 0.5)
# 			axs.set_yticklabels(sorted_proximity.index[ticks_index], fontsize='small')
# 			axs.set_xticks(ticks_index + 0.5)
# 			axs.set_xticklabels(sorted_proximity.columns[ticks_index], fontsize='small', rotation=xlabel_rot)
# 		else:
# 			idx = index_array(ylabel, label_prods)
# 			centre_adj = (yscale[idx+1] - yscale[idx]) /2
# 			axs.set_yticks(yscale[idx] + centre_adj)
# 			axs.set_yticklabels(ylabel[idx])
# 			idx = index_array(xlabel, label_prods)
# 			centre_adj = (xscale[idx+1] - xscale[idx]) /2
# 			axs.set_xticks(xscale[idx] + centre_adj)
# 			axs.set_xticklabels(xlabel[idx])
# 		#- Set Colorbar - #
# 		cbar = plt.colorbar(plot, ticks=[0,matr.max()/2,matr.max()])
# 		cbar.set_ticklabels(['PROX = 0', 'PROX = '+str(matr.max()/2), 'PROX >= '+str(matr.max())])
# 		# - Add Figure Text - #
# 		figtitle = "Proximity Matrix [Yr: " + str(self.year) + "]"
# 		plt.title(figtitle)
# 		if type(sortby) == pd.Series and type(scaleby) == pd.Series:
# 			xylabel = 'Products [Ordered by ' + sortby_text + ', Scaled by ' + scaleby_text + ']'
# 		elif type(sortby) == pd.Series: 																#Could Add this: str(sortby).lower() == 'sorted'
# 			xylabel = 'Products [Ordered by ' + sortby_text + ']'
# 		elif type(scaleby) == pd.Series:
# 			xylabel = 'Products [Scaled by ' + scaleby_text + ']'
# 		else: 
# 			xylabel = 'Products'
# 		axs.set_ylabel(xylabel)
# 		axs.set_xlabel(xylabel)
# 		note = 'Products: ' + str(len(products))
# 		plt.figtext(0.7, 0.01, note)
# 		plt.tight_layout()
# 		return fig


# 	def plot_proximity_simple(self, sortby_label=''):
# 		'''
# 			Simple Proximity Plot for Exploring Data
# 			Note: 
# 				[1] To use sortby_label then you should presort the data
# 		'''
# 		# - Plot Generation - #    
# 		fig = plt.figure()
# 		ax = fig.add_subplot(1,1,1)
# 		ax.matshow(self.proximity)
# 		# - Graph Labels & Titles - #
# 		ax.set_title('ProductSpace Matrix Year: %s' % self.year)
# 		if sortby_label != '': 
# 			ax.set_xlabel('Products [Sorted by: ' + sortby_label + ']')
# 			ax.set_ylabel('Products [Sorted by: ' + sortby_label + ']')
# 		else:
# 			ax.set_xlabel('Products')
# 			ax.set_ylabel('Products')
# 		return fig


# 	## -- Network Visualisation -- ##
# 	#################################

# 	def draw_network(self, show=False):
# 		'''
# 			Display Network Diagram of ProductLevelExportSystem

# 			To Do:
# 			----- 
# 				[1] Include Drawing Options

# 			Future Work:
# 			----------- 
# 				[1] Make BiPartiteGraph Representation
# 				[2] Cleaner View of MultiDiGraph 			=> Currently use Cytoscape and Gephi for Viz
# 		'''	
# 		fig = nx.draw(self.network)
# 		if show: plt.show() 
# 		else: return fig							

# 	#################################
# 	## -- Data Export Functions -- ##
# 	#################################

# 	def to_csv(self, fn, dn='data', verbose=False):
# 		'''
# 			Write Data to CSV
# 			Notes:
# 			-----
# 				[1] Build in Future Architecture for Retrieving Data and Writing to CSV
# 		'''
# 		print "\nWriting (%s) to csv file: %s" % (dn, fn)
# 		if dn == 'data':
# 			self.data.to_csv(fn)
# 		else:
# 			#self.matrix[dn].to_csv(fn)
# 			if dn == 'rca':
# 				self.rca.to_csv(fn)
# 			elif dn == 'mcp':
# 				self.rca.to_csv(fn)
# 			elif dn == 'proximity':
# 				self.proximity.to_csv(fn)
# 			else:
# 				raise ValueError("Data Obects: Data, RCA, Mcp, and Proximity are the only current options for dn : data name")

# 	##################################
# 	## -- State Saving Functions -- ##
# 	##################################

# 	## -- Entire Object -- ##
# 	#########################

# 	def to_pickle(self, fn, verbose=False):
# 		'''
# 			Pickle Entire PLES
# 		'''
# 		raise NotImplementedError

# 	def from_pickle(self, fn, verbose=False):
# 		'''
# 			Restore Entire PLES from a Pickle
# 		'''
# 		raise NotImplementedError

# 	## -- Matrices/DataFrames Only -- ##
# 	####################################

# 	def pickle_matrix(self, matr, fn=None, directory='./pickles/'):
# 		'''
# 			Pickle a Single Matrix
# 			Useful when doing analysis that takes time and just want to preserve a single Matrix (i.e PCI)
			
# 			A.pickle_matrix(A.rca, 'rca')

# 			Future Work:
# 			-----------
# 				[1] Check Directory Exists
# 		'''
# 		if type(fn) != str:
# 			fn = matr.name
# 		fn = directory + fn + '.pickle'
# 		matr.to_pickle(fn)
# 		print "Saved pickle file of matrix (%s) to: %s" % (matr.name, fn)
# 		return True

# 	def restore_pickled_matrix(self, matr, matr_name, fn, directory='./pickles/'):
# 		'''
# 			Restore a Pickled Matrix to self.rca etc.
			
# 			A.restore_pickled_matrix(A.rca, 'RCA', 'rca.pickle')

# 			Note:
# 			----
# 				[1] If change A.rca etc to use setter() then this will need to be updated!
# 		'''
# 		fn = directory + fn
# 		self.matrix = pd.read_pickle(fn)
# 		self.matrix.name = matr_name
# 		print "Read Pickle from File (%s) and created a matrix named: %s" % (fn, self.matrix.name)
# 		#self.matrix = matr
# 		return self.matrix
		


# ###
# ### DynProductLevelExportSystem moved to a new file {Imported to Use .from_csv()}
# ###
# from DynamicProductLevelExportSystem import *


# ### -- Main For Library File: ProductLevelExportSystem --- ###


# if __name__ == '__main__':
# 	print "Library File for ProductLevelExportSystem Class"
# 	print 
# 	### --- Options --- ###
# 	graphics = False

# 	### --- Test Data --- ###
# 	data = './test-data/export-testdata.csv'

# 	#################################
# 	### --- Testing DataFrame --- ###
# 	#################################
# 	A = DynProductLevelExportSystem() 	# Use DynProductLevelExportSystem for reading in test csv
# 	A.from_csv(data, dtypes=['DataFrame'])
# 	A = A[2000] 						#Tests here should only focus on A
	
# 	# - A = ProductLevelExportSystem - #
# 	print A
# 	print "Long Indexed DataFrame"
# 	print A.data
# 	print type(A.data)
# 	print "CP Matrix"
# 	print A.as_cp_matrix()  			#Need to write a as_cp_matrix from self.data option rather than just from bipartite network
# 	print "Adjaceny Matrix: Test Error"
# 	try:
# 		print A.as_adj_matrix()
# 	except:
# 		pass
# 	A.construct_bipartite()
# 	print A.as_adj_matrix()

# 	fig = A.draw_network(show=graphics)

# 	print
# 	print "Testing get_countries_from_df and get_products_from_df"
# 	A.get_countries_from_df(df=A.data, verbose=True)
# 	A.get_products_from_df(df=A.data, verbose=True)

# 	print "Testing get_countries and get_products"
# 	A.get_countries(verbose=True)
# 	A.get_products(verbose=True)

# 	# - Error Test - #
# 	print 
# 	print "Error test for NO supp_data"
# 	print "Testing RCA Matrix"
# 	try:
# 		print A.rca_matrix() 			#Base Object has self.complete_trade_network = False
# 	except:
# 		print "Test Passed!"

# 	print 
# 	print "Testing RCA Matrix"
# 	A.complete_trade_network = True
# 	print A.rca_matrix()

# 	### Converting to New Arrangement and testing only a sample cross-section ###

# 	### - Test Data Constructed - ###
# 	### - Data in pd.Series and Value --- ###
# 	A.supp_data['TotalWorldExport'] = pd.read_csv('test-data/export-TotalWorldExport-testdata.csv').set_index(keys='year').ix[2000]['TotalWorldExport']
# 	A.supp_data['TotalProductExport'] = pd.read_csv('test-data/export-TotalProductExport-testdata.csv', dtype={'productcode' : str}).set_index(keys=['year', 'productcode']).ix[2000]['TotalProductExport']
# 	A.supp_data['TotalCountryExport'] = pd.read_csv('test-data/export-TotalCountryExport-testdata.csv').set_index(keys=['year', 'country']).ix[2000]['TotalCountryExport']

# 	A.complete_trade_network = False
# 	print A.rca_matrix(verbose=True)

# 	### - Data in pd.DataFrames and pd.Series - ###
# 	### - This has been implimented as this will be a common way to import data - ###
# 	A.supp_data['TotalWorldExport'] = pd.read_csv('test-data/export-TotalWorldExport-testdata.csv').set_index(keys='year').ix[2000]
# 	A.supp_data['TotalProductExport'] = pd.read_csv('test-data/export-TotalProductExport-testdata.csv', dtype={'productcode' : str}).set_index(keys=['year', 'productcode']).ix[2000]
# 	A.supp_data['TotalCountryExport'] = pd.read_csv('test-data/export-TotalCountryExport-testdata.csv').set_index(keys=['year', 'country']).ix[2000]

# 	A.complete_trade_network = False
# 	print A.rca_matrix(verbose=True)
# 	A.complete_trade_network = True

# 	### -- Testing Mcp Method --- ###
# 	print
# 	print A.mcp_matrix(fillna=True, verbose=True)

# 	### --- Testing Proximity Method --- ###
# 	print
# 	print "Testing Proximity Method"
# 	print A.proximity_matrix(clear_temp=False,verbose=True)

# 	### --- Testing Compute_Proximity Method --- ###
# 	print 
# 	print "Testing Compute_Proximity Method"
# 	print "Symmetric Matrix"
# 	print A.compute_proximity(matrix_type='symmetric', verbose=True)
# 	print "Asymmetric Matrix"
# 	print A.compute_proximity(matrix_type='asymmetric', verbose=True)
# 	print "MinMax Matrix"
# 	print A.compute_proximity(matrix_type='minmax', verbose=True)

# 	### --- Testing Diversity and Ubiquity --- ###
# 	print
# 	print "Testing Diversity and Ubiquity"
# 	print A.compute_ubiquity(verbose=True)
# 	print A.compute_diversity(verbose=True)

# 	### --- Testing Kcn, Kpn --- ###

# 	print
# 	print "Testing Method of Reflections Iterator Function - Start with an Initial Random Ordering of Countries"
# 	A.ubiquity = A.ubiquity.reindex(np.random.permutation(A.ubiquity.index)) 
# 	print A.ubiquity
# 	print "self.compute_iterated_countryproduct_complexity(verbose=True)"
# 	print A.compute_iterated_countryproduct_complexity(verbose=True) 	#Default to 20 Iterations
# 	print "self.compute_iterated_countryproduct_complexity(iterations='rank_stability', verbose=True)"
# 	print A.compute_iterated_countryproduct_complexity(iterations='rank_stability', verbose=True)
# 	print "self.compute_iterated_countryproduct_complexity(iterations='rank_stability_country', verbose=True)"
# 	print A.compute_iterated_countryproduct_complexity(iterations='rank_stability_country', verbose=True)
# 	print "self.compute_iterated_countryproduct_complexity(iterations='rank_stability_product', verbose=True)"
# 	print A.compute_iterated_countryproduct_complexity(iterations='rank_stability_product', verbose=True)

# 	### --- Testing ECI --- ###
	
# 	### TO DO: Check if this makes sense on the Test Data ###
# 	print "TO DO: Check if this makes sense on the Test Data"

# 	print
# 	print "Testing ECI"
# 	print A.compute_eci(verbose=True)


# 	### --- Testing ECI --- ###
	
# 	### TO DO: Check if this makes sense on the Test Data ###
# 	print "TO DO: Check if this makes sense on the Test Data"

# 	print
# 	print "Testing PCI"
# 	print A.compute_pci(verbose=True)


# 	### --- Testing Filtering Methods --- ###
# 	print 
# 	print "Testing Filtering Methods"
# 	print
# 	B = A.filter_for_countries(countries=['AUS', 'USA', 'ZWE'], verbose=True)
# 	print B

# 	print
# 	C = A.filter_for_products(products=['0001', '0002'], verbose=True)
# 	print C

# 	print 
# 	print "Check Transfer of Appropriate Supp Data"
# 	print A.rca

# 	print "B-Matrix (Filtered Countries): rca_matrix()"
# 	print B.rca_matrix()
# 	print "C-Matrix (Filtered Products): rca_matrix()"
# 	print C.rca_matrix()


#  	### --- Test Sorting Methods --- ###
#  	print
#  	print "Test Sorting Methods"
#  	r = pd.Series([3,2,1], index=['AUS', 'USA', 'AFG']) 			#Produce a Random Ordering According to 3,2,1
#  	c = pd.Series([3,2,1], index=['0001', '0002', '0003'])
#  	print "Original Matrix"
#  	print A.mcp
#  	print "Sorted Rows"
#  	print r
#  	print A.sorted_matrix(A.mcp, row_sortby=r)
#  	print A.sorted_matrix(A.mcp, row_sortby=r, row_ascending=False)
#  	print "Sorted Columns"
#  	print c
#  	print A.sorted_matrix(A.mcp, column_sortby=c)
#  	print A.sorted_matrix(A.mcp, column_sortby=c, column_ascending=False)
#  	print "Sorted Rows and Columns"
#  	print A.sorted_matrix(A.mcp, row_sortby=r, column_sortby=c)

# 	print "--------"
# 	print "TESTING COMPLETED SUCCESFULLY!"
# 	print "--------"

# 	sys.exit()

# 	###############################################
# 	### --- Testing Network Object Creation --- ###
# 	###############################################
# 	print
# 	print "Testing Network Object Creation"
# 	print "dtypes=['DataFrame', 'BiPartiteGraph']"
# 	B = DynProductLevelExportSystem() 	# Use DynProductLevelExportSystem for reading in test csv
# 	B.from_csv(data, dtypes=['DataFrame', 'BiPartiteGraph'], verbose=True)
# 	print
# 	print "dtypes=['DataFrame', 'MultiDiGraph']"
# 	B = DynProductLevelExportSystem() 	# Use DynProductLevelExportSystem for reading in test csv
# 	B.from_csv(data, dtypes=['DataFrame', 'MultiDiGraph'], verbose=True)
# 	print
# 	print "dtypes=['DataFrame', 'BiPartiteGraph', 'MultiDiGraph']"
# 	B = DynProductLevelExportSystem() 	# Use DynProductLevelExportSystem for reading in test csv
# 	B.from_csv(data, dtypes=['DataFrame', 'BiPartiteGraph', 'MultiDiGraph'], verbose=True)

# 	print "--------"
# 	print "TESTING COMPLETED SUCCESFULLY!"
# 	print "--------"

# 	sys.exit()

# 	######################################
# 	### --- Testing BipartiteGraph --- ###
# 	######################################
	
# 	### --- Simple Objects --- ###
# 	A = DynProductLevelExportSystem() 	# Use DynProductLevelExportSystem for reading in test csv
# 	A.from_csv(data, dtypes=['BiPartiteGraph'])
# 	A = A[2000] 						#Tests here should only focus on A
# 	print
# 	print "[Bipartite] Testing Simple Objects"

# 	### --- Testing with Country() and Product() Objects --- ###

# 	print 
# 	print "[Bipartite] Testing Country() and Product() Objects"
# 	### Load WDI ###
# 	W = wdi.WDI('WDI_Data.csv',source_ds='d1352f394ef8e7519797214f52ccd7cc', hash_file_sep=r' ', verbose=True)
# 	C = Countries()
# 	C.build_from_wdi(W)
# 	print
# 	A = DynProductLevelExportSystem()
# 	A.from_csv(data, use_objects=True, countries_obj=C, td_classification='SITCR2L4', verbose=True)  		#td_classification is example only
# 	print A
	

# 	print A.network
# 	B = A[2000].network
# 	print B.nodes()
# 	nds = B.nodes()
# 	cntry_nodes = set(n for n,d in B.nodes(data=True) if d['bipartite']=='countries')
# 	prod_nodes = set(B) - cntry_nodes
# 	print "Country Nodes: %s" % cntry_nodes
# 	print "Product Nodes: %s" % prod_nodes
# 	# Check Country Objects #
# 	for cntry in cntry_nodes:
# 		print "Country Info for: %s" % cntry
# 		print cntry.name
# 		print cntry.iso3n  
# 	#Check Product Objects #
# 	for prod in prod_nodes:
# 		print "Product Info for: %s" % prod
# 		print "Code: %s, Classification: %s" % (prod.code, prod.classification)

# 	# Graphing B
# 	if graphics: A[2000].draw_network()

# 	### --- Testing Functions --- ###

# 	#Test CP Matrix
# 	print "Testing as_cp_matrix()"
# 	N = A[2000].network
# 	for cntry in cntry_nodes:
# 		for prod in prod_nodes:
# 			try:
# 				print "From: %s to %s has edge value: %s" % (cntry, prod, N[cntry][prod])
# 			except:
# 				pass
# 	print "as_cp_matrices()"
# 	A.as_cp_matrices()
# 	print A[2000].matrix
# 	print A[2001].matrix

# 	print "as_adj_matrix()"
# 	A[2000].as_adj_matrix() 		#Note: Adjacency Matrix is the CP Matrix
# 	print A[2000].matrix

# 	### --- Test Sortedness of Products --- ###

# 	print
# 	print "Test Sortedness of Products"
# 	l = [A[2000].products[prod] for prod in A[2000].products.keys()]
# 	print  "List of Products" 		
# 	print l
# 	print "Sorted List of Products"
# 	print sorted(l) 								#Sortedness Works
# 	print l[0].classification

# 	print
# 	print "Test Sortedness of Countries"
# 	c = [A[2000].countries[cntry] for cntry in A[2000].countries.keys()]
# 	print "List of Countries"
# 	print c
# 	print "Sorted List of Countries"
# 	print sorted(c)
# 	print c[0].name

# 	######################################
# 	### --- Testing MultiDiGraph() --- ###
# 	######################################

# 	# Load Data for Tests #
# 	#W = wdi.WDI('WDI_Data.csv',source_ds='d1352f394ef8e7519797214f52ccd7cc', hash_file_sep=r' ', verbose=False)
# 	#C = Countries().build_from_wdi(W)

# 	print
# 	print "Testing MultiDiGraph()"
# 	print
# 	print "Testing without Objects"
# 	A = DynProductLevelExportSystem()
# 	A.network_type = 'MultiDiGraph' 						
# 	A.from_csv(data, verbose=True)  		

# 	if graphics: A[2000].draw_network()
# 	print "Edges:"
# 	print A[2000].network.edges(data=True)
# 	print "Edge between AUS and WLD"
# 	print A[2000].network['AUS']['WLD']
# 	A[2000].as_adj_matrix() 		#Note: Adjacency Matrix is the CP Matrix
# 	print A[2000].matrix


# 	### --- Testing with Country and Product Objects --- ###

# 	print
# 	print "Testing with Objects"
# 	A = DynProductLevelExportSystem()
# 	A.network_type = 'MultiDiGraph'
# 	A.from_csv(data, use_objects=True, countries_obj=C, td_classification='SITCR2L4', verbose=True)  		#td_classification is example only

# 	if graphics: A[2000].draw_network()
# 	print "Edges:"
# 	print A[2000].network.edges(data=True)
# 	print "Edge between AUS and WLD"
# 	print A[2000].network[A[2000].countries['AUS']][A[2000].countries['WLD']]
# 	A[2000].as_adj_matrix() 		#Note: Adjacency Matrix is the CP Matrix
# 	print A[2000].matrix

# 	### --- NEED TO WRITE TEST CASES FOR --- ###
# 	#remove_zero_relationships_matrix()
# 	#multiindex sorting