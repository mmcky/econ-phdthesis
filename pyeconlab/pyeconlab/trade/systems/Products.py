'''
	Project: 	TradeSystem
	Class: 		Products
	Author: 	Matthew McKay <mamckay@gmail.com>

	General Structure:
	------------------
		[1] Simple Container for Storing Product Values

	Purpose: 
	--------
		[1] Product() can serve as Node or Edge Objects in TradeSystem Networks

	Example:
	--------
		A = Products()
		A['3330'] 	: 	Product() 		# Instance of Product Oil
		
		# - Core Attributes - #
		A.value 				: 	float
		A.code 					: 	str()
		A.classification 		: 	str()
		A.description 			: 	str()		

	[SEE _future/producst.py for Enhancements (i.e. SITC and HS objects)]

'''

### --- Standard Library Imports --- ###

import os
import sys
import pickle
import numpy as np

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

# try: 
# 	import mydatasets as md
# except:
# 	print "[mydatasets] The MyDatasets Repository cannot be found!"
# 	sys.exit()

### --- Product Class --- ###

class Product():
	'''
		Product Object for Network Edge Data
		Notes:
			[1] ntype 	-> 	'networktype' to indicate if it is a node or edge
			[2] Direction on DiGraph() indicates export and import
		Future:
			[1] Might be better to model these as SITC or HS Products (See: _future/HSProduct() and SITCProduct())
	'''
	def __init__(self, value=np.nan, code=None, classification=None, ntype='edge', verbose=False):
		# - Core Items - #
		self.value = value
		self.code = code
		self.description = None
		self.classification = classification
		## -- Display Settings -- ##
		self.ext_display = False
		# - Type Settings - #
		self.ntype = ntype

	def __repr__(self):
		'''
			Allows for extended and summary information. 
			Default is to return a Value so that network analysis can use transparently on top of the object without having to know anything 
			about the underlying Prodcut() object.			
		'''
		if self.ntype == 'edge':
			if self.ext_display: return str(self.value) + ' [' + self.code + '-' + self.classification + ']'
			else: return repr(self.value)
		elif self.ntype == 'node':
			if self.ext_display: return self.code + '\t' + self.classification + '\t' + self.description
			else: return str(self.code)
		else:
			raise ValueError("ntype attribute must be either 'node' or 'edge'")

	def __add__(self, other):
		'''
			Options:
				[1] Could construct a new Product() object with self.value = sum() and self.productcode=[<list of productcodes>]
		'''
		return self.value + other.value

	def __lt__(self, other):
		'''
			Given a List of Product Objects, sort them by the code attribute
		'''
		return self.code < other.code

	### --- Meta Data for Objects --- ###

	def set_description(self, description):
		self.description = description


### --- Products Class --- ###

### Note: This may not be needed as because of product.value is different then there is no pre-populating until network formation

class Products():
	'''
		Product Catalogue for Product() instances
	'''

	def __init__(self, fn=None, verbose=False):
		## -- Check if ./pickles/products.pickle exists -- ##
		if fn == None:
			if os.path.isfile('pickles/products.pickle'):
				if verbose: print "Loading data from default pickle file: ./pickles/products.pickle"
				self.products = pickle.load('pickles/products.pickle')
			else:
				if verbose: print "No Default Pickle Data Found ... Creating an empty Dict() but will need to run self.from_csv()"
				self.products = dict()
			self.productcodes = sorted(self.products.keys())
		else:
			## - From CSV -- ##
			raise NotImplementedError
		return self.products

	## -- IO -- ##

	def from_csv(self, fn):
		'''
			Load Product Data from: <filename>
		'''
		raise NotImplementedError	

	def to_pickle(self, fn='pickles/products.pickle'):
		'''
			Save Products() to Pickle File
		''' 
		raise NotImplementedError	

	## -- Additions & Subtractions -- ##

	def add_product(self, productcode, df_data, save_changes=True, verbose=False):
		'''
			Add Product to Products()
		'''
		p = Product()
		raise NotImplementedError	

	def remove_product(self, productcode, save_changes=True, verbose=False):
		'''
			Remove Product from Products()
		'''
		raise NotImplementedError	

### --- Main --- ###

if __name__ == '__main__':
	### --- Options --- ###
	graphics = False

	print "Library File for Products Class"
	print
	print "Tests for Product Class"
	p = Product()
	p.value = 100
	print p
	print p + p
	
	q = Product(value=200, code='3330', classification='SITCR2')
	q.ext_display = True
	print q

	print p + q

	#Product as a Node
	print
	print "Testing Product as a Node"
	p = Product(code='3330', classification='SITCR2', ntype='node')
	print p
	p.set_description('Crude Oil')
	p.ext_display = True
	print p