"""
HS Trade Classifications
========================

This package contains HS trade classification details. This includes items such as Codes and Names. 
If you are looking for conversion between trade classification tables (i.e. SITCR2-HS2002). 
This information is contained in the ``pyeconlab.trade.concordance`` subpackage

Data Files
----------
see data/README.md

Notes
-----
1. Could have implemented a generic ProductCode Class but I think this approach is easier to understand and edit etc.

Future Work
-----------
1. Add Metadata to the HS objects (i.e. applicable_years, data_available_years etc.)
2. Find out what year data/H4.txt applies.
3. Add source_institution option to Revision Functions

"""

import os
import pandas as pd

from pyeconlab.util import check_directory

#-Data in `data/`-#
this_dir, this_filename = os.path.split(__file__)
DATA_PATH = check_directory(os.path.join(this_dir, "data"))

class HS(object):
	"""
	HS Classification Object

	Provides an interface to the HS Trade Classification System

	Parameters
	----------
	revision 	: 	int
					Specify HS Revision Number [1992, 1996, 2002, 2007]

	source_institution 	: 	str, optional(default="un")
							Provide source institution string (i.e. "un"). 
							See data/README.md for more information

	"""
	
	def __init__(self, revision, source_institution='un', verbose=False):
		"""
		Load HS Classification Data
		"""
		#-Attributes-@
		self.revision 			= 	revision
		self.revision_map 		= 	{ 	
										1992 : 0, 		 
										1996 : 1,
										2002 : 2,
										2007 : 3,
										# ???? : 4
									}
		self.source_institution = 	source_institution

		#-Source: United Nations-#
		if source_institution == 'un':
			self.source_web = u"http://wits.worldbank.org/referencedata.html"
			self.data = pd.read_csv(DATA_PATH + 'un/' + 'H'+str(self.revision_map[revision])+'.txt')
		#-Source: World Bank - WITS-#
		elif source_institution == 'wits': 	
			raise NotImplementedError('wits not yet implemented')	#-Update Error Once Implemented-#
		else:
			raise ValueError("source_institution must be 'un'")
		#-Run Some Standard Methods to Populate attributes-#
		self.construct_level()
		self.construct_description()

	def __repr__(self):
		obstring 	= 	"HS Revision: %s\n" % self.revision 	+\
						"-----------------\n"					+\
						"Level 1 Codes: %s\n" % len(self.L1) 	+\
						"Level 2 Codes: %s\n" % len(self.L2) 	+\
						"Level 3 Codes: %s\n" % len(self.L3) 	+\
						"Level 4 Codes: %s\n" % len(self.L4) 	+\
						"Level 5 Codes: %s\n" % len(self.L5) 	+\
						"Level 6 Codes: %s\n" % len(self.L6)  	+\
						"\n"									+\
						"Source: %s (%s)" % (self.source_institution, self.source_web)
		return obstring

	#------------#
	#-Properties-#
	#------------#

	@property 
	def L1(self):
		return self.data[self.data['level'] == 1] 			#Does this really make sense to list? Do I need to construct my own Level Codes based on Level 6

	@property 
	def L2(self):
		return self.data[self.data['level'] == 2]

	@property 
	def L3(self):
		return self.data[self.data['level'] == 3] 			#Does this really make sense to list? Do I need to construct my own Level Codes based on Level 6

	@property 
	def L4(self):
		return self.data[self.data['level'] == 4]

	@property 
	def L5(self):
		return self.data[self.data['level'] == 5]			#Does this really make sense to list? Do I need to construct my own Level Codes based on Level 6
 
	@property 
	def L6(self):
		return self.data[self.data['level'] == 6]

	def get_level(self, level):
		""" 
		Return Level Data based on a specified level

		Parameters
		----------
		level 	: 	int
					Specify which level of the HS system (1 to 6)
		"""
		if level == 1:
			return self.L1
		elif level == 2:
			return self.L2
		elif level == 3:
			return self.L3
		elif level == 4:
			return self.L4
		elif level == 5:
			return self.L5
		elif level == 6:
			return self.L6
		else:
			raise ValueError("[ERROR] Level can only be specifed as 1,2,3,4,5 or 6!")

	@property 
	def codes(self):
		return self.data['Code']


	#-------------------#
	#-Construct Methods-#
	#-------------------#

	def construct_level(self):
		"""
		Build Level Indicator From the Code Length
		"""
		self.data['level'] = self.data['Code'].apply(lambda x: len(x))

	def construct_description(self):
		"""
		Construct a Full Description from ShortDescription and LongDescription

		Warnings
		--------
		1. Currently this doesn't look necessary. ShortDescription contains enough of the information
		"""
		self.data['Description'] = self.data[['ShortDescription']]

	#---------------#
	#-Other Methods-#
	#---------------#

	def description(self, code):
		""" 
		Return Code Description String

		Parameters
		----------
		code 	: 	<verify this> str or int
					Supply Code of desired HS item

		Returns
		-------
		value 	: 	str
					Description of the HS Code

		"""
		return self.data[self.data['Code'] == code]['Description'].values[0] 		


	def code_description_dict(self, level=None):
		""" 
		Return a Dictionary of HS Codes and Descriptions

		Parameters
		----------
		level 	: 	int, optional(default=None)
					Specify a specific Level [1,2,3,4,5 or 6] otherwise it will return ALL levels
		Returns
		-------
		dictionary 	: 	dict
						A dictionary of HS Codes to Descriptions
		"""
		if type(level) == int:
			data = self.get_level(level)
			return data[['Code', 'Description']].set_index(['Code'])['Description'].to_dict()
		return self.data[['Code', 'Description']].set_index(['Code'])['Description'].to_dict()
		

#----------#
#-Revision-#
#----------#

def HS1992():
	"""
	Return a HS 1992 Object
	"""
	return HS(revision=1992)

def HS1996():
	"""
	Return a HS 1996 Object
	"""
	return HS(revision=1996)

def HS2002():
	"""
	Return a HS 2002 Object
	"""
	return HS(revision=2002)

def HS2007():
	"""
	Return a HS 2007 Object
	"""
	return HS(revision=2007)

