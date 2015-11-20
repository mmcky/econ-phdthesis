"""
SITC Trade Classifications
==========================

This package contains SITC trade classification details. This includes items such as Codes and Names. 
If you are looking for conversion between trade classification tables (i.e. SITCR2-HS2002). 
This information is contained in the ``pyeconlab.trade.concordance`` subpackage

Data Files
----------
see data/README.md

Future Work
-----------
1. Add Metadata to the SITC objects (i.e. applicable_years, data_available_years etc.)
2. Add source_institution option to Revision Functions

"""

import os
import copy
import pandas as pd

from pyeconlab.util import check_directory

# - Data in data/ - #
this_dir, this_filename = os.path.split(__file__)
DATA_PATH = check_directory(os.path.join(this_dir, "data"))

class SITC(object):
	"""
	SITC Classification Object

	Provides an interface to the SITC Trade Classification System

	Parameters
	----------
	revision 	: 	int
					Specify SITC Revision Number [1,2,3,4]

	source_institution 	: 	str, optional(default="un")
							Provide source institution string (i.e. "un"). 
							See data/README.md for more information

	"""

	def __init__(self, revision, source_institution='un', verbose=False):
		"""
		Load SITC Classification Data
		"""
		#-Attributes-#
		self.revision 			= revision
		self.source_institution = source_institution

		#-Source: United Nations-#
		if source_institution == 'un':
			self.source_web = u"http://unstats.un.org/unsd/tradekb/Knowledgebase/UN-Comtrade-Reference-Tables"
			self.data = pd.read_csv(DATA_PATH + 'un/' + 'S'+str(revision)+'.txt')
		#-Source: World Bank - WITS-#
		elif source_institution == 'wits':
			raise NotImplementedError('wits not yet implemented') 		#-Update Error Message when Implemented
		else:
			raise ValueError("source_institution must be 'un'")
		#-Run Some Standard Methods-#
		self.construct_level()
		self.construct_description()

	def __repr__(self):
		obstring 	= 	"SITC Revision: %s\n" % self.revision 	+\
						"-----------------\n"					+\
						"Level 1 Codes: %s\n" % len(self.L1) 	+\
						"Level 2 Codes: %s\n" % len(self.L2) 	+\
						"Level 3 Codes: %s\n" % len(self.L3) 	+\
						"Level 4 Codes: %s\n" % len(self.L4) 	+\
						"Level 5 Codes: %s\n" % len(self.L5) 	+\
						"\n"									+\
						"Source: %s (%s)" % (self.source_institution, self.source_web)
		return obstring

	#------------#
	#-Properties-#
	#------------#

	@property 
	def L1(self):
		return self.data[self.data['level'] == 1]

	@property 
	def L2(self):
		return self.data[self.data['level'] == 2]

	@property 
	def L3(self):
		return self.data[self.data['level'] == 3]

	@property 
	def L4(self):
		return self.data[self.data['level'] == 4]

	@property 
	def L5(self):
		return self.data[self.data['level'] == 5]

	def get_level(self, level):
		""" 
		Return level data based on a specified level
		
		Parameters
		----------
		level 	: 	int
					Specify which level of the SITC system (1 to 5)

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
		else:
			raise ValueError("[ERROR] Level can only be specifed as 1,2,3,4 or 5!")

	@property 
	def codes(self):
		return self.data['Code']

	def get_codes(self, level):
		"""
		Retrive a Code List by Level
		"""
		data = copy.deepcopy(self.get_level(level)['Code']) 	#Copy to Make New List rather than a Slice
		data = data.reset_index()
		del data['index'] 										#Drop obs number from Original Data File
		colname = 'SITCL' + str(level)
		data.rename_axis({'Code' : colname}, axis=1, inplace=True)
		return sorted(list(data[colname]))


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
			#-Started Work but current will just return the Short Description-#
			#
			# def decide_join(sd,ld):
			# 	""" Decide How to Joing ShortDescription and LongDescription """
			# 	if ld[0:2] == '--':
			# 		return sd + ld[2:]
			# 	elif sd == ld:
			# 		return sd
			# 	else:
			# 		raise ValueError("What to do?")
		
		self.data['Description'] = self.data[['ShortDescription']]

	#---------------#
	#-Other Methods-#
	#---------------#

	def description(self, code):
		""" 
		Return SITC Code Description String

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
		Return a Dictionary of SITC Codes and Descriptions

		Parameters
		----------
		level 	: 	int, optional(default=None)
					Specify a specific Level [1 to 5] otherwise it will return ALL levels
		Returns
		-------
		dictionary 	: 	dict
						A dictionary of SITC Codes to Descriptions
		"""
		if type(level) == int:
			data = self.get_level(level)
			return data[['Code', 'Description']].set_index(['Code'])['Description'].to_dict()
		return self.data[['Code', 'Description']].set_index(['Code'])['Description'].to_dict()

	#--------------#
	#-File Methods-#
	#--------------#

	def codes_to_file(self, level, fltype='csv', verbose=True):
		"""
		Write Requested SITC Codes to a File (i.e. Stata, CSV)

		Parameters
		----------
		level 	: 	int
					Desired Level of the SITC System [None = ALL]

		fltype 	: 	str, optional(default="csv")
					Specify File Type [Available: 'csv', 'stata']

		.. 	Future Work
			1. Specify a Location? Currently this will save the file in the users working directory

		"""
		l = self.get_codes(level=level)
		l = pd.DataFrame(l, columns=['sitc%s'%level])
		l['marker'] = 1
		if fltype == 'stata':
			l.to_stata('SITC-R%s-L%s-codes.dta'%(self.revision, level), write_index=False)
		elif fltype == 'csv':
			l.to_csv('SITC-R%s-L%s-codes.csv'%(self.revision, level), columns=['sitc%s'%level, 'marker'], index=False)
		else:
			raise NotImplementedError("%s is not yet implimented" % fltype)


#-----------#
#-Revisions-#
#-----------#

def SITCR1():
	"""
	Return an SITC Revision 1 Object
	"""
	sitc = SITC(revision=1)
	return sitc 

def SITCR2():
	"""
	Return an SITC Revision 2 Object
	"""
	return SITC(revision=2)

def SITCR3():
	"""
	Return an SITC Revision 3 Object
	"""
	return SITC(revision=3)

def SITCR4():
	"""
	Return an SITC Revision 4 Object
	"""
	return SITC(revision=4)


