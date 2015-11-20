'''
	Classification Tables
'''

import os
import re
import pandas as pd

# - Local Data Directories - #
data_dir = 'data'
classification_dir = 'classification'

class UNSITCR2L4(object):

	def __init__(self):
		src_fl = data_dir + r'/' + classification_dir + r'/' + 'S2.txt'
		self.raw_data = pd.read_csv(src_fl)
		self.classification = 'SITCR2'
		self.revision = 'Rev. 2'
		self.source = 'UNSTATS'
		self.source_web = r'http://unstats.un.org/unsd/tradekb/Knowledgebase/UN-Comtrade-Reference-Tables'
		# Generate Different Level Names #

	@property
	def names(self):
		pass

	def get_name(self, code):
		pass

# - WITS Objects - #
class WITSSITC(object):
	'''
		Core Data: self.data[Revision#][Level#][ProductCodes]
	'''

	classification 	= 	'SITC'
	revisions 		= 	[1,2,3,4]
	source 			= 	u'WITS, World Bank'
	source_web 		= 	u'https://wits.worldbank.org/referencedata.html'

	data 			= dict() # Revisions #

	def __init__(self, verbose=False):
		# - Source Data - #
		src_fn = data_dir + r'/' + classification_dir + r'/' + 'SITCProducts.xls'
		for revision in self.revisions:
			raw_data = pd.read_excel(io=src_fn, sheetname=self.classification + ' ' + str(revision))
			tiers = dict()
			for tier in raw_data['Tier'].unique():
				tiers[tier] = raw_data[raw_data['Tier'] == tier].set_index('ProductCode')['ProductDescription']
			# Replace UN Special Codes Hierarchicaly #
			for tier in tiers.keys():
				if tier <= 1:
					pass
				for code, description in tiers[tier].iteritems():
					if re.search("UN Special Code", description):
						level = len(code)
						if verbose: print "Replacing: (%s, %s) with (%s, %s)" % (code, tiers[tier][code], code[0:level-1], tiers[tier-1][code[0:level-1]])
						tiers[tier][code] = tiers[tier-1][code[0:level-1]]
			# Write To Data Dictionary #
			self.data[revision] = tiers

	def get_table(self, revision, level):
		return self.data[revision][level]

	def get_class(self, revision, level):
		if revision == 2 and level == 4:
			return WITSSITCR2L4(dta=self.data[revision][level]) 		#Think about this interface?
		else:
			raise NotImplementedError
			

class WITSSITCR2L4(object):
	'''
		Construct Table of Information for SITC rev 2 Level 4 Codes
	'''
	classification 	= u'SITC'
	revision 		= 2
	level 			= 4
	source 			= u'WITS, World Bank'
	source_web 		= u'https://wits.worldbank.org/referencedata.html'

	data 			= pd.DataFrame()

	def __init__(self, dta=None):		
		# - Name - #
		self.name = self.classification + 'R' + str(self.revision) + 'L' + str(self.level)
		if type(dta) == pd.Series:
			self.data = self.data.join(dta, how='right')
		else:
			# Load Data From a File #
			# - Source Data - #
			src_fn = data_dir + r'/' + classification_dir + r'/' + 'SITCProducts.xls'
			try:
				raw_data = pd.read_excel(io=src_fn, sheetname=self.classification + ' ' + str(self.revision))
			except:
				raise ValueError("[Error] File: %s is not able to be read as specified (Check Sheet Name)" % (src_fn))
			# Product Descriptions #
			prod_desc = raw_data[raw_data['Tier'] == self.level].set_index('ProductCode')['ProductDescription']

			### ---> WORKING HERE < ---- ####
			# Treat 'UN Special Code' Aggregates #
			for code, desc in prod_desc.iteritems():
				if desc == u'UN Special Code':
					pass 									
			### ---> WORKING HERE < ---- ####
			
			self.data = self.data.join(prod_desc, how='right')
			
			# Other Meta Data Goes Here with appropriate getter function #

	def __getitem__(self, index):
		return self.data.ix[index]  	# Return Data Series

	def get_description(self, productcode):
		return self.data.ix[productcode]['ProductDescription']


if __name__ == '__main__':
	print "Testing: WITSSITCR2L4"
	wits = WITSSITCR2L4()
	print wits.name
	print wits['0010']
	print wits.get_description('0010') 		#Currently this is coded as UN Special Code
	print wits['3330']
	print wits.get_description('3330')

	print
	print "Testing: WITSSITC"
	wits = WITSSITC()
	witsr2l4 = wits.get_class(revision=2, level=4)
	print witsr2l4.name
	print witsr2l4['0010']
	print witsr2l4.get_description('0010')
	print witsr2l4['3330']
	print witsr2l4.get_description('3330')