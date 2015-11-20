'''
	Tests for Trade Datasets
	Package Location: pyeconlab/trade/datasets/tests
'''

import unittest
import os

from ..NBERFeenstraWTF.constructor import NBERFeenstraWTFConstructor

source_dir = os.path.expanduser('~')+'/work-data/36a376e5a01385782112519bddfac85e/'

class TestRandomDataPointsInRawDataset():
	'''
		Some Random Checks of Known Data From the Dataset
		Known Data Points are extracted from the Original Stata *.dta files
	'''

	# - List of Test Cases - #
	# (year,icode,importer,ecode,exporter,sitc4,unit,dot,value,quantity,obs)					
	# file: data/nberfeenstra_wtf62_random_sample.csv
	test_data_wtf62	= 	[	
						(1962,"135040","Morocco","586160","Poland","6732","",1,50,,50365),
						(1962,"557560","Switz.Liecht","441960","Cyprus","0570","",1,4,,431506),
						(1962,"218400","USA","353880","Jamaica","8210","",1,9,,131209),
						(1962,"582000","Czechoslovak","100000","World","6129","",,3,,440012),
						(1962,"162880","Ghana","538260","UK","5419","",1,2520,,82311),
						(1962,"334840","Mexico","211240","Canada","8852","",1,78,,160438),
						(1962,"451160","Cambodia","538260","UK","5530","",1,23,,266789),
						(1962,"211240","Canada","537240","Spain","6648","",1,1,,126335),
						(1962,"454100","Korea Rep.","458960","Taiwan","2472","",1,70,,282370),
						(1962,"164040","Kenya","538260","UK","6129","",2,12,,89457),
					]


	def setUp(self):
		'''
			Load NBERFeenstraWTF
		'''
		self.a = NBERFeenstraWTFConstructor(source_dir=source_dir, years=[1962], standardize=False, verbose=True) #This should Return a NBERFeenstraWTF Object

		# - Working Here - #


	def test_wtf62_random_raw(self):
		'''
			Conduct some random tests to ensure data alignment is retained
		'''
		for item in self.test_data_wtf62:
			# True Data #
			(year,icode,importer,ecode,exporter,sitc4,unit,dot,value,quantity,obs) = item
			# Data in Object #

			# - WORKING HERE - #
			# Construct a Test Generator of Results etc.

	def check_equal(a,b):
		assert a == b, "%s != %s" % (a,b)


