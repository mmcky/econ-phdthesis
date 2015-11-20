"""
Testing DTA and HDF Data Structures using NBERFeenstraWTFConstructor

Running Time: 2303.473s [38 minutes]
"""

from nose import with_setup
import pandas as pd
from pandas.util.testing import assert_series_equal, assert_frame_equal
from numpy.testing import assert_allclose

from pyeconlab.util import package_folder, expand_homepath, check_directory
from ..constructor import NBERFeenstraWTFConstructor

#-DATA Paths-#
SOURCE_DATA_DIR = check_directory("E:\\work-data\\x_datasets\\36a376e5a01385782112519bddfac85e\\") 			#Win7!
TEST_DATA_DIR = package_folder(__file__, "data") 


class TestConstructorDTAvsHDFYearIndex():
	""" 
	Test HDF Year Indexed File
	Test the Constructor Conversion to HDF Year Indexed DataFormat

	Files
	-----	
	STATA 	.dta files: wt??.dta 
	HDF 	.h5 file: 	wtf00-62_yearindex.h5

	Notes
	-----
	[1] This Test Suite Takes a LONG time to complete. [RAM:~12GB]
		You can filter out these tests out using ``nosetests -a "!slow"`` or you can select them ``nosetests -a "slow"

	To Inspect in IPYthon:
	---------------------
	from pyeconlab import NBERFeenstraWTFConstructor
	SOURCE_DATA_DIR = check_directory("E:\\work-data\\x_datasets\\36a376e5a01385782112519bddfac85e\\") #win7
	a = NBERFeenstraWTFConstructor(source_dir=SOURCE_DATA_DIR)
	b = pd.HDFStore(SOURCE_DATA_DIR + 'wtf62-00_yearindex.h5')
	"""
	
	@classmethod
	def setUpClass(cls):
		""" Setup NBERFeenstraWTFConstructor using: source_dir """
		cls.obj = NBERFeenstraWTFConstructor(source_dir=SOURCE_DATA_DIR, ftype='dta') 		#Load Raw Data from dta files into Object
		#-YearIndexed HDF File-#
		try: 																				#No Need to Recompute if File is found
			cls.hdf_year = pd.HDFStore(SOURCE_DATA_DIR + 'wtf62-00_yearindex.h5')
		except:
			cls.obj.convert_stata_to_hdf_yearindex()  							
			cls.hdf_year = pd.HDFStore(SOURCE_DATA_DIR + 'wtf62-00_yearindex.h5')
	
	@classmethod
	def tearDownClass(cls):
		del cls.obj
		cls.hdf_year.close()

	# - Tests for Year Indexed HDF File - #

	def test_dta_hdfyearindex_obs(self):
		""" Test Number of Observations Match in Every Year """
		df1 =  	self.obj.raw_data
		hdf = 	self.hdf_year
		for year in self.obj.years:
			obj_year = df1[df1['year'] == year]
			obj_year_obs = len(obj_year)
			hdf_year = hdf['Y'+str(year)] 				#Extract Year from File
			hdf_year_obs = len(hdf_year)
			assert obj_year_obs == hdf_year_obs, "DTA Observations: %s != HDF Observations: %s" % (obj_year_obs, hdf_year_obs)

	def test_convert_stata_to_hdf_yearindex(self):
		""" 
		Check if Pandas Frames are Equal for every year between obj.raw_data and the hdf file
		Notes: This MAY fail as HDF converts float64 to int32 when appropriate in a given year. 
		"""
		df1 =  	self.obj.raw_data
		hdf = 	self.hdf_year
		for year in self.obj.years:
			obj_year = df1[df1['year'] == year]
			hdf_year = hdf['Y'+str(year)] 				#Extract Year from File
			assert_frame_equal(obj_year, hdf_year)

	def test_convert_stata_to_hdf_yearindex_values(self):
		""" 
		Compares the Numeric Export/Import Values

		Notes
		-----
		[1] This test uses assert_allclose() becuase assert_frame_equal asserts equal types in addition to values
			format='table' in HDF saves many years 'value' column as int32 rather than float64
		"""
		df1 =  	self.obj.raw_data
		hdf = 	self.hdf_year
		# - Values - #
		for year in self.obj.years:
			obj_year = df1[df1['year'] == year] 	#Filter
			s1 = obj_year['value'] 					#Value Series
			hdf_year = hdf['Y'+str(year)] 			#Extract Year from File
			s2 = hdf_year['value']
			assert_allclose(s1, s2) 				#Compare Numeric Values and Not Type.

	def test_convert_stata_to_hdf_yearindex_quantity(self):
		""" Compares the Numeric Export/Import Quantities
		
		Notes
		-----
		[1] This test uses assert_allclose() becuase assert_frame_equal asserts equal types in addition to values
			format='table' in HDF saves many years 'value' column as int32 rather than float64
		"""
		df1 =  	self.obj.raw_data
		hdf = 	self.hdf_year
		# - Quantity - #
		for year in self.obj.years:
			obj_year = df1[df1['year'] == year] 	#Filter
			s1 = obj_year['quantity'] 				#Value Series
			hdf_year = hdf['Y'+str(year)] 			#Extract Year from File
			s2 = hdf_year['quantity']
			assert_allclose(s1, s2) 

TestConstructorDTAvsHDFYearIndex.slow = True 		#Class Attribute To Indicate Slow Test 


class TestConstructorDTAvsHDFRawData():
	""" 
	Tests for HDF Raw Data File
	Test the Constructor Conversion to HDF RAW DATA file

	Files
	-----	
	STATA 	.dta files: wt??.dta 
	HDF 	.h5 file: 	wtf00-62_raw.h5

	Notes
	-----
	[1] This Test Suite Takes a LONG time to complete. [RAM: ~25.4GB]
		You can filter out these tests out using ``nosetests -a "!slow"`` or you can select them ``nosetests -a "slow"
	[2] Import raw_data from HDF to prevent multiple imports

	To Inspect in IPYthon:
	---------------------
	from pyeconlab import NBERFeenstraWTFConstructor
	SOURCE_DATA_DIR = check_directory("E:\\work-data\\x_datasets\\36a376e5a01385782112519bddfac85e\\") 	#win7
	a = NBERFeenstraWTFConstructor(source_dir=SOURCE_DATA_DIR)
	b = pd.HDFStore(SOURCE_DATA_DIR + 'wtf62-00_raw.h5')
	"""

	@classmethod
	def setUpClass(cls):
		""" Setup NBERFeenstraWTFConstructor using: source_dir """
		cls.obj = NBERFeenstraWTFConstructor(source_dir=SOURCE_DATA_DIR, ftype='dta') 		#Load Raw Data from dta files into Object
		try: 																				#No Need to Recompute if File is found
			cls.hdf_raw_df = pd.HDFStore(SOURCE_DATA_DIR + 'wtf62-00_raw.h5')['raw_data'] 	#Import Data to prevent multiple re-imports
		except:
			cls.obj.convert_raw_data_to_hdf() 									
			cls.hdf_raw_df = pd.HDFStore(SOURCE_DATA_DIR + 'wtf62-00_raw.h5')['raw_data'] 	#Import Data to prevent multiple re-imports

	@classmethod
	def tearDownClass(cls):
		del cls.obj
		del cls.hdf_raw_df

	# - Raw Data HDF File Tests - #

	def test_dta_hdfraw_obs(self):
		""" Test Number of Observations Match """
		obj_obs = len(self.obj.raw_data)
		hdf_obs = len(self.hdf_raw_df)
		assert obj_obs == hdf_obs, "DTA Observations: %s != HDF Observations: %s" % (obj_obs, hdf_obs)

	def test_convert_raw_data_to_hdf(self):
		""" Test if DataFrames are Equivalent """
		df1 = self.obj.raw_data
		df2 = self.hdf_raw_df
		assert_frame_equal(df1, df2)

	def test_convert_raw_data_to_hdf_values(self):
		""" Compares the Numeric Export/Import Values """
		s1 =  self.obj.raw_data['value']
		s2 =  self.hdf_raw_df['value']
		assert_series_equal(s1, s2)

	def test_convert_raw_data_to_hdf_quantity(self):
		""" Compares the Numeric Export/Import Quantities """
		s1 =  self.obj.raw_data['quantity']
		s2 =  self.hdf_raw_df['quantity']
		assert_series_equal(s1, s2)

TestConstructorDTAvsHDFRawData.slow = True 			#Class Attribute To Indicate Slow Test




### -------------------------------------------------------------------- ###
### ---> Combined Version of Above to Reduce DTA Import Duplication <--- ###
### ---> If choose to reinstate it will require updating 			<--- ###
### -------------------------------------------------------------------- ###

	# class TestConstructorRAWvsHDF5():
	# 	"""
	# 	Test the Constructor Conversion to HD5 DataFormat
		
	# 	Files
	# 	-----	
	# 	STATA 	.dta files: wt??.dta 
	# 	HDF 	.h5 file: wtf00-62_yearindex.h5, wtf00-62_raw.h5
		
	# 	Notes
	# 	----- 
	# 	[1] This Class doesn't seem to destroy objects after tests!? **Trying setUp for EVERY test**
	# 	[2] This Test Suite Takes a LONG time to complete. 
	# 		You can filter out these tests out using ``nosetests -a "!slow"`` or you can select them ``nosetests -a "slow"
	# 	[3] These Tests use ~25Gb RAM

	# 	To Inspect in IPYthon:
	# 	---------------------
	# 	from pyeconlab import NBERFeenstraWTFConstructor
	# 	SOURCE_DATA_DIR = check_directory("E:\\work-data\\x_datasets\\36a376e5a01385782112519bddfac85e\\") #win7
	# 	a = NBERFeenstraWTFConstructor(source_dir=SOURCE_DATA_DIR)
	# 	b = pd.HDFStore(SOURCE_DATA_DIR + 'wtf62-00_yearindex.h5')
	# 	"""

	# 	#-SetUp-#

	# 	@classmethod
	# 	def setUpClass(cls):
	# 		""" Setup NBERFeenstraWTFConstructor using: source_dir """
	# 		cls.obj = NBERFeenstraWTFConstructor(source_dir=SOURCE_DATA_DIR, ftype='dta') 		
	# 		#-YearIndexed HDF File-#
	# 		try: 																	
	# 			cls.hdf_year = pd.HDFStore(SOURCE_DATA_DIR + 'wtf62-00_yearindex.h5') 				#Reference and bring in year by year
	# 		except:
	# 			cls.obj.convert_stata_to_hdf_yearindex()  							
	# 			cls.hdf_year = pd.HDFStore(SOURCE_DATA_DIR + 'wtf62-00_yearindex.h5')
	# 		#-RAWData HDF File -#
	# 		try: 																	
	# 			cls.hdf_raw_df = pd.HDFStore(SOURCE_DATA_DIR + 'wtf62-00_raw.h5')['raw_data'] 		#Import the Data so it doesn't get imported multiple times
	# 		except:
	# 			cls.obj.convert_raw_data_to_hdf() 									
	# 			cls.hdf_raw_df = pd.HDFStore(SOURCE_DATA_DIR + 'wtf62-00_raw.h5')['raw_data']

	# 	# - Year Indexed HDF File - #
	# 	# - Importing the Data via HDF File is Efficient In this Case - #

	# 	def test_convert_stata_to_hdf_yearindex(self):
	# 		df1 =  	self.obj.raw_data
	# 		hdf = 	self.hdf_year
	# 		for year in self.obj.years:
	# 			obj_year = df1[df1['year'] == year]
	# 			hdf_year = hdf['Y'+str(year)]
	# 			assert_frame_equal(obj_year, hdf_year)

	# 	test_convert_stata_to_hdf_yearindex.slow = True 							#Slow Attribute Can be skipped nosetests -a '!slow'

	# 	def test_convert_stata_to_hdf_yearindex_values(self):
	# 		""" 
	# 		Compares the Numeric Export/Import Values

	# 		Notes
	# 		-----
	# 		[1] This test uses assert_allclose() becuase assert_frame_equal asserts equal types in addition to values
	# 			format='table' in HDF saves many years 'value' column as int32 rather than float64
	# 		"""
	# 		df1 =  	self.obj.raw_data
	# 		hdf = 	self.hdf_year
	# 		# - Values - #
	# 		for year in self.obj.years:
	# 			obj_year = df1[df1['year'] == year] #Filter
	# 			s1 = obj_year['value'] 				#Value Series
	# 			hdf_year = hdf['Y'+str(year)]
	# 			s2 = hdf_year['value']
	# 			assert_allclose(s1, s2) 											#Compare Numeric Values and Not Type.

	# 	test_convert_stata_to_hdf_yearindex_values.slow = True

	# 	def test_convert_stata_to_hdf_yearindex_quantity(self):
	# 		""" Compares the Numeric Export/Import Quantities """
	# 		df1 =  	self.obj.raw_data
	# 		hdf = 	self.hdf_year
	# 		# - Quantity - #
	# 		for year in self.obj.years:
	# 			obj_year = df1[df1['year'] == year] 	#Filter
	# 			s1 = obj_year['quantity'] 				#Value Series
	# 			hdf_year = hdf['Y'+str(year)]
	# 			s2 = hdf_year['quantity']
	# 			assert_allclose(s1, s2) 

	# 	test_convert_stata_to_hdf_yearindex_quantity.slow = True

	# 	# - Raw Data HDF File - #

	# 	def test_convert_raw_data_to_hdf(self):
	# 		df1 = self.obj.raw_data
	# 		df2 = self.hdf_raw_df
	# 		assert_frame_equal(df1, df2)

	# 	test_convert_raw_data_to_hdf.slow = True 										

	# 	def test_convert_raw_data_to_hdf_values(self):
	# 		""" Compares the Numeric Export/Import Values """
	# 		s1 =  self.obj.raw_data['value']
	# 		s2 =  self.hdf_raw_df['value']
	# 		assert_series_equal(s1, s2)

	# 	test_convert_raw_data_to_hdf_values.slow = True

	# 	def test_convert_raw_data_to_hdf_quantity(self):
	# 		""" Compares the Numeric Export/Import Quantities """
	# 		s1 =  self.obj.raw_data['quantity']
	# 		s2 =  self.hdf_raw_df['quantity']
	# 		assert_series_equal(s1, s2)

	# 	test_convert_raw_data_to_hdf_quantity.slow = True

	# 	@classmethod
	# 	def tearDownClass(cls):
	# 		del cls.obj
	# 		cls.hdf_raw.close()
	# 		cls.hdf_year.close()