"""
Tests NBERFeenstra World Trade Flows (Constructor Object)

Test Suites:
-----------
[1] TestSmallSampleDataset 				> Test the Constructor for Logical Flaws (test attributes etc. on a small scale)
[2] TestConstructorAgainstKnownRawData 	> Test the Constructor with Real Raw Data 

Notes
-----
[1] Test Data is stored in ./data
[2] Use a Package Config file to determing locations of SOURCE_DIR etc. 
	Currently tuned to my DEV environment using "~/work-data"
[3] read_stata and read_csv don't import "units" in the same way
	stata 	: '' -> ''
	csv 	: '' -> np.nan
	df['units'] = df['units'].apply(lambda x: np.nan if x == '' else x)

Current Work:
------------
[1] TestConstructorAgainstKnownRawData - Needing Work
"""

import unittest
import copy
import pandas as pd
from pandas.util.testing import assert_series_equal, assert_frame_equal
import numpy as np
from numpy.testing import assert_allclose

from pyeconlab.util import package_folder, expand_homepath, check_directory
from pyeconlab.util import find_row, assert_rows_in_df, assert_unique_rows_in_df, assert_merged_series_items_equal
from ..constructor import NBERFeenstraWTFConstructor

#-DATA Paths-#
SOURCE_DATA_DIR = check_directory("E:\\work-data\\x_datasets\\36a376e5a01385782112519bddfac85e\\") 			#Win7!
TEST_DATA_DIR = package_folder(__file__, "data") 

#-Support Functions-#

def import_csv_as_statatypes(fl):
	"""
	Import CSV Files so that dtype is the same as Stata Files 
	"""
	#-Import Types to Match Stata Import Types-#
	import_types = {
						'year' 		: int,
						'icode'		: object,
						'importer' 	: object,
						'ecode'     : object,
						'exporter'  : object,
						'sitc4'     : object,
						'unit'      : object,
						'dot'       : float,
						'value'     : int,
						'quantity'  : float,
					}
	return pd.read_csv(fl, dtype=import_types)


def import_excel_as_statatypes(fl):
	
	# NOTE: THIS DOESN"T WORK (https://github.com/pydata/pandas/issues/5891)
	# Currently Avoid Excel when constructing test datasets and stick with csv. 

	import_types = {
						'year' 		: int,
						'icode'		: object,
						'importer' 	: object,
						'ecode'     : object,
						'exporter'  : object,
						'sitc4'     : object,
						'unit'      : object,
						'dot'       : float,
						'value'     : int,
						'quantity'  : float,
					}
	return pd.read_excel(fl, dtype=import_types)

def convert_import_excel_to_statatypes(fl):
	""" Convert Excel Imports to Stata Types """
	import_types = {
						'year' 		: int,
						'icode'		: str,
						'importer' 	: str,
						'ecode'     : str,
						'exporter'  : str,
						'sitc4'     : str,
						'unit'      : str,
						'dot'       : float,
						'value'     : int,
						'quantity'  : float,
					}

	data = pd.read_excel(fl)
	for item in import_types.keys():
		try:
			if import_types[item] == str:
				data[item] = data[item].apply(lambda x: str(x))
			elif import_types[item] == int:
				data[item] = data[item].apply(lambda x: int(x))
			elif import_types[item] == float:
				data[item] = data[item].apply(lambda x: float(x))
		except:
			pass 																#This may except if trying to import items not in the file like ('unit' or 'quantity' etc.)
	return data


# - Test Suites - #

class TestSmallSampleDataset(unittest.TestCase):
	""" 
	Tests NBERFeenstraWTFConstructor from a Small Sample Dataset

	Tests:
	-----
		[1] Attributes (Exporters, Importers)
		[2] Standardisation of Data
	"""

	def setUp(self):
		"""
		Import and Setup Class from a Small Known Dataset
		File: 'data/nberfeenstra_wtf62_random_sample.csv' (md5hash: da092cc4b8053083d53c5dc5b72df79d)
		Dependancies: import_csv_as_statatypes()
		"""
		#-Import Test Data-#
		self.df = import_csv_as_statatypes(TEST_DATA_DIR+"nberfeenstra_wtf62_random_sample.csv")
		self.obj = NBERFeenstraWTFConstructor(source_dir=TEST_DATA_DIR, skip_setup=True)
		self.obj.set_raw_data(df=self.df, force=True)
		
		#-Known Solutions-#
		self.exp = set(['Canada', 'Cyprus', 'Jamaica', 'Poland', 'Spain', 'Taiwan', 'UK', 'World'])
		self.imp = set(['Cambodia','Canada','Czechoslovak', 'Ghana', 'Kenya', 'Korea Rep.','Mexico','Morocco', 'Switz.Liecht', 'USA'])

	def test_exporters(self):
		""" Test Exporters Property """
		exp = self.exp
		obj = self.obj
		computed_exp = set(obj.exporters)
		assert exp == computed_exp, "Different Elements in the Set of Exporters: %s" % (exp.difference(computed_exp))
		
	def test_importers(self):
		""" Test Importers Property """
		imp = self.imp
		obj = self.obj
		computed_imp = set(obj.importers)
		assert imp == computed_imp, "Different Elements in the Set of Importers: %s" % (imp.difference(computed_imp))
		
	def test_standardisation(self):
		""" Test Standardisation method """
		df = self.df
		obj = self.obj
		obj.set_dataset(df=copy.deepcopy(df[df.columns[0:10]])) 	#Remove Obs Columns
		obj.standardise_data()
		df2 = obj.dataset
		#-Importer Codes-#
		assert_series_equal(df2['iregion'], pd.Series(['13', '55', '21', '58', '16', '33', '45', '21', '45', '16'], name='iregion')) 				#Note: This is Order Specific!
		assert_series_equal(df2['iiso3n'], pd.Series(['504', '756', '840', '200', '288', '484', '116', '124', '410', '404'], name='iiso3n'))
		#-Exporter Codes-#
		assert_series_equal(df2['eregion'], pd.Series(['58', '44', '35', '10', '53', '21', '53', '53', '45', '53'], name='eregion'))
		assert_series_equal(df2['eiso3n'], pd.Series(['616', '196', '388', '000', '826', '124', '826', '724', '896', '826'], name='eiso3n'))


class TestConstructorAgainstKnownRawDataFromDTA(unittest.TestCase):
	"""
		Test the Constructor against random known data points.
		Note: These Tests are based on importing from RAW DTA files

		File
		----
		'data/nberfeenstra_wtf62_random_sample.csv' (md5hash: da092cc4b8053083d53c5dc5b72df79d)
		'data/nberfeenstra_wtf85_random_sample.csv' (mdfhash: e1a7d3b651d5df837e0ece12c288c8a5)
		'data/nberfeenstra_wtf90_random_sample.csv' (md5hash: 639c9be4e67126d64bd23a3285cf2484)
		'data/nberfeenstra_wtf00_random_sample.csv' (md5hash: 84305bef20043a78bd01badfeea10162)
		'data/nberfeenstra_wtf00_random_sample2.csv' (md5hash: 47bcd5a3de86cdd9c6f2ef47d9243fbd)

		Years
		-----
		Conducting tests on 4-Year CrossSections [1962, 1985, 1990, 2000] 
	"""

	#-SetUp-#

	@classmethod
	def setUpClass(cls): #should this be cls
		""" Setup NBERFeenstraWTFConstructor using: source_dir """
		years = [1962, 1985, 1990, 2000]
		cls.obj = NBERFeenstraWTFConstructor(source_dir=SOURCE_DATA_DIR, years=years, ftype='hdf', standardise=False, skip_setup=False, verbose=False) #hdf is faster, dta from dta files
		#-Adjust Units as Stata Imports '' as '' whereas csv imports '' as np.nan-#
		cls.obj.raw_data['unit'] = cls.obj.raw_data['unit'].apply(lambda x: np.nan if x == '' else x)

	#-Basic Tests-#

	def test_years(self):
		""" Test setUpClass has imported the correct years"""
		obj = self.obj 
		yrs = obj.raw_data['year'].unique()
		assert set(yrs) == set(self.obj.years)

	def test_1962(self):
		""" 
		Tests for 1962 Data
		-----
		[1] number of observations
		[2] number of unique icountries and ecountries
		[3] number of unique sitc4 products

		Notes:
		-----
		[1] These Test Values have been found using Stata
		"""
		df = self.obj.raw_data[self.obj.raw_data['year'] == 1962]
		#-Number of Observations-#
		assert df.shape[0] == 470438, "Total Number of observations: %s != 470438" % (df.shape[0])
		num_obs_117100 = len(df[df['icode'] == '117100'])
		#-Number of 117100 observations-#
		assert  num_obs_117100 == 4496, "number of icode: 117100 observations (%s) != 4496" % (num_obs_117100)
		#- Number of Fiji observations-#
		num_obs_Fiji = len(df[df['importer'] == 'Fiji'])
		assert num_obs_Fiji == 972, "number of 'Fiji' observations (%s) != 972" % (num_obs_Fiji)
		num_obs_0023 = len(df[df['sitc4'] == '0023'])
		assert num_obs_0023 == 15, "number of SITC4: 0023 observations (%s) != 15" % (num_obs_0023)
		num_sitc4_codes = len(df['sitc4'].unique())
		assert num_sitc4_codes == 696, "number of SITC4 codes (%s) != 696" % (num_sitc4_codes)

	def test_random_sample_1962(self):
		""" 
		Test a Random Sample from 1962
		File: 'data/nberfeenstra_wtf62_random_sample.csv' (md5hash: da092cc4b8053083d53c5dc5b72df79d)
		Dependancies: import_csv_as_statatypes()
		"""
		#-Load Random Sample From RAW DATASET-#
		years = [1962]
		obj = self.obj
		rs = import_csv_as_statatypes(TEST_DATA_DIR+"nberfeenstra_wtf62_random_sample.csv") 		#random sample
		del rs['obs']
		assert_rows_in_df(df=self.obj.raw_data, rows=rs)
		assert_unique_rows_in_df(df=self.obj.raw_data, rows=rs)


	def test_1985(self):
		"""
		Tests for 1985 Data
		-----
		[1] number of observations
		[2] number of unique icountries and ecountries
		[3] number of unique sitc4 products	

		Notes:
		-----
		[1] These Test Values have been found using Stata
		"""
		df = self.obj.raw_data[self.obj.raw_data['year'] == 1985]
		#-Number of Observations-#
		assert df.shape[0] == 509175, "Total Number of observations: %s != 509175" % (df.shape[0])
		num_obs_117100 = len(df[df['icode'] == '117100'])
		#-Number of 117100 observations-#
		assert  num_obs_117100 == 4666, "number of icode: 117100 observations (%s) != 4666" % (num_obs_117100)
		#- Number of Fiji observations-#
		num_obs_Fiji = len(df[df['importer'] == 'Fiji'])
		assert num_obs_Fiji == 783, "number of 'Fiji' observations (%s) != 783" % (num_obs_Fiji)
		#-Test Some Product Codes-#
		num_obs_0023 = len(df[df['sitc4'] == '0023'])
		assert num_obs_0023 == 0, "number of SITC4: 0023 observations (%s) != 0" % (num_obs_0023)
		num_obs_0013 = len(df[df['sitc4'] == '0013'])
		assert num_obs_0013 == 145, "number of SITC4: 0013 observations (%s) != 145" % (num_obs_0013)
		num_sitc4_codes = len(df['sitc4'].unique())
		assert num_sitc4_codes == 1438, "number of SITC4 codes (%s) != 1438" % (num_sitc4_codes)
	
	def test_random_sample_1985(self):
		""" 
		Test a Random Sample from 1985
		File: 'data/nberfeenstra_wtf85_random_sample.csv' (md5hash: ???)
		Dependancies: import_csv_as_statatypes()
		"""
		#-Load Random Sample From RAW DATASET-#
		years = [1985]
		rs = import_csv_as_statatypes(TEST_DATA_DIR+"nberfeenstra_wtf85_random_sample.csv") 		#random sample
		del rs['obs']
		assert_rows_in_df(df=self.obj.raw_data, rows=rs)
		assert_unique_rows_in_df(df=self.obj.raw_data, rows=rs)

	def test_1990(self):
		"""
		Tests for 1990 Data
		-----
		[1] number of observations
		[2] number of unique icountries and ecountries
		[3] number of unique sitc4 products

		Notes:
		-----
		[1] These Test Values have been found using Stata		
		"""
		df = self.obj.raw_data[self.obj.raw_data['year'] == 1990]
		#-Number of Observations-#
		assert df.shape[0] == 662705, "Total Number of observations: %s != 662705" % (df.shape[0])
		num_obs_117100 = len(df[df['icode'] == '117100'])
		#-Number of 117100 observations-#
		assert  num_obs_117100 == 7354, "number of icode: 117100 observations (%s) != 7354" % (num_obs_117100)
		#- Number of Fiji observations-#
		num_obs_Fiji = len(df[df['importer'] == 'Fiji'])
		assert num_obs_Fiji == 1182, "number of 'Fiji' observations (%s) != 1182" % (num_obs_Fiji)
		#-Test Some Product Codes-#
		num_obs_0023 = len(df[df['sitc4'] == '0023'])
		assert num_obs_0023 == 0, "number of SITC4: 0023 observations (%s) != 0" % (num_obs_0023)
		num_obs_0013 = len(df[df['sitc4'] == '0013'])
		assert num_obs_0013 == 190, "number of SITC4: 0013 observations (%s) != 190" % (num_obs_0013)
		num_sitc4_codes = len(df['sitc4'].unique())
		assert num_sitc4_codes == 1422, "number of SITC4 codes (%s) != 1422" % (num_sitc4_codes) 
	
	def test_random_sample_1990(self):
		""" 
		Test a Random Sample from 1990
		File: 'data/nberfeenstra_wtf90_random_sample.csv' (md5hash: ???)
		Dependancies: import_csv_as_statatypes()
		"""
		#-Load Random Sample From RAW DATASET-#
		years = [1990]
		rs = import_csv_as_statatypes(TEST_DATA_DIR+"nberfeenstra_wtf90_random_sample.csv") 		#random sample
		del rs['obs']
		assert_rows_in_df(df=self.obj.raw_data, rows=rs)
		assert_unique_rows_in_df(df=self.obj.raw_data, rows=rs)

	def test_2000(self):
		"""
		Tests for 2000 Data
		-----
		[1] number of observations
		[2] number of unique icountries and ecountries
		[3] number of unique sitc4 products

		Notes:
		-----
		[1] These Test Values have been found using Stata		
		"""
		df = self.obj.raw_data[self.obj.raw_data['year'] == 2000]
		#-Number of Observations-#
		assert df.shape[0] == 857189, "Total Number of observations: %s != 857189" % (df.shape[0])
		num_obs_117100 = len(df[df['icode'] == '117100'])
		#-Number of 117100 observations-#
		assert  num_obs_117100 == 8732, "number of icode: 117100 observations (%s) != 8732" % (num_obs_117100)
		#- Number of Fiji observations-#
		num_obs_Fiji = len(df[df['importer'] == 'Fiji'])
		assert num_obs_Fiji == 1088, "number of 'Fiji' observations (%s) != 1088" % (num_obs_Fiji)
		#-Test Some Product Codes-#
		num_obs_0023 = len(df[df['sitc4'] == '0023'])
		assert num_obs_0023 == 0, "number of SITC4: 0023 observations (%s) != 0" % (num_obs_0023)
		num_obs_0013 = len(df[df['sitc4'] == '0013'])
		assert num_obs_0013 == 201, "number of SITC4: 0013 observations (%s) != 201" % (num_obs_0013)
		num_sitc4_codes = len(df['sitc4'].unique())
		assert num_sitc4_codes == 1288, "number of SITC4 codes (%s) != 1288" % (num_sitc4_codes) 
	
	def test_random_sample_2000(self):
		""" 
		Test a Random Sample from 2000
		File: 'data/nberfeenstra_wtf00_random_sample.csv' (md5hash: ???)
		Dependancies: import_csv_as_statatypes()
		"""
		#-Load Random Sample From RAW DATASET-#
		years = [2000]
		rs = import_csv_as_statatypes(TEST_DATA_DIR+"nberfeenstra_wtf00_random_sample.csv") 		#random sample
		del rs['obs']
		assert_rows_in_df(df=self.obj.raw_data, rows=rs)
		assert_unique_rows_in_df(df=self.obj.raw_data, rows=rs)


	def test_standardise_data(self):
		""" Test Standardisation of Data Method """
		assert False, "Write Test"

	def test_china_hongkongdata(self):
		""" Test Import of China HongKong Adjustment Data """
		assert False, "Write Test"

	def test_adjust_china_hongkongdata(self):
		""" Test Adjustment of China & HongKong Data """
		assert False, "Write Test"

	def test_collapse_to_valuesonly_1990(self):
		""" Test the Collapse to Export Values Only (collapse_to_valuesonly()) Against Some Random Test Cases in Year 1990 """
		obj = self.obj
		obj.set_dataset(obj.raw_data)
		obj.collapse_to_valuesonly()
		rs = convert_import_excel_to_statatypes(TEST_DATA_DIR+'testdata_collapse_to_valuesonly_1_result.xlsx')
		assert_rows_in_df(df=obj.dataset, rows=rs)
		assert_unique_rows_in_df(df=obj.dataset, rows=rs)

	def test_bilateral_flows(self):
		""" Test Import of Bilateral Flows to Supp Data """
		assert False, "Write Test"

	### - Global Dataset Tests - ###

	def test_generate_global_info(self):
		""" Test Global Info Method """
		assert False, "Write Test"

	#-TearDown-#

	@classmethod
	def tearDownClass(self):
		""" Delete Large Memory Objects """
		pass


### --- Full Dataset Tests --- ###

class TestConstructorAgainstKnownSolutionsAllYears():
	"""
		Test the Constructor against random known data points.
		Note: These tests are based on importing from the 'hdf'
	"""

	#-SetUp-#

	@classmethod
	def setUpClass(cls): #should this be cls
		""" Setup NBERFeenstraWTFConstructor using: source_dir """
		cls.obj = NBERFeenstraWTFConstructor(source_dir=SOURCE_DATA_DIR)

	def setUp(self):
		""" Reset Dataset to Raw Data Before each tests """
		self.obj.reset_dataset()

	def test_world_exports(self):
		""" Test World Exports """
		sol = pd.read_csv(TEST_DATA_DIR + 'stata_wtf62-00_WLD_total_export.csv', index_col=['year'])['value']
		df = self.obj.dataset
		vs = df.loc[(df.exporter == 'World') & (df.importer=='World')].groupby(['year']).sum()['value']
		for year in self.obj.years:
			assert vs[year] == sol[year], "Year (%s) Totals (stata: %s != %s) do not match Stata Derived Tests Data" % (year, sol[year], vs[year])

	def test_total_cntry_exports(self):
		"""
		Test Total Exports from a selection of countries
		"""
		from pyeconlab.trade.dataset.NBERFeenstraWTF import iso3c_to_countryname
		self.obj.drop_world_observations()
		df = self.obj.dataset
		for cntry in ['GBR', 'ISR', 'TWN', 'USA']: 																	#'CHE'
			sol = pd.read_csv(TEST_DATA_DIR + 'stata_wtf62-00_%s_total_export.csv' % cntry, index_col=['year'])['value']
			vs = df.loc[(df.exporter == iso3c_to_countryname[cntry]) & (df.importer != 'World')].groupby(['year']).sum()['value']
			for year in self.obj.years:
				assert vs[year] == sol[year], "Cntry (%s) Year (%s) Totals (stata: %s != %s) do not match Stata Derived Tests Data" % (cntry, year, sol[year], vs[year])

	def test_product_exports(self):
		""" 
		Test Product Exports from a Selection of Countries
		"""
		from pyeconlab.trade.dataset.NBERFeenstraWTF import iso3c_to_countryname
		self.obj.drop_world_observations()
		df = self.obj.dataset
		for prod in ['6540', '6517', '7431', '8924', '0421']: 												
			sol = pd.read_csv(TEST_DATA_DIR + 'stata_wtf62-00_sitc4_%s_total.csv' % prod, index_col=['year'])['value']
			vs = df.loc[(df.sitc4 == prod)].groupby(['year']).sum()['value']
			for year in self.obj.years:
				assert vs[year] == sol[year], "Product (%s) Year (%s) Totals (stata: %s != %s) do not match Stata Derived Tests Data" % (prod, year, sol[year], vs[year])

	def test_country_product_exports(self):
		""" 
		Test Country x Product Exports from a Selection of Countries
		"""
		from pyeconlab.trade.dataset.NBERFeenstraWTF import iso3c_to_countryname
		self.obj.drop_world_observations()
		df = self.obj.dataset
		for cntry, prod in [('ESP', '8973'), ('DNK', '0620')]: 												
			sol = pd.read_csv(TEST_DATA_DIR + 'stata_wtf62-00_%s_sitc4_%s_total.csv' % (cntry, prod), index_col=['year'])['value']
			vs = df.loc[(df.exporter == iso3c_to_countryname[cntry]) & (df.sitc4 == prod)].groupby(['year']).sum()['value']
			for year in self.obj.years:
				assert vs[year] == sol[year], "Country (%s) Product (%s) Year (%s) Totals (stata: %s != %s) do not match Stata Derived Tests Data" % (cntry, prod, year, sol[year], vs[year])

	def test_collapse_to_valuesonly(self):
		""" Test the Collapse to Export Values Only (collapse_to_valuesonly()) Against Some Random Test Cases Across ALL Years """
		rs = convert_import_excel_to_statatypes(TEST_DATA_DIR+'testdata_collapse_to_valuesonly_2_result.xlsx')
		obj = self.obj
		obj.collapse_to_valuesonly()
		assert_rows_in_df(df=obj.dataset, rows=rs)
		assert_unique_rows_in_df(df=obj.dataset, rows=rs)

	def test_collapse_to_valuesonly_2(self):
		""" Test Collapse to values only - before and after sum of exporter and importer values should be the same """
		obj = self.obj
		#-Exporter-#
		s1 = obj.exporter_total_values(self.dataset)
		obj.collapse_to_valuesonly(subidx=['year', 'icode', 'ecode', 'sitc4'], verbose=verbose) 			#This will remove unit, quantity, dot, exporter and importer
		s2 = obj.exporter_total_values(self.dataset)
		assert_merged_series_items_equal(s1,s2)
		#-Importer-#
		s1 = obj.importer_total_values(self.dataset)
		obj.collapse_to_valuesonly(subidx=['year', 'icode', 'ecode', 'sitc4'], verbose=verbose) 			#This will remove unit, quantity, dot, exporter and importer
		s2 = obj.importer_total_values(self.dataset)
		assert_merged_series_items_equal(s1,s2)

	def test_collapse_to_valuesonly_3(self):
		""" Test collapse_to_valuesonly() for adaptable indexing """
		obj = self.obj 
		#-Route1-#
		obj.collapse_to_valuesonly(verbose=True)						#Main Round of Reductions
		obj.reduce_to(['year', 'icode', 'ecode', 'sitc4', 'value'])
		obj.collapse_to_valuesonly(verbose=True) 						#Extra Reductions Due to Yemen Names
		l1 = obj.dataset.shape[0]
		#-Route2-#
		obj.reset_dataset()
		obj.reduce_to(['year', 'icode', 'ecode', 'sitc4', 'value'])
		obj.collapse_to_valuesonly(verbose=True)
		l2 = obj.dataset.shape[0]
		assert l1 == l2, "The Number of Observations differ (%s != %s) When they should be the same" % (l1, l2)

TestConstructorAgainstKnownSolutionsAllYears.slow = True

