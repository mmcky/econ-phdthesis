"""
Test CSV and HDF File Construction to ensure they are consistent

STATUS: IN-WORK

"""

from pandas.util.testing import assert_frame_equal

from pyeconlab.trade import BACIConstructor

SOURCE_DIR = r"C:\Users\Matt-Work\work\testing\baci_test"

class TestRawDataToHDF(object):
	"""
	Build HDF Files from CSV and Test that they are equivalent
	"""

	# @classmethod
	# def setUpClass(cls):
	# 	""" Using HS02 {Not Required but good to be explicit as class will autogenerate} """
	# 	cls.obj = BACIConstructor(source_dir=SOURCE_DIR, src_class="HS02", ftype='csv', verbose=True)
	# 	cls.obj.convert_raw_data_to_hdf()																#Should this generate test files? hdf_fn='test_raw_hdf.h5'
	# 	cls.obj.convert_raw_data_to_hdf_yearindex() 													#hdf_fn='test_raw_hdf_yearindex.h5'

	def test_raw_data_to_hdf(self):
		""" test_raw_data_to_hdf method """
		self.a = BACIConstructor(source_dir=SOURCE_DIR, src_class="HS02", ftype='csv', verbose=True)
		self.a.convert_raw_data_to_hdf(verbose=True) 	
		self.b = BACIConstructor(source_dir=SOURCE_DIR, src_class="HS02", ftype='hdf', verbose=True)
		assert_frame_equal(self.a.dataset,self.b.dataset)

	def test_raw_data_to_hdf_yearindex(self):
		""" test_raw_data_to_hdf method """
		self.a = BACIConstructor(source_dir=SOURCE_DIR, src_class="HS02", years=[2003,2004], ftype='csv', verbose=True)
		self.a.convert_raw_data_to_hdf_yearindex(verbose=True) 	
		self.b = BACIConstructor(source_dir=SOURCE_DIR, src_class="HS02", years=[2003,2004], ftype='hdf', verbose=True)
		assert_frame_equal(self.a.dataset,self.b.dataset)

	# def tearDown(self):
	# 	del self.a 
	# 	del self.b

	# @classmethod
	# def tearDownClass(cls):
	# 	"""
	# 	Remove Generated Test Files if named differently to defaults
	# 	"""
	# 	pass

class TestCSVtoHDF(object):
	"""
	Test convert_csv_to_hdf_yearindex
	"""

	def test_convert_csv_to_hdf_yearindex(self):
		a = BACIConstructor(source_dir=SOURCE_DIR, src_class="HS02", skip_setup=True, verbose=True)
		a.convert_csv_to_hdf_yearindex(years=[2005,2006]) 																#hdf_fn='test_csv_to_hdf_yearindex.h5'
		b = BACIConstructor(source_dir=SOURCE_DIR, src_class="HS02", years=[2005,2006], verbose=True)


	# @classmethod
	# def tearDownClass(cls):
	# 	"""
	# 	Remove Generated Test Files if named differently to defaults
	# 	"""
	# 	pass