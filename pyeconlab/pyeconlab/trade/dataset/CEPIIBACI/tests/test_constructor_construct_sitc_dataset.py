"""
Test construct_sitc_dataset() method against Stata

Additional Files
---------------
../do/construct_sitc_dataset.do

"""

import os
import gc
import pandas as pd
from pyeconlab import BACIConstructor
from numpy.testing import assert_allclose

TEST_DATA_DIR = os.path.expanduser("~/work-data/repos-pyeconlab-testdata/")
SOURCE_DATA_DIR = os.path.expanduser("~/work-data/datasets/e988b6544563675492b59f397a8cb6bb/")

class TestConstructSITCDatasetAgainstStataData():
    """
    Test Suite for Comparing Data with STATA script
    Stata: ../do/basic_sitc_data.do
    """

    @classmethod
    def setUpClass(cls):
        cls.obj = BACIConstructor(source_dir=SOURCE_DATA_DIR, source_classification="HS96")

    def setUp(self):
        self.obj.reset_dataset() #-Reset to RAW Data after each test-#
        self.obj.complete_dataset=True  
        self.obj.classification="HS96"  #Reset from SITC Conversion Process
        gc.collect() 

    #-Dataset A-#

    def test_bilateral_data_A(self):
        #-pyeconlab-#
        self.obj.construct_sitc_dataset(data_type='trade', dataset="A", product_level=3, sitc_revision=2, report=False, verbose=False)
        #-stata-#
        self.A = pd.read_stata(TEST_DATA_DIR + "bacihs96_stata_trade_sitcr2l3_1998to2012_A.dta")
        self.A.sort(['year', 'eiso3c', 'iiso3c', 'sitc3'], inplace=True)
        self.A.reset_index(inplace=True)
        del self.A['index']
        try:
            assert_frame_equal(self.obj.dataset, self.A)
        except:
            assert_allclose(self.obj.dataset['value'].values, self.A['value'].values)
        del self.A

    def test_export_data_A(self):
        #-pyeconlab-#
        self.obj.construct_sitc_dataset(data_type='export', dataset="A", product_level=3, sitc_revision=2, report=False, verbose=False)
        #-stata-#
        self.A = pd.read_stata(TEST_DATA_DIR + "bacihs96_stata_export_sitcr2l3_1998to2012_A.dta")
        self.A.sort(['year', 'eiso3c', 'sitc3'], inplace=True)
        self.A.reset_index(inplace=True)
        del self.A['index']
        assert_allclose(self.obj.dataset['value'].values, self.A['value'].values)
        del self.A

    def test_import_data_A(self):                                                                                  
        #-pyeconlab-#
        self.obj.construct_sitc_dataset(data_type='import', dataset="A", product_level=3, sitc_revision=2, report=False, verbose=False)
        #-stata-#
        self.A = pd.read_stata(TEST_DATA_DIR + "bacihs96_stata_import_sitcr2l3_1998to2012_A.dta")
        self.A.sort(['year', 'iiso3c', 'sitc3'], inplace=True)
        self.A.reset_index(inplace=True)
        del self.A['index']
        assert_allclose(self.obj.dataset['value'].values, self.A['value'].values)
        del self.A
