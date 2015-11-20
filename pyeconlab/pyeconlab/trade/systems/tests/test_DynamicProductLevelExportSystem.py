"""
Tests for DynamicProductLevelExportSystem Module
"""

import pandas as pd
from pyeconlab.util import package_folder
from pyeconlab.trade.systems import DynamicProductLevelExportSystem

from pandas.util.testing import assert_frame_equal

### --- Options --- ###
verbose = False

#-DATA Paths-#
TEST_DATA_DIR = package_folder(__file__, "data") 

class TestDynamicProductLevelExportSystemBasic(object):
	"""
	Basic Tests for Dynamic Product Level Export System
	"""

	odata = pd.DataFrame([	[2000,"AUS","0001",200], 
							[2000,"AUS","0002",100],
							[2000,"USA","0001",400],
							[2000,"USA","0003",300],
							[2000,"AFG","0004",50],
							[2001,"AFG","0004",25],
							[2001,"AUS","0005",30],
							[2001,"AUS","0001",100],
							[2001,"USA","0004",200],
							[2001,"USA","0006",500],
							[2001,"USA","0007",900] ], columns=['year','country','productcode','export'])

	odata_yeardict = { 	2000 : 	pd.DataFrame( [	["AUS","0001",200], 
												["AUS","0002",100],
												["USA","0001",400],
												["USA","0003",300],
												["AFG","0004",50]
											   ], columns=['country','productcode','export']
											),
						2001 : 	pd.DataFrame( [	["AFG","0004",25],
												["AUS","0005",30],
												["AUS","0001",100],
												["USA","0004",200],
												["USA","0006",500],
												["USA","0007",900] 
											   ], columns=['country','productcode','export']
											)
					}

	@classmethod
	def setUpClass(cls):
		cls.fn = TEST_DATA_DIR + 'export-testdata.csv'
	
	def test_simple_data(self):
		"""
		Test Simple Networks
		"""
		A = DynamicProductLevelExportSystem()
		A.from_csv(self.fn, dtypes=['DataFrame', 'BiPartiteGraph'], verbose=True) 	# - Funtions Tested: from_csv, from_df
		for year in [2000, 2001]:
			assert_frame_equal(self.odata_yeardict[year], A[year].data.reset_index())
		
		# print "Object A[2000] Network Nodes:" 		# These features will be migrated to DynamicProductLevelExportNetwork Class
		# print A[2000].network.nodes()
		# print "Object A[2000] Network Edges:"
		# print A[2000].network.edges()

	#-WORKING HERE-#
	#-Itegrate these into the test-#

	# ################
	# #Test Functions#
	# ################

	# print "Test .data property"
	# print A.data
	# print "Test get_data() Extended method"
	# print "Default Settings"
	# print A.get_data()
	# print "Specific Year Data"
	# print A.get_data(year=2000)
	# print "Long Panel of Data"
	# print A.get_data(rtype='long')
	# print "Wide Panel of Data"
	# print A.get_data(rtype='wide')
	# print "Panel of Data"
	# print A.get_data(rtype='panel')

	# print "Test as_cp_matrices()"
	# A.as_cp_matrices(verbose=True)
	# print A[2000].cp_matrix
	# print A[2001].cp_matrix

	# print "Testing .cp_matrix Property"
	# print A.cp_matrix

	# print "Testing rca_matrices()"
	# A.complete_trade_network = True
	# A.rca_matrices(verbose=True)
	# print A[2000].rca
	# print A[2001].rca
	# print "Testing .rca Property"
	# print A.rca

	# print "Testing Mcp_matrices()"
	# A.mcp_matrices(fillna=True, verbose=True)
	# print A[2000].mcp
	# print A[2001].mcp
	# print "Testing .mcp Property"
	# print A.mcp

	# print "Testing proximity_matrices()"
	# A.proximity_matrices(verbose=True)
	# print A[2000].proximity
	# print A[2001].proximity
	# print "Testing .proximity Property"
	# print A.proximity

	# print "Testing proximity_matrices(matrix_type='asymmetric')"
	# A.proximity_matrices(matrix_type='asymmetric', verbose=True)
	# print A[2000].proximity
	# print A[2001].proximity
	# print "Testing .proximity Property"
	# print A.proximity

	# print "Testing Product Ubiquity Method"
	# A.compute_ubiquity(verbose=True)
	# print A[2000].ubiquity
	# print A[2001].ubiquity
	# print "Testing .ubiquity Property"
	# print A.ubiquity

	# print "Testing Country Diversity Method"
	# A.compute_diversity(verbose=True)
	# print A[2000].diversity
	# print A[2001].diversity
	# print "Testing .diversity Property"
	# print A.diversity

	# print "--------"
	# print "TESTING COMPLETED SUCCESFULLY!"
	# print "--------"

	# sys.exit()

	# #################################################
	# ## Alternative Ways of Working with the Object ##
	# #################################################

	# print "\nObject B:"
	# B = DynProductLevelExportSystem()
	# # - Import Test Data - #
	# B.from_csv(data, dtypes=['DataFrame'], verbose=True)
	# B.construct_bipartite(verbose=True)
	# print B[2000].network.nodes()

	# if graphics: B[2000].draw_network()


	# ####################################
	# ### --- Test Object Networks --- ###
	# ####################################
	# # - Country Data from WDI - #
	# W = wdi.WDI('WDI_Data.csv',source_ds='d1352f394ef8e7519797214f52ccd7cc', hash_file_sep=r' ', verbose=True)
	# C = Countries() 																								#Investigate a Pickle Here
	# C.build_from_wdi(W)

	# A = DynProductLevelExportSystem()
	# # - Import Test Data - #
	# # - Funtions Tested: from_csv, from_df
	# A.from_csv(data, cntry_obj=C, dtypes=['DataFrame', 'BiPartiteGraph'], verbose=True)
	# print "Object A:"
	# print A
	# print "Object A.data => Should be the Same due to __repr__"
	# print A.ples
	# print "Object A[2000] Data:"
	# print A[2000].data
	# print "Object A[2000] Network Nodes:"
	# print A[2000].network.nodes()
	# print "Object A[2000] Network Edges:"
	# print A[2000].network.edges()

	# ################
	# #Test Functions#
	# ################

	# if graphics: 
	# 	print "\nDrawing Network"
	# 	A[2000].draw_network()

	# print "\nTest to_cp_matrices()"
	# A.as_cp_matrices(verbose=True)
	# print A[2000].cp_matrix
	# print A[2001].cp_matrix

	# print "\nTesting cp_matrices() Getter Method"
	# print A.cp_matrices()