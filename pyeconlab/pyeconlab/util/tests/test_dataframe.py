"""
Tests for DataFrame Utilities
"""

import unittest
import pandas as pd
import numpy as np

from pandas.util.testing import assert_frame_equal, assert_series_equal
from pyeconlab.util import merge_columns


class TestSuite_merge_columns(unittest.TestCase):
	"""
	Test Suite for merge_columns()
	"""

	columns = ['iso3c', 'sitc4', 'year', 'value', 'quantity']

	data1 = [
			['AFG', '0011', 1970, np.nan, np.nan], 	\
			['ZWE', '0012', 1970, 300, np.nan], 	\
			['USA', '0011', 1970, 500, 5], 			\
			['USA', '0012', 1970, 1000, 10]			\
			]

	data2 = [
			['AFG', '0011', 1970, 300, np.nan], 	\
			['ZWE', '0012', 1970, np.nan, 1], 		\
			['USA', '0011', 1970, 750, 2], 			\
			['USA', '0013', 1970, 5000, np.nan]		\
			]

	a = pd.DataFrame(data1, columns=columns)
	b = pd.DataFrame(data2, columns=columns)

	def test_merge_columns1(self, a=a, b=b, columns=columns):
		"""
		Simple Test Cases of merge_columns
		"""	

		# Solution: Right Dominant #

		# - Settings - #
		on 					= list(a.columns[0:3])
		collapse_columns 	= ('value_x', 'value_y', 'value')
		dominant 			= 'right'	

		# - Diagnostic Return - #

		sol1_columns = ['iso3c', 'sitc4', 'year', 'value_x', 'quantity_x', 'value_y', 'quantity_y', 'value']

		sol1 =  	[
					['AFG', '0011', 1970, np.nan, 	np.nan, 	300, 		np.nan, 	300], 	\
					['ZWE', '0012', 1970, 300, 		np.nan, 	np.nan,		1, 			300],	\
					['USA', '0011', 1970, 500, 		5,			750, 		2, 			750], 	\
					['USA', '0012', 1970, 1000, 	10, 		np.nan, 	np.nan, 	1000],	\
					['USA', '0013', 1970, np.nan, 	np.nan, 	5000, 		np.nan, 	5000],	\
					]
		R1 = pd.DataFrame(sol1, columns=sol1_columns)

		# Usual Return #

		sol2 =  	[
					['AFG', '0011', 1970, 300, np.nan], 	\
					['ZWE', '0012', 1970, 300, np.nan],		\
					['USA', '0011', 1970, 750, 5], 			\
					['USA', '0012', 1970, 1000, 10],		\
					['USA', '0013', 1970, 5000, np.nan]		\
					]

		R2 = pd.DataFrame(sol2, columns=columns)

		computed = merge_columns(a,b, on=on, collapse_columns=collapse_columns, dominant=dominant, verbose=False)

		assert_series_equal(computed['value'], R1['value'], check_dtype=False)


#-Should these be at the top of the file OR near the use-#

from pyeconlab.util import compute_number_of_spells, compute_spell_lengths

class TestIntertemporalFunctions(unittest.TestCase):
	"""
	Test Intertemporal DataFrame Utilities

	compute_number_of_spells
	compute_spell_lengths

	"""

	data = 	[ 
				[4, 4, 4, 4], 						#Unique Case
				[32, 32, 45, 45], 					#Two with Positive Increment
				[1, 2, 3, 4], 						#All Different
				[66, 66, 20, 20] 					#Two with Negative Increment
			]
	data = pd.DataFrame(data, index=['C1', 'C2', 'C3', 'C4'], columns=['Y1', 'Y2', 'Y3', 'Y4'])

	def test_compute_number_of_spells_simple(self):
		"""Test compute_number_of_spells """
		sol = [ 
				[1, 1, 1, 1],
				[1, 1, 2, 2],
				[1, 2, 3, 4],
				[1, 1, 2, 2]
			  ]	
		sol = pd.DataFrame(sol, index=['C1', 'C2', 'C3', 'C4'], columns=['Y1', 'Y2', 'Y3', 'Y4'])
		comp = compute_number_of_spells(self.data)
		assert_frame_equal(comp, sol)

	def test_compute_number_of_spells_wnan(self):
		"""Test compute_number_of_spells with np.nan"""
		data = self.data.copy(deep=True) 
		data['Y2'] = np.nan
		sol = [ 
				[1, np.nan, 1, 1], 		#Is this the right behaviour? 1 Isn't a uniquely different item but it also isn't continuous!
				[1, np.nan, 2, 2],
				[1, np.nan, 2, 3],
				[1, np.nan, 2, 2]
			  ]	
		sol = pd.DataFrame(sol, index=['C1', 'C2', 'C3', 'C4'], columns=['Y1', 'Y2', 'Y3', 'Y4'])
		comp = compute_number_of_spells(data)
		assert_frame_equal(comp, sol, check_dtype=False)


	def test_compute_spell_lengths_simple(self):
		""" Test compute_spell_lengths """
		sol = [ 
				[4, 4, 4, 4],
				[2, 2, 2, 2],
				[1, 1, 1, 1],
				[2, 2, 2, 2]
			  ]	
		sol = pd.DataFrame(sol, index=['C1', 'C2', 'C3', 'C4'], columns=['Y1', 'Y2', 'Y3', 'Y4'])
		comp = compute_spell_lengths(self.data)
		assert_frame_equal(comp, sol)