"""
Special or Experimental Research Functions for Trade
"""

from pyeconlab.trade.util import from_dict_to_dataframe
import pandas as pd
import numpy as np

# def productspace_5yearavg_improbableemergence(Mcp_ImProbableProducts, GDPGrowth, window=(5,5), verbose=False):
# 	""" 
# 	Construct a table:
#  					-5Y, -4Y ... 0Y ... 4Y, 5Y
# 	<country>  		 GDPGrowth

# 	Mcp_ImProbableProducts 	: Dict(pd.DataFrame(index=(country), columns=(productcode)))
# 	GDPGrowth 				: pd.Series(index=(country, year))
# 	"""
# 	#-Find Improbable Product Emergence Events-#
# 	r,i = [],[]
# 	for idx,s in  from_dict_to_dataframe(Mcp_ImProbableProducts).iterrows():
# 	    year, country, productcode = idx
# 	    value = s['ImProbProducts']
# 	    if value > 0:
# 	    	i.append((year, country))
# 	    	r.append(value)  #1?	
# 	i = pd.MultiIndex.from_tuples(i)
# 	df = pd.DataFrame(r, index=i)
# 	df.columns = ['ImProb']
# 	df.index.names = ['year', 'country']
# 	df.sort_index(inplace=True)
# 	#-Sum Duplicates-#
# 	df = df.groupby(level=['year', 'country']).sum()
	
# 	#-Find GDPGrowth n Years before and n years after
# 	cols = []
# 	for item in range(window[0],0,-1):
# 		cols.append('Y(-%s)'%item)
# 	cols.append('Y0')
# 	for item in range(window[1],0,-1):
# 		cols.append('Y(%s)'%item)
	
# 	cols = ['Y(-5)', 'Y(-4)', 'Y(-3)', 'Y(-2)', 'Y(-1)', 'Y(0)', 'Y(1)', 'Y(2)', 'Y(3)', 'Y(4)', 'Y(5)']
# 	i = []
# 	r = []
# 	for idx, s in df.iterrows():
# 		year, country = idx
# 		row = []
# 		i.append((year, country))
# 		for yr in range(year-window[0]-1, year+window[1]+1, 1):
# 			if (yr, country) in i:
# 				continue
# 			if yr < 1962:
# 				row.append(np.nan)
# 				continue
# 			if yr > 2012:
# 				row.append(np.nan)
# 				continue
# 			try:
# 				gdpgrowth = WDIData["GDPGrowth"].ix[(country, yr)]
# 			except:
# 				gdpgrowth = np.nan
# 			row.append(gdpgrowth)
# 		r.append(row)
# 	i = pd.MultiIndex.from_tuples(i)
# 	df = pd.DataFrame(r, index=i, columns=cols)
# 	df.index.names = ['year', 'country']
# 	df.sort_index(inplace=True)
# 	return df


def productspace_5yearavg_improbableemergence(Mcp_ImProbableProducts, GDPGrowth, window=(5,5), verbose=False):
	""" 
	Construct a table:
 					-5Y, -4Y ... 0Y ... 4Y, 5Y
	<country>  		 GDPGrowth

	Mcp_ImProbableProducts 	: Dict(pd.DataFrame(index=(country), columns=(productcode)))
	GDPGrowth 				: pd.Series(index=(country, year))
	"""
	#-Find Improbable Product Emergence Events-#
	r,i = [],[]
	for idx,s in  from_dict_to_dataframe(Mcp_ImProbableProducts).iterrows():
	    year, country, productcode = idx
	    value = s['ImProbProducts']
	    if value > 0:
	    	i.append((year, country))
	    	r.append(value)  #1?	
	i = pd.MultiIndex.from_tuples(i)
	df = pd.DataFrame(r, index=i)
	df.columns = ['ImProb']
	df.index.names = ['year', 'country']
	df.sort_index(inplace=True)
	#-Sum Duplicates-#
	df = df.groupby(level=['year', 'country']).sum()
	
	#-Find GDPGrowth n Years before and n years after
	cols = []
	for item in range(window[0],0,-1):
		cols.append('Y(-%s)'%item)
	cols.append('Y0')
	for item in range(window[1],0,-1):
		cols.append('Y(%s)'%item)
	
	cols = ['Y(-5)', 'Y(-4)', 'Y(-3)', 'Y(-2)', 'Y(-1)', 'Y(0)', 'Y(1)', 'Y(2)', 'Y(3)', 'Y(4)', 'Y(5)']
	i = []
	r = []
	for idx, s in df.iterrows():
		year, country = idx
		row = []
		i.append((year, country))
		for yr in range(year-window[0]-1, year+window[1]+1, 1):
			if (yr, country) in i:
				continue
			if yr < 1962:
				row.append(np.nan)
				continue
			if yr > 2012:
				row.append(np.nan)
				continue
			try:
				gdpgrowth = GDPGrowth.ix[(country, yr)]
			except:
				gdpgrowth = np.nan
			row.append(gdpgrowth)
		r.append(row)
	i = pd.MultiIndex.from_tuples(i)
	df = pd.DataFrame(r, index=i, columns=cols)
	df.index.names = ['year', 'country']
	df.sort_index(inplace=True)
	return df