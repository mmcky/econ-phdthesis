"""
Meta Data and Statistical Descriptive Tools for Trade Datasets

"""

import pandas as pd

def describe(dataset, table_name="", productcode="productcode", importer="iiso3c", exporter="eiso3c", year="year", verbose=False):
	"""
	Describe a Trade Dataset 

	Parameters
	----------
	table_name 	: 	str
					Name for Data Series
	productcode : 	str
					Name given to Productcode (NBER has 'SITCL3'!)

	Notes:
	------
	1. Requires iso3c codes
	1. Infer parameter from calling class?
	"""
	table_data = [] 								#Collector For Table Data
	table_idx = [] 									#Collector for Table Index
	#-Export and Import Specific Data-#
	try:
		table_data.append(len(dataset[exporter].unique()))
		table_idx.append("Exporters")
		if table_name == "": table_name = "Exports"
	except:
		if table_name == "": table_name = "Imports"
	try:
		table_data.append(len(dataset[importer].unique()))
		table_idx.append("Importers")
		if table_name == "Exports": table_name = "Trade"
	except:
		pass
	#-General Statistics-#
	table_data.append(len(dataset[productcode].unique()))
	table_idx.append('Products')
	table_data.append(len(dataset['year'].unique()))
	table_idx.append('Years')
	table_data.append(len(dataset))
	table_idx.append('Trade Flows')
	#-Construct Table
	return pd.DataFrame({table_name : table_data}, index=table_idx)
	
