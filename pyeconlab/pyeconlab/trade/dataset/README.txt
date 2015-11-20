Folder: pyeconlab/trade/data

This folder does not contain trade data, but methods to build and compile data from specific source files

Supported Datasets
------------------

1. NBERFeenstraWTF     :   NBER Feenstra World Trade Flows (SITCR2, 1962 to 2000)
2. CEPII-BACI          :   Trade Dataset from CEPII (various datasets available)

Simplification Projects
-----------------------

1. Trade Friendly DataFrames

	1xD Vectors
	-----------
	CTradeDataFrame
	PTradeDataFrame

	CYTradeDataFrame
	PYTradeDataFrame

	2xD Vectors (Cross-Sections)
	----------------------------
	CPTradeDataFrame

	Index: 		Country Objects 		-> Any Country Specific Information
				Primary Identifier: ISO3C
	Columns: 	Product Objects 	-> Any Product Specific Information (Types: General, SITC, HS)
				Primary Identifier: HS or SITC Code

			0011 		0012
			 SITC.R1
			 SITC.R2
			 SITC.R3
			 HS
	
	AFG 	 VALUES
	 iso3c
	 iso3n
	 wdi
	 ...
	AUS

	Requirements: 	Pre-Computed Country and Product Objects (Stored as a Pickle or H5 database)
					__init__'s job is to assign appropriate objects to the incoming data

	3xD Vectors (Panels)
	--------------------

	CPYTradePanel (Yearly Data)



