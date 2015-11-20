Trade Concordance Data Files
============================

Trade Concordance Data Files. 
Concordances are added as they are required

Files
=====

un/
~~~

[1] HS2002_to_SITCR2.xls
	Description: 		UN HS2002 to SITC Revision 2 Concordance and Correlation Table
	Sheets: 			"Conversion Table", "Correlation Table"
	MD5 Hash: 			30bfcd3afb56b2e9a3c185614b4d4b9d
	Original Name: 		HS2002 to SITC2 Conversion and Correlation Tables.xls
	Source: 			http://unstats.un.org/unsd/trade/conversions/HS%20Correlation%20and%20Conversion%20tables.htm
	Downloaded: 		09/09/2014
	Notes: 				The internal structure is not straight forward when writing parser

	Manually Constructed Children
	~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	HS2002_to_SITCR2.csv (md5hash = e5799dca9d763dfc6e4c0fe815fe3e40)
		This file contains the "conversion table" sheet with titles ['HS2002', 'SITCR2'] saved as a csv file format
		Because pd.read_excel() cannot set dtype's the codes are entered as integers, this allows the use of read_csv() which can import product codes as strings (if required)

[2] HS1996_to_SITCR2.xls
	Description: 		UN HS1996 to SITC Revision 2 Concordance and Correlation Table
	Sheets: 			"Conversion Table, "Correlation Table"
	MD5 Hash: 			5068c6b22f54763b058c809a77f5e88c
	Original Name: 		HS1996 to SITC2 Conversion and Correlation Tables.xls
	Source: 			http://unstats.un.org/unsd/trade/conversions/HS%20Correlation%20and%20Conversion%20tables.htm
	Downloaded: 		09/09/2014
	Notes: 				The internal structure is not straight forward when writing parser

	Manually Constructed Children
	~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	HS1996_to_SITCR2.csv (md5hash = 5ab1898aba4c4ce21257b73845c6ca34)
		This file contains the "conversion table" sheet with titles ['HS1996', 'SITCR2'] saved as a csv file format
		Because pd.read_excel() cannot set dtype's the codes are entered as integers, this allows the use of read_csv() which can import product codes as strings (if required)		


wits/
~~~~

[1] HS2002_to_SITC2.csv	
	Description: 		WITS HS2002 to SITC Revision 2 Concordance Table
	MD5 Hash: 			8df5c069d55ca90a94f46ddadd5fa60d
	Original Name: 		JobID-37_Concordance_H2_to_S2.CSV
	Source: 			https://wits.worldbank.org/product_concordance.html
	Downloaded: 		09/09/2014


Notes
-----
  1. Should I include the binary files here? No Real Point except for validation of the CSV File