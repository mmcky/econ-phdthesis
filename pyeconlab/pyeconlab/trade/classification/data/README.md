Trade Classification Subpackage Data
====================================

Sources
=======

	un
	--	 	
		http://unstats.un.org/unsd/tradekb/Knowledgebase/UN-Comtrade-Reference-Tables
		http://unstats.un.org/unsd/tradekb/Knowledgebase/Harmonized-Commodity-Description-and-Coding-Systems-HS
		http://unstats.un.org/unsd/tradekb/Knowledgebase/Comtrade-Country-Code-and-Name


	wits
	----
		http://wits.worldbank.org/referencedata.html


Folders
=======


	mit-medialabs/
		SITC
		----
		sitc_classification.csv : 	Description: SITC Classification Table
									Type: 		csv delimited text file
									Headers: 	[SITC,Name]
									Downloaded: 01/08/2014
									md5hash: 	a27565834eb229f491ca36ac5c7f1f7e

	un/
		HS (Harmonised System)
		----------------------
		H0.txt 				: 	Description: HS1992(1988 - Introduction, 1992 - Adoption)
								Type: 		csv delimited text file
								Headers: 	["Code","ShortDescription","LongDescription","isBasicLevel","AggregateLevel","parentCode"]
								Downloaded: 31/07/2014
								md5hash: 	c1a5806a5e4b44c7116398d7469e0d1f

		H1.txt 				: 	Description: HS1996
								Type: 		csv delimited text file
								Headers: 	["Code","ShortDescription","LongDescription","isBasicLevel","AggregateLevel","parentCode"]
								Downloaded: 31/07/2014
								md5hash: 	7d00c9f7e0e5af26510a2ce6ea916352
		
		H2.txt 				: 	Description: HS2002
								Type: 		csv delimited text file
								Headers: 	["Code","ShortDescription","LongDescription","isBasicLevel","AggregateLevel","parentCode"]
								Downloaded: 31/07/2014
								md5hash: 	0fb0d725a64a24370bf04556071199ac

		H3.txt 				: 	Description: HS2007
								Type: 		csv delimited text file
								Headers: 	["Code","ShortDescription","LongDescription","isBasicLevel","AggregateLevel","parentCode"]
								Downloaded: 31/07/2014
								md5hash: 	691ada3ec69a7b5e12b5a2de918dd82a

		H4.txt 				: 	Description: HS???? 
								Type: 		csv delimited text file
								Headers: 	["code","ShortDescription","LongDescription","isBasicLevel","AggregateLevel","ParentCode"]
								Downloaded: 31/07/2014
								md5hash: 	0e08111f6aeb9e6b639c5604fa7074d7

		SITC (Standard Industry Trade Classification)
		---------------------------------------------
		S1.txt 				: 	Description: SITC Revision 1 Classifications
								Type:		csv separated text file
								Headers: 	["Code","ShortDescription","LongDescription","isBasicLevel","AggregateLevel","parentCode"]
								Downloaded: 31/07/2014
								md5hash: 	38295f99db5ee8d588411d1c6cc01a1f

		S2.txt 				: 	Description: SITC Revision 2 Classifications
								Type:		csv separated text file
								Headers: 	["Code","ShortDescription","LongDescription","isBasicLevel","AggregateLevel","parentCode"]
								Downloaded: 31/07/2014
								md5hash: 	57709687a9aa8bc9ab45c10ae0eeaf48

		S3.txt 				: 	Description: SITC Revision 3 Classifications
								Type:		csv separated text file
								Headers: 	["Code","ShortDescription","LongDescription","isBasicLevel","AggregateLevel","parentCode"]
								Downloaded: 31/07/2014
								md5hash: 	bb120abdee8c8eb97edde8bf43394a11

		S4.txt 				: 	Description: SITC Revision 4 Classifications
								Type:		csv separated text file
								Headers: 	["Code","ShortDescription","LongDescription","isBasicLevel","AggregateLevel","parentCode"]
								Downloaded: 31/07/2014
								md5hash: 	f53be9c2c7edd12fe2d03c7be09a49ce				

		**Note:** There are other archives: http://unstats.un.org/unsd/tradekb/Knowledgebase/UN-Comtrade-Reference-Tables

	wits/
		SITCProducts.zip 	:	Description: SITC Product Classifications
								Contains: 	SITCPrdoucts.xls (With Sheets: `SITC 1`, `SITC 2`, `SITC 3`, `SITC 4`)
								Type: 		ZIP Archive
								Downloaded: 31/07/2014 
								md5hash: 	8fa873eabaddcd23752ffdb82fbee94f
								Notes: 		Stored as an xls file (binary)

		**Note:** There are other archives: http://wits.worldbank.org/referencedata.html
