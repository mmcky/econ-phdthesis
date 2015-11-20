XLSX Meta Files
---------------

STATUS: NEEDS UPDATING

Files
-----

Manually Edited / Notes
-----------------------

[1] importer-iiso3n_intertemporal_countrycode_adjustments.xlsx 													{2}
		Description: 	Contains Adjustments and Special Cases to get Consistent Country Classifications Over time. 
						[WARNING: Includes Adjustment notes for Splits and Merges - Do not simply replace from {2}]
		md5hash: 		2a7d7a2ae3ee3308a7837eeb914dcffe
		Notes: 			This File Requires MANUAL ADITIONS to Establish Appropriate Groups

[2] exporter-eiso3n_intertemporal_countrycode_adjustments.xlsx 													{2}
		Description: 	Contains Adjustments and Special Cases to get Consistent Country Classifications Over time. 
						[WARNING: Includes Adjustment notes for Splits and Merges - Do not simply replace from {2}]
		md5hash: 		ac11487884239e53d39efb4ecb30983e
		Notes: 			This File Requires MANUAL ADITIONS to Establish Appropriate Groups


[3] intertemporal_sitc4_wmeta_adjustments.xlsx 																	{3}
		Status: 		BEING EDITED
		Description: 	Same as [7] Except Contains only cases that need to be adjusted
						Codes != SITCR2 Offical Code OR 'prc_coverage' != 1

Generated Data Files
--------------------

[1] intertemporal_eiso3n 	
		Description: 	list of all eiso3n codes and how they are used across the years 1962 to 2000			{1}
		md5hash:  		3331242177b82a4972027f25b7f05664

[2] intertemporal_iiso3n 	
		Description: 	list of all iiso3n codes and how they are used across the years 1962 to 2000 			{1}
		md5hash: 		063e34620148c99f59d968fd248c1cb6

[4] importer-iiso3n_intertemporal_countrycode_spells.xlsx 														{2}
		Description: 	Intertemporal CountryCode Spells of Unique iso3n country codes found in the dataset
		md5hash: 		d5d473689b87da17e767c8eb6b63eb26

[5] exporter-eiso3n_intertemporal_countrycode_spells.xlsx 														{2}
		Description: 	Intertemporal CountryCode Spells of Unique iso3n country codes found in the dataset
		md5hash: 		d5d473689b87da17e767c8eb6b63eb26

/sitc4
~~~~~~

[1] intertemporal_sitc4 	
		Description: 	list of all when sitc4 codes are used across the years 1962 to 2000 					{1}
		md5hash: 		357662baf4db691b10b182e2a683adda

[2] intertemporal_sitc4.xlsx 																					{3}
		Description: 	Contains table of intertemporal sitc4 codes
		md5hash: 		357662baf4db691b10b182e2a683adda
		Notes: 			[1] Country Specific Versions are also available but NOT held in the repository due to 
						their size ~45Mb. These can be generated for exporters using country='exporter' and
						importers using country='importer' in the recipes below. 

[3] intertemporal_sitc4_wmeta.xlsx 																				{3}
		Description: 	Contains Adjustments and Notes considering intertemporal sitc4 codes and includes a marker
						for SITC Revision 2 official Codes
		md5hash: 		dc37c350133cc60bf294a71acf9bdedb
		Notes: 			[1] Country Specific Versions are also available but NOT held in the repository due to 
						their size ~45Mb. These can be generated for exporters using country='exporter' and
						importers using country='importer' in the recipes below. 

[4] intertemporal_sitc4_values_wmeta.xlsx																		{3}
		Description: 	Same as [7] except includes Total Aggregated Values Contained in the Dataset 		
		md5hash: 		29d154760c3826942797e4d6167ba488
		Notes: 			[1] Country Specific Versions are also available but NOT held in the repository due to 
						their size ~45Mb. These can be generated for exporters using country='exporter' and
						importers using country='importer' in the recipes below. 

[5] intertemporal_sitc4_valuecompositions_L3.xlsx 														{3}
		Description 	Same as [7] except values are Compositions relative to Level 3 Codes
		md5hash: 		208eb56e6bda87b1a4e41f604402fe43
		Notes: 			[1] Country Specific Versions are also available but NOT held in the repository due to 
						their size ~45Mb. These can be generated for exporters using country='exporter' and
						importers using country='importer' in the recipes below. 

[6] intertemporal_sitc4_valuecompositions_L2.xlsx 														{3}
		Description 	Same as [7] except values are Compositions relative to Level 2 Codes
		md5hash: 		bdbf90cd6178ce5c7fe1357e21b73753
		Notes: 			[1] Country Specific Versions are also available but NOT held in the repository due to 
						their size ~45Mb. These can be generated for exporters using country='exporter' and
						importers using country='importer' in the recipes below. 

[7] intertemporal_sitc4_valuecompositions_L1.xlsx 														{3}
		Description 	Same as [7] except values are Compositions relative to Level 1 Codes
		md5hash: 		ba60871412a8045e1ade3caf37d7c3d2
		Notes: 			[1] Country Specific Versions are also available but NOT held in the repository due to 
						their size ~45Mb. These can be generated for exporters using country='exporter' and
						importers using country='importer' in the recipes below. 


Construction Recipe:
--------------------
{1}		from pyeconlab import NBERFeenstraWTFConstructor
		source_dir=r"E:\work-data\x_datasets\36a376e5a01385782112519bddfac85e"
		a = NBERFeenstraWTFConstructor(source_dir=source_dir)
		a.set_dataset(a.raw_data)
		a.write_metadata()

{2} 	from pyeconlab import NBERFeenstraWTFConstructor
		SOURCE_DATA_DIR = "E:\\work-data\\x_datasets\\36a376e5a01385782112519bddfac85e\\" #win7
		a = NBERFeenstraWTFConstructor(source_dir=SOURCE_DATA_DIR)
		a.countries_only()
		i,e = a.intertemporal_countrycodes(dataset=True)
		
		from pyeconlab.util import compute_number_of_spells
		#ispells
		ispells = compute_number_of_spells(i)
		total_coverage = len(ispells.columns)
		ispells['coverage'] = ispells.sum(axis=1)
		ispells['prc_coverage'] = ispells['coverage'] / total_coverage
		ispells.to_excel('importer-iiso3n_intertemporal_countrycode_spells.xlsx')

		ispell_cases = ispells[ispells['coverage'] != total_coverage]
		ispell_cases.to_excel('importer-iiso3n_intertemporal_countrycode_adjustments.xlsx') #Requires Manual Adjustment

		#espells
		espells = compute_number_of_spells(e)
		total_coverage = len(espells.columns)
		espells['coverage'] = espells.sum(axis=1)
		espells['prc_coverage'] = espells['coverage'] / total_coverage
		espells.to_excel('exporter-eiso3n_intertemporal_countrycode_spells.xlsx')

		espell_cases = espells[espells['coverage'] != total_coverage]
		espell_cases.to_excel('exporter-eiso3n_intertemporal_countrycode_adjustments.xlsx') #Requires Manual Adjustment

{3} 	from pyeconlab import NBERFeenstraWTFConstructor
		SOURCE_DATA_DIR = "E:\\work-data\\x_datasets\\36a376e5a01385782112519bddfac85e\\" #win7
		a = NBERFeenstraWTFConstructor(source_dir=SOURCE_DATA_DIR)

		x = a.intertemporal_productcodes_dataset(tabletype='values')
		x.to_excel('intertemporal_sitc4_values_wmeta.xlsx')

		x = a.intertemporal_productcodes_dataset(tabletype='indicator')
		x.to_excel('intertemporal_sitc4_wmeta.xlsx')

		x = x.reset_index()
		x = x.loc[(x.SITCR2 != 1) | (x.prc_coverage != 1)]
		x = x.set_index(['sitc4', 'SITCR2'])
		x.to_excel('./intertemporal_sitc4_wmeta_adjustments.xlsx')

		x = a.intertemporal_productcodes_dataset(tabletype='composition', level=3)
		x.to_excel('intertemporal_sitc4_valuecompositions_L3.xlsx')

		x = a.intertemporal_productcodes_dataset(tabletype='composition', level=2)
		x.to_excel('intertemporal_sitc4_valuecompositions_L2.xlsx')

		x = a.intertemporal_productcodes_dataset(tabletype='composition', level=1)
		x.to_excel('intertemporal_sitc4_valuecompositions_L1.xlsx')

