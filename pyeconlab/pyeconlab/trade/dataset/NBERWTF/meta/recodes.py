"""
NBERFeenstraWTF Recodes Meta Data
=================================

STATUS: DEPRECATED (Moved to Current Info to Intertemporal)

This Module contains custom recodes for NBERFeenstraWTF Dataset

Notes
-----
[1] Many of these concordances are HAND constructed by looking at supporting meta data contained in ``xlsx`` etc.

Future Work 
----------- 
[1] Countries with '.' should they remain in the dataset under the assumption that years with no reports indicate 0 trade?
[2] Document '.' decisions clearly in a within repo file

"""

from pyeconlab.util import from_dict_to_csv

#-----------------------#
#-Intertemporal Recodes-#
#-----------------------#

class intertemporal(object):
	""" 
	Class Containing Intertemporal / Dynamic Consistent Recodes
	"""

	#-Joint Importer / Exporter Recodes-#
	#-----------------------------------#

	#-Contains ISO3C to SPECIAL CODES Required to Make 1962 to 2000 Dynamically Consistent (Strict)-#
	#-Moved '.' recodes to incomplete_iso3c_for_1962_2000-#
	iso3c_for_1962_2000 = {
			'ARM' 	: 'SP1',
			'AZE' 	: 'SP1',
			'BGD' 	: 'SP2',
			'BLR' 	: 'SP1',
			'BIH' 	: 'SP3',
			'MAC' 	: 'CHN', 	#Merge Macao with China
			'HRV' 	: 'SP3',
			'CZE' 	: 'SP4',
			'CSK'	: 'SP4',
			'EST' 	: 'SP1',
			'DDR' 	: 'SP5',
			'DEU'	: 'SP5',
			'SUN' 	: 'SP1',
			'YEM' 	: 'SP6',
			'YMD' 	: 'SP6', 
			'YUG' 	: 'SP3',
			'GEO'	: 'SP1',
			'KAZ' 	: 'SP1',
			'KGZ'	: 'SP1',
			'LVA' 	: 'SP1',
			'LTU' 	: 'SP1',
			'MDA' 	: 'SP1',
			'RUS' 	: 'SP1',
			'SVK' 	: 'SP4',
			'SVN' 	: 'SP3',
			'MKD' 	: 'SP3',
			'TJK'	: 'SP1',
			'TKM' 	: 'SP1',
			'UKR' 	: 'SP1',
			'UZB' 	: 'SP1',
			'SCG' 	: 'SP3',
	}


	iso3c_for_1962_2000_definitions = {
			#-Splits-#
			'SP1' 	: 	('SUN', ['ARM', 'AZE', 'BLR', 'EST', 'GEO', 'KAZ', 'KGZ', 'LVA', 'LTU', 'MDA', 'RUS', 'TJK', 'TKM', 'UKR', 'UZB']),
			'SP2' 	: 	('IND', ['IND', 'BGD']),
			'SP3' 	: 	('YUG', ['BIH', 'HRV', 'MKD', 'MNE', 'SVN', 'SRB']),
			'SP4' 	: 	('CSK', ['CZE', 'SVK']),
			#-Joins-#
			'SP5' 	: 	(['DEU','DDR'], 'DEU'), 		
			'SP6' 	: 	(['YEM', 'YMD'], 'YEM'), 		
	}

	incomplete_iso3c_for_1962_2000 = {
			'ARE' 	: '.', 		#INTERPOLATE?
			'BLZ'	: '.', 		#INTERPOLATE?
			'FLK' 	: '.',
			'GIB' 	: '.', 		#INTERPOLATE?
			'GRL' 	: '.', 		#INTERPOLATE?
			'MWI'	: '.',		#INTERPOLATE?
			'PSE' 	: '.',
			'RWA' 	: '.', 		#INTERPOLATE?
			'SYC' 	: '.', 		#Add to FRA?
			'SHN' 	: '.', 		#Merge with Britain?
			'KNA' 	: '.', 		#INTERPOLATE?
			'ZWE' 	: '.', 		#INTERPOLATE?
	}

	def iso3c_for_1962_2000_csv(self, fl='intertemporal_iso3c_for_1962_2000.csv', target_dir='csv/'):
		"""
		Simple Utility for writing iso3c_for_1962_2000 to csv file
		"""
		from_dict_to_csv(self.iso3c_for_1962_2000, header=['iso3c', '_recode_'], fl=fl, target_dir=target_dir)

	def incomplete_iso3c_for_1962_2000_csv(self, fl='incomplete_iso3c_for_1962_2000.csv', target_dir='csv/'):
		"""
		Simple Utility for writing incomplete_iso3c_for_1962_2000 to csv file
		"""
		from_dict_to_csv(self.incomplete_iso3c_for_1962_2000, header=['iso3c', '_recode_'], fl=fl, target_dir=target_dir)

	#----INWORK----#


	#-SAME AS ABOVE except "countryname" to SPECIAL CODES Required to Make 1962 to 2000 Dynamically Consistent-#
	
	countryname_for_1962_2000 = { 
		
		# - WORKING HERE - # 
	
	}

	#-Exporter Specific-#
	#-------------------#

	exporter_recodes_for_1962_2000 = {
			'ARM' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'AZE' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'BGD' 	: 'IND-INDBGD',
			'BLR' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'BLZ'	: '.', 														#INTERPOLATE?
			'BIH' 	: 'YUG-BIHHRVMKDMNESVNSRB',
			'MAC' 	: 'CHN', 													#Merge Macao with China
			'HRV' 	: 'YUG-BIHHRVMKDMNESVNSRB',
			'CZE' 	: 'CSK-CZESVK',
			'CSK'	: 'CSK-CZESVK',
			'EST' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'FLK' 	: '.',
			'DDR' 	: 'DEU-DDRDEU',
			'DEU'	: 'DEU-DDRDEU',
			'SUN' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'YEM' 	: 'YEMYMD-YEM',
			'YMD' 	: 'YEMYMD-YEM', 
			'YUG' 	: 'YUG-BIHHRVMKDMNESVNSRB',
			'GEO'	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'GIB' 	: '.', 														#INTERPOLATE?
			'GRL' 	: '.', 														#INTERPOLATE?
			'KAZ' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'KGZ'	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'LVA' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'LTU' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'MWI'	: '.',														#INTERPOLATE?
			'PSE' 	: '.',
			'MDA' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'RUS' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'RWA' 	: '.', 														#INTERPOLATE?
			'SYC' 	: '.', 														#Add to FRA?
			'SVK' 	: 'CSK-CZESVK',
			'SVN' 	: 'YUG-BIHHRVMKDMNESVNSRB',
			'SHN' 	: '.', 														#Merge with Britain?
			'KNA' 	: '.', 														#INTERPOLATE?
			'MKD' 	: 'YUG-BIHHRVMKDMNESVNSRB',
			'TJK'	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'TKM' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'UKR' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'ARE' 	: '.', 														#INTERPOLATE?
			'UZB' 	: 'SUN-ARMAZEBLRESTGEOKAZKGZLVALTUMDARUSTJKTKMUKRUZB',
			'SCG' 	: 'YUG-BIHHRVMKDMNESVNSRB',
			'ZWE' 	: '.', 														#INTERPOLATE?
	}

	#-Importer Specific-#
	#-------------------#

	importer_recodes_for_1962_2000 = {
		
		# - WORKING HERE - #
	
	}