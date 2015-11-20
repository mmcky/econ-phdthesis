"""
ISO3N to CountryName Dictionary for Classification: HS02

Manual Check: 09/09/2014

Notes
-----

Checking Code Snippets

[1] This list contains some country codes that are NOT found in ISO3166
	<code snippet>
	#-BACI-#
	from pyeconlab.trade.dataset.CEPIIBACI.meta import hs02_iso3n_to_iso3c
	baci_iso3n = set([int(x) for x in hs02_iso3n_to_iso3c.keys()])
	#-ISO3166-#
	from pyeconlab.country import ISO3166
	iso = ISO3166()
	iso3n = [int(item) for item in iso.iso3n]
	#-Compare Sets-#
	x = set(baci_iso3n)
	y = set(iso3n)
	print len(x) 							#217
	print len(y) 							#249
	print len(set(x).intersection(y)) 		#203

	Codes in BACI but not ISO3166 (x - y)
	{58, 251, 381, 490, 530, 536, 579, 699, 711, 736, 757, 842, 849, 891}

Not Countries
~~~~~~~~~~~~~
'536' : 'NTZ' 	=> Neutral Zone

[2] Checking the Inverse of the Dictionary using ISO3C
	<code snippet>
	#-BACI-#
	from pyeconlab.trade.dataset.CEPIIBACI.meta import iso3n_to_iso3c
	hs02_iso3c_to_iso3n = {v:k for k,v in iso3n_to_iso3c['HS02'].items()}
	baci_iso3c = set(hs02_iso3c_to_iso3n.keys())
	#-ISO3166-#
	from pyeconlab.country import ISO3166
	iso = ISO3166()
	#-Compare Sets-#
	x = set(baci_iso3c)
	y = set(iso.iso3c)
	print len(x) 							#217
	print len(y) 							#249
	print len(set(x).intersection(y)) 		#211

	Codes in BACI but not ISO3166 (x - y)
	~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	{'ANT', 'NTZ', 'ROM', 'TMP', 'YUG', 'ZAR'}

	These are transitional codes that have been reserved after deletion from ISO3166-1

	ANT Netherlands Antilles
	NTZ Neutral Zone  
	ROM Romania
	TMP East Timor
	YUG Yugoslavia
	ZAR Zaire

Not Countries:
~~~~~~~~~~~~~
'NTZ' : Neutral Zone

"""

iso3n_to_iso3c = {
				'4' : 'AFG',
				'8' : 'ALB',
				'12' : 'DZA',
				'16' : 'ASM',
				'20' : 'AND',
				'24' : 'AGO',
				'28' : 'ATG',
				'31' : 'AZE',
				'32' : 'ARG',
				'36' : 'AUS',
				'40' : 'AUT',
				'44' : 'BHS',
				'48' : 'BHR',
				'50' : 'BGD',
				'51' : 'ARM',
				'52' : 'BRB',
				'58' : 'BEL',
				'60' : 'BMU',
				'64' : 'BTN',
				'68' : 'BOL',
				'70' : 'BIH',
				'76' : 'BRA',
				'84' : 'BLZ',
				'86' : 'IOT',
				'90' : 'SLB',
				'92' : 'VGB',
				'96' : 'BRN',
				'100' : 'BGR',
				'104' : 'MMR',
				'108' : 'BDI',
				'112' : 'BLR',
				'116' : 'KHM',
				'120' : 'CMR',
				'124' : 'CAN',
				'132' : 'CPV',
				'136' : 'CYM',
				'140' : 'CAF',
				'144' : 'LKA',
				'148' : 'TCD',
				'152' : 'CHL',
				'156' : 'CHN',
				'162' : 'CXR',
				'166' : 'CCK',
				'170' : 'COL',
				'174' : 'COM',
				'178' : 'COG',
				'180' : 'ZAR',
				'184' : 'COK',
				'188' : 'CRI',
				'191' : 'HRV',
				'192' : 'CUB',
				'196' : 'CYP',
				'203' : 'CZE',
				'204' : 'BEN',
				'208' : 'DNK',
				'212' : 'DMA',
				'214' : 'DOM',
				'218' : 'ECU',
				'222' : 'SLV',
				'226' : 'GNQ',
				'231' : 'ETH',
				'232' : 'ERI',
				'233' : 'EST',
				'238' : 'FLK',
				'242' : 'FJI',
				'246' : 'FIN',
				'251' : 'FRA',
				'258' : 'PYF',
				'260' : 'ATF',
				'262' : 'DJI',
				'266' : 'GAB',
				'268' : 'GEO',
				'270' : 'GMB',
				'276' : 'DEU',
				'288' : 'GHA',
				'292' : 'GIB',
				'296' : 'KIR',
				'300' : 'GRC',
				'304' : 'GRL',
				'308' : 'GRD',
				'316' : 'GUM',
				'320' : 'GTM',
				'324' : 'GIN',
				'328' : 'GUY',
				'332' : 'HTI',
				'340' : 'HND',
				'344' : 'HKG',
				'348' : 'HUN',
				'352' : 'ISL',
				'360' : 'IDN',
				'364' : 'IRN',
				'368' : 'IRQ',
				'372' : 'IRL',
				'376' : 'ISR',
				'381' : 'ITA',
				'384' : 'CIV',
				'388' : 'JAM',
				'392' : 'JPN',
				'398' : 'KAZ',
				'400' : 'JOR',
				'404' : 'KEN',
				'408' : 'PRK',
				'410' : 'KOR',
				'414' : 'KWT',
				'417' : 'KGZ',
				'418' : 'LAO',
				'422' : 'LBN',
				'428' : 'LVA',
				'430' : 'LBR',
				'434' : 'LBY',
				'440' : 'LTU',
				'446' : 'MAC',
				'450' : 'MDG',
				'454' : 'MWI',
				'458' : 'MYS',
				'462' : 'MDV',
				'466' : 'MLI',
				'470' : 'MLT',
				'478' : 'MRT',
				'480' : 'MUS',
				'484' : 'MEX',
				'490' : 'TWN',
				'496' : 'MNG',
				'498' : 'MDA',
				'500' : 'MSR',
				'504' : 'MAR',
				'508' : 'MOZ',
				'512' : 'OMN',
				'520' : 'NRU',
				'524' : 'NPL',
				'528' : 'NLD',
				'530' : 'ANT',
				'533' : 'ABW',
				'536' : 'NTZ',
				'540' : 'NCL',
				'548' : 'VUT',
				'554' : 'NZL',
				'558' : 'NIC',
				'562' : 'NER',
				'566' : 'NGA',
				'570' : 'NIU',
				'574' : 'NFK',
				'579' : 'NOR',
				'580' : 'MNP',
				'583' : 'FSM',
				'584' : 'MHL',
				'585' : 'PLW',
				'586' : 'PAK',
				'591' : 'PAN',
				'598' : 'PNG',
				'600' : 'PRY',
				'604' : 'PER',
				'608' : 'PHL',
				'612' : 'PCN',
				'616' : 'POL',
				'620' : 'PRT',
				'624' : 'GNB',
				'626' : 'TMP',
				'634' : 'QAT',
				'642' : 'ROM',
				'643' : 'RUS',
				'646' : 'RWA',
				'654' : 'SHN',
				'659' : 'KNA',
				'660' : 'AIA',
				'662' : 'LCA',
				'666' : 'SPM',
				'670' : 'VCT',
				'674' : 'SMR',
				'678' : 'STP',
				'682' : 'SAU',
				'686' : 'SEN',
				'690' : 'SYC',
				'694' : 'SLE',
				'699' : 'IND',
				'702' : 'SGP',
				'703' : 'SVK',
				'704' : 'VNM',
				'705' : 'SVN',
				'706' : 'SOM',
				'711' : 'ZAF',
				'716' : 'ZWE',
				'724' : 'ESP',
				'732' : 'ESH',
				'736' : 'SDN',
				'740' : 'SUR',
				'752' : 'SWE',
				'757' : 'CHE',
				'760' : 'SYR',
				'762' : 'TJK',
				'764' : 'THA',
				'768' : 'TGO',
				'772' : 'TKL',
				'776' : 'TON',
				'780' : 'TTO',
				'784' : 'ARE',
				'788' : 'TUN',
				'792' : 'TUR',
				'795' : 'TKM',
				'796' : 'TCA',
				'798' : 'TUV',
				'800' : 'UGA',
				'804' : 'UKR',
				'818' : 'EGY',
				'826' : 'GBR',
				'834' : 'TZA',
				'842' : 'USA',
				'849' : 'UMI',
				'854' : 'BFA',
				'858' : 'URY',
				'860' : 'UZB',
				'862' : 'VEN',
				'876' : 'WLF',
				'882' : 'WSM',
				'887' : 'YEM',
				'891' : 'YUG',
				'894' : 'ZMB',
}