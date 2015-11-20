"""
ISO3N to ISO3C Dictionary for Classification: HS96Source: BACI Country Concordance File

Manual Check: 19/03/2015

Missing Codes
-------------
[1] To find a list of unique missing iso3n to iso3c identifiers (using country_code_hs96.csv file)
	<code snippet>
	from pyeconlab import BACIConstructor
	SOURCE_DIR = "/home/matthewmckay/work-data/datasets/e988b6544563675492b59f397a8cb6bb/"
	baci = BACIConstructor(source_dir=SOURCE_DIR, source_classification="HS96", reduce_memory=True)
	baci.load_country_data()
	baci.add_country_iso3c()
	exporters = set(baci.dataset.loc[baci.dataset.eiso3c.isnull()].eiso3n.unique())
	importers = set(baci.dataset.loc[baci.dataset.iiso3c.isnull()].iiso3n.unique())
	missing = exporters.union(importers)

	<results 27-March-2015>
	{10, 74, 80, 129, 221, 239, 275, 290, 334, 336, 471, 473, 492, 499, 527, 531, 534, 535, 568, 577, 581, 636, 637, 688, 697, 728, 729, 807, 837, 838, 839, 879, 899}
	10 	: "Antarctica" (ATA)
	74  : "Bouvet Island" (BVT)
	80 	: "British Antarctic Territory"  							#Withdrawn Code
	129 : "." 							 							#Not Found in ISO3166
	221 : "." 														#Not Found in ISO3166
	239 : "South Georgia and the South Sandwich Islands" (SGS)
	275 : "Palestine, State of" (PSE)
	290 : "." 														#Not Found in ISO3166
	334 : "Heard Island and McDonald Islands" (HMD)
	336 : "Holy See (Vatican City State)" (VAT)
	471 : "." 														#Not Found in ISO3166
	473 : "." 														#Not Found in ISO3166
	492 : "Monaco" (MCO)
	499 : "Montenegro" (MNE)
	527 : "." 														#Not Found in ISO3166
	531 : "Curaçao" (CUW)
	534 : "Sint Maarten (Dutch part)" ()
	535 : "Bonaire, Sint Eustatius and Saba" (BES)
	568 : "." 														#Not Found in ISO3166
	577 : "." 														#Not Found in ISO3166
	581 : "United States Minor Outlying Islands" (UMI)
	636 : "." 														#Not Found in ISO3166
	637 : "." 														#Not Found in ISO3166
	688 : "Serbia" (SRB)
	697 : "." 														#Not Found in ISO3166
	728 : "South Sudan"	(SSD)										#Note: Spanish North Africa (note: this code is now used by South Sudan)
	729 : "Sudan" (SDN)
	807 : "Macedonia, the former Yugoslav Republic of" (MKD)
	837 : "." 														#Not Found in ISO3166
	838 : "." 														#Not Found in ISO3166
	839 : "." 														#Not Found in ISO3166
	879 : "." 														#Not Found in ISO3166
	899 : "." 														#Not Found in ISO3166

[2] To check which missing values iso3n-to-iso3c are important for coding
	<stata code snippet>
	use "$SOURCE/wtf62.dta", clear
	foreach year of num 63(1)99 {
		append using "$SOURCE/wtf`year'.dta"
	}
	append using "$SOURCE/wtf00.dta"
	merge m:1 i using "i_to_eiso3c.dta", keepusing(eiso3c)
	codebook i if _merge == 1

	<results 28-March-2015>
         range:  [499,729]                    units:  1
         unique values:  7                        missing .:  0/253395

         tabulation: Freq.  Value
                       19589  499
                        6255  531
                         255  534
                          11  535
                      2.3e+05 688
                          27  728
                        1541  729

    <stata code snippet>
	drop _merge
	merge m:1 j using "j_to_iiso3c.dta", keepusing(iiso3c)
	codebook j if _merge == 1

	<results 28-March-2015>
	     range:  [221,729]                    units:  1
         unique values:  9                        missing .:  0/692346

            tabulation:  Freq.  Value
                           141  221
                        1.8e+05 499
                         21383  531
                          2806  534
                            38  535
                        4.7e+05 688
                             2  697
                          1006  728
                         25106  729

    missing codes
    ~~~~~~~~~~~~~
    499 : 'MNE' 	#Montenegro
    531 : 'CUW' 	#Curaçao
   	688 : 'SRB' 	#Serbia
   	728 : 'SSD' 	#South Sudan
   	729 : 'SDN' 	#Sudan
   	807 : 'MKD' 	#Macedonia, the former Yugoslav Republic of 		


Notes
-----

Checking Code Snippets

[1] This list contains some country codes that are NOT found in ISO3166
	<code snippet>
	#-BACI-#
	from pyeconlab.trade.dataset.CEPIIBACI.meta import hs96_iso3n_to_iso3c
	baci_iso3n = set([int(x) for x in hs96_iso3n_to_iso3c.keys()])
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
	hs96_iso3c_to_iso3n = {v:k for k,v in iso3n_to_iso3c['HS96'].items()}
	baci_iso3c = set(hs96_iso3c_to_iso3n.keys())
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

[3] Checking all ISO3C codes have a corresponding match 
	<python code snippet>
	from pyeconlab import BACIConstructor
	SOURCE_DIR = "/home/matthewmckay/work-data/datasets/e988b6544563675492b59f397a8cb6bb/"
	baci = BACIConstructor(source_dir=SOURCE_DIR, source_classification="HS96", reduce_memory=True)
	from pyeconlab.trade.dataset.CEPIIBACI.meta import iso3n_to_iso3c
	concord = iso3n_to_iso3c["HS96"]
	from pyeconlab.util import concord_data
	baci.dataset["eiso3c"] = baci.dataset.eiso3n.apply(lambda x: concord_data(concord, x))
	missing = sorted(baci.dataset.loc[baci.dataset.eiso3c.isnull()].eiso3n.unique())
	missing

	<results 29/03/2015>
	[10,74,80,129,239,275,290,334,336,471,473,492,527,534,535,568,577,581,636,637,837,838,839,879,899]

"""

iso3n_to_iso3c = {
				4  : 'AFG',
				8  : 'ALB',
				12 : 'DZA',
				16 : 'ASM',
				20 : 'AND',
				24 : 'AGO',
				28 : 'ATG',
				31 : 'AZE',
				32 : 'ARG',
				36 : 'AUS',
				40 : 'AUT',
				44 : 'BHS',
				48 : 'BHR',
				50 : 'BGD',
				51 : 'ARM',
				52 : 'BRB',
				58 : 'BEL',
				60 : 'BMU',
				64 : 'BTN',
				68 : 'BOL',
				70 : 'BIH',
				76 : 'BRA',
				84 : 'BLZ',
				86 : 'IOT',
				90 : 'SLB',
				92 : 'VGB',
				96 : 'BRN',
				100 : 'BGR',
				104 : 'MMR',
				108 : 'BDI',
				112 : 'BLR',
				116 : 'KHM',
				120 : 'CMR',
				124 : 'CAN',
				132 : 'CPV',
				136 : 'CYM',
				140 : 'CAF',
				144 : 'LKA',
				148 : 'TCD',
				152 : 'CHL',
				156 : 'CHN',
				162 : 'CXR',
				166 : 'CCK',
				170 : 'COL',
				174 : 'COM',
				178 : 'COG',
				180 : 'ZAR',
				184 : 'COK',
				188 : 'CRI',
				191 : 'HRV',
				192 : 'CUB',
				196 : 'CYP',
				203 : 'CZE',
				204 : 'BEN',
				208 : 'DNK',
				212 : 'DMA',
				214 : 'DOM',
				218 : 'ECU',
				222 : 'SLV',
				226 : 'GNQ',
				231 : 'ETH',
				232 : 'ERI',
				233 : 'EST',
				238 : 'FLK',
				242 : 'FJI',
				246 : 'FIN',
				251 : 'FRA',
				258 : 'PYF',
				260 : 'ATF',
				262 : 'DJI',
				266 : 'GAB',
				268 : 'GEO',
				270 : 'GMB',
				276 : 'DEU',
				288 : 'GHA',
				292 : 'GIB',
				296 : 'KIR',
				300 : 'GRC',
				304 : 'GRL',
				308 : 'GRD',
				316 : 'GUM',
				320 : 'GTM',
				324 : 'GIN',
				328 : 'GUY',
				332 : 'HTI',
				340 : 'HND',
				344 : 'HKG',
				348 : 'HUN',
				352 : 'ISL',
				360 : 'IDN',
				364 : 'IRN',
				368 : 'IRQ',
				372 : 'IRL',
				376 : 'ISR',
				381 : 'ITA',
				384 : 'CIV',
				388 : 'JAM',
				392 : 'JPN',
				398 : 'KAZ',
				400 : 'JOR',
				404 : 'KEN',
				408 : 'PRK',
				410 : 'KOR',
				414 : 'KWT',
				417 : 'KGZ',
				418 : 'LAO',
				422 : 'LBN',
				428 : 'LVA',
				430 : 'LBR',
				434 : 'LBY',
				440 : 'LTU',
				446 : 'MAC',
				450 : 'MDG',
				454 : 'MWI',
				458 : 'MYS',
				462 : 'MDV',
				466 : 'MLI',
				470 : 'MLT',
				478 : 'MRT',
				480 : 'MUS',
				484 : 'MEX',
				490 : 'TWN',
				496 : 'MNG',
				498 : 'MDA',
				499 : 'MNE', 				#Manually Added 29/03/2015
				500 : 'MSR',
				504 : 'MAR',
				508 : 'MOZ',
				512 : 'OMN',
				520 : 'NRU',
				524 : 'NPL',
				528 : 'NLD',
				530 : 'ANT',
				531 : 'CUW', 				#Manually Added 29/03/2015
				533 : 'ABW',
				536 : 'NTZ',
				540 : 'NCL',
				548 : 'VUT',
				554 : 'NZL',
				558 : 'NIC',
				562 : 'NER',
				566 : 'NGA',
				570 : 'NIU',
				574 : 'NFK',
				579 : 'NOR',
				580 : 'MNP',
				583 : 'FSM',
				584 : 'MHL',
				585 : 'PLW',
				586 : 'PAK',
				591 : 'PAN',
				598 : 'PNG',
				600 : 'PRY',
				604 : 'PER',
				608 : 'PHL',
				612 : 'PCN',
				616 : 'POL',
				620 : 'PRT',
				624 : 'GNB',
				626 : 'TMP',
				634 : 'QAT',
				642 : 'ROM',
				643 : 'RUS',
				646 : 'RWA',
				654 : 'SHN',
				659 : 'KNA',
				660 : 'AIA',
				662 : 'LCA',
				666 : 'SPM',
				670 : 'VCT',
				674 : 'SMR',
				678 : 'STP',
				682 : 'SAU',
				686 : 'SEN',
				688 : 'SRB', 				#Manually Added 29/03/2015
				690 : 'SYC',
				694 : 'SLE',
				699 : 'IND',
				702 : 'SGP',
				703 : 'SVK',
				704 : 'VNM',
				705 : 'SVN',
				706 : 'SOM',
				711 : 'ZAF',
				716 : 'ZWE',
				724 : 'ESP',
				728 : 'SSD', 	 			#Manually Added 29/03/2015
   				729 : 'SDN', 				#Manually Added 29/03/2015
				732 : 'ESH',
				736 : 'SDN',
				740 : 'SUR',
				752 : 'SWE',
				757 : 'CHE',
				760 : 'SYR',
				762 : 'TJK',
				764 : 'THA',
				768 : 'TGO',
				772 : 'TKL',
				776 : 'TON',
				780 : 'TTO',
				784 : 'ARE',
				788 : 'TUN',
				792 : 'TUR',
				795 : 'TKM',
				796 : 'TCA',
				798 : 'TUV',
				800 : 'UGA',
				804 : 'UKR',
				807 : 'MKD', 				#Manually Added 29/03/2015				
				818 : 'EGY',
				826 : 'GBR',
				834 : 'TZA',
				842 : 'USA',
				849 : 'UMI',
				854 : 'BFA',
				858 : 'URY',
				860 : 'UZB',
				862 : 'VEN',
				876 : 'WLF',
				882 : 'WSM',
				887 : 'YEM',
				891 : 'YUG',
				894 : 'ZMB',
}
