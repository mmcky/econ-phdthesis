"""
Concordance for countryname to iso3c
Class: <class 'pyeconlab.trade.dataset.NBERFeenstraWTF.constructor.NBERFeenstraWTFConstructor'>
Years: xrange(1962, 2001)
Complete Dataset: True
Source Last Checked: 2014-07-04

Manual Check: 	27/08/2014

Updates (June-2015)
-------------------
[6] 'France,Monac' : 'FRA' 	=> 'France,Monac' : 'FRA'

Updates
-------
[1] 'China HK SAR' : 'CHN' 	=> 'China HK SAR' : 'HKG'
[2] 'China MC SAR' : 'CHN' 	=> 'China MC SAR' : 'MAC'
[3] 'Fm Yemen Dm'  : 'YEM' 	=> 'Fm Yemen Dm'  : 'YMD'
[4] 'Fm Yugoslav'  : '.' 	=> 'Fm Yugoslav'  : 'YUG'
[5] 'Fr.Guiana'    : '.' 	=> 'Fr.Guiana' 	  : 'GUF'
[6] 'France,Monac' : 'MCO' 	=> 'France,Monac' : 'MCO'
[7] 'Korea D P Rp' : 'KOR' 	=> 'Korea D P Rp' : 'PRK'
[8] 'Neth.Ant.Aru' : '.'	=> 'Neth.Ant.Aru' : 'ANT'
[9] 'New Calednia' : '.'  	=> 'New Calednia' : 'NCL'
[10] 'St.Kt-Nev-An' : '.' 	=> 'St.Kt-Nev-An' : 'KNA'
[11] 'St.Pierre Mq' : '.'	=> 'St.Pierre Mq' : 'SPM'
[12] 'TFYR Macedna' : '.'	=> 'TFYR Macedna' : 'MKD' 		
[13] 'Untd Arab Em' : '.'	=> 'Untd Arab Em' : 'ARE'
[14] 'Yugoslavia'	: '.'	=> 'Yugoslavia'	  : 'YUG'


Resolved
--------
[1] 'Fm Yemen Dm'  => ISO3C: 'YMD' Source: http://en.wikipedia.org/wiki/South_Yemen (ISO3N: 720) http://en.wikipedia.org/wiki/ISO_3166-1_numeric
[2] 'Fm Yemen Ar'  => ISO3C: 'YEM' Source: http://en.wikipedia.org/wiki/North_Yemen (ISO3N: 886) http://en.wikipedia.org/wiki/ISO_3166-1_numeric
[3] 'Fm Yemen AR'  => ISO3C: 'YEM' Source: http://en.wikipedia.org/wiki/North_Yemen (ISO3N: 886) http://en.wikipedia.org/wiki/ISO_3166-1_numeric
[4] 'China SC' 	   => ISO3C: 'CHN' 
[5] 'Br.Antr.Terr' => ISO3C: 'ATB' Source: http://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
[6] 'Czechoslovak' => ISO3C: 'CSK' Source: http://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
[7] 'Dominican Rp' => ISO3C: 'DOM' Source: http://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
[8] 'Occ.Pal.Terr' => ISO3C: 'OPT' Source: http://en.wikipedia.org/wiki/Palestinian_territories
[9] 'Russian Fed'  => ISO3C: 'RUS' Source: http://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
[10] 'Switz.Liecht' => ISO3C: 'CHE'
[11] 'Fm German DR' => ISO3C: 'DDR'
[12] 'Fm German FR' => ISO3C: 'DEU'

"""

countryname_to_iso3c = {
				'Afghanistan' 	: 'AFG',
				'Afr.Other NS' 	: '.', 		#Drop
				'Africa N.NES' 	: '.', 		#Drop
				'Albania' 		: 'ALB',
				'Algeria' 		: 'DZA',
				'Angola' 		: 'AGO',
				'Areas NES' 	: '.', 		#Drop
				'Argentina' 	: 'ARG',
				'Armenia' 		: 'ARM',
				'Asia NES' 		: '.', 		#Drop
				'Asia West NS' 	: '.', 		#Drop
				'Australia' 	: 'AUS',
				'Austria' 		: 'AUT',
				'Azerbaijan' 	: 'AZE',
				'Bahamas' 		: 'BHS',
				'Bahrain' 		: 'BHR',
				'Bangladesh' 	: 'BGD',
				'Barbados' 		: 'BRB',
				'Belarus' 		: 'BLR',
				'Belgium-Lux' 	: 'BEL',
				'Belize' 		: 'BLZ',
				'Benin' 		: 'BEN',
				'Bermuda' 		: 'BMU',
				'Bolivia' 		: 'BOL',
				'Bosnia Herzg' 	: 'BIH',
				'Br.Antr.Terr' 	: 'ATB', 
				'Brazil' 		: 'BRA',
				'Bulgaria' 		: 'BGR',
				'Burkina Faso' 	: 'BFA',
				'Burundi' 		: 'BDI',
				'CACM NES' 		: '.', 		#Drop
				'Cambodia' 		: 'KHM',
				'Cameroon' 		: 'CMR',
				'Canada' 		: 'CAN',
				'Carib. NES' 	: '.',		#Drop
				'Cent.Afr.Rep' 	: 'CAF',
				'Chad' 			: 'TCD',
				'Chile' 		: 'CHL',
				'China' 		: 'CHN',
				'China FTZ' 	: 'CHN',
				'China HK SAR' 	: 'HKG',
				'China MC SAR' 	: 'MAC',
				'China SC' 		: 'CHN', 	
				'Colombia' 		: 'COL',
				'Congo' 		: 'COG',
				'Costa Rica' 	: 'CRI',
				'Cote Divoire' 	: 'CIV',
				'Croatia' 		: 'HRV',
				'Cuba' 			: 'CUB',
				'Cyprus' 		: 'CYP',
				'Czech Rep' 	: 'CZE',
				'Czechoslovak' 	: 'CSK', 		
				'Dem.Rp.Congo' 	: 'COD',
				'Denmark' 		: 'DNK',
				'Djibouti' 		: 'DJI',
				'Dominican Rp' 	: 'DOM', 		
				'E Europe NES' 	: '.', 		#Drop
				'EEC NES' 		: '.', 		#Drop
				'Ecuador' 		: 'ECU',
				'Egypt' 		: 'EGY',
				'El Salvador' 	: 'SLV',
				'Eq.Guinea' 	: 'GNQ',
				'Estonia' 		: 'EST',
				'Ethiopia' 		: 'ETH',
				'Eur. EFTA NS' 	: '.', 		#Drop
				'Eur.Other NE' 	: '.', 		#Drop
				'Falkland Is' 	: 'FLK',
				'Fiji' 			: 'FJI',
				'Finland' 		: 'FIN',
				'Fm German DR' 	: 'DDR',
				'Fm German FR' 	: 'DEU',
				'Fm USSR' 		: 'RUS',
				'Fm Yemen AR' 	: 'YEM', 	
				'Fm Yemen Ar' 	: 'YEM',	
				'Fm Yemen Dm' 	: 'YMD', 	#Changed from YMYD to YMD
				'Fm Yugoslav' 	: 'YUG',
				'Fr Ind O' 		: '.', 		#Check
				'Fr.Guiana' 	: 'GUF',
				'France,Monac' 	: 'FRA', 	#Updated from MCO [BUGFIX]
				'Gabon' 		: 'GAB',
				'Gambia' 		: 'GMB',
				'Georgia' 		: 'GEO',
				'Germany' 		: 'DEU',
				'Ghana' 		: 'GHA',
				'Gibraltar' 	: 'GIB',
				'Greece' 		: 'GRC',
				'Greenland' 	: 'GRL',
				'Guadeloupe' 	: 'GLP',
				'Guatemala' 	: 'GTM',
				'Guinea' 		: 'GIN',
				'GuineaBissau' 	: 'GNB',
				'Guyana' 		: 'GUY',
				'Haiti' 		: 'HTI',
				'Honduras' 		: 'HND',
				'Hungary' 		: 'HUN',
				'Iceland' 		: 'ISL',
				'India' 		: 'IND',
				'Indonesia' 	: 'IDN',
				'Int Org' 		: '.', 		#Drop
				'Iran' 			: 'IRN',
				'Iraq' 			: 'IRQ',
				'Ireland' 		: 'IRL',
				'Israel' 		: 'ISR',
				'Italy' 		: 'ITA',
				'Jamaica' 		: 'JAM',
				'Japan' 		: 'JPN',
				'Jordan' 		: 'JOR',
				'Kazakhstan' 	: 'KAZ',
				'Kenya' 		: 'KEN',
				'Kiribati' 		: 'KIR',
				'Korea D P Rp' 	: 'PRK',
				'Korea Rep.' 	: 'KOR',
				'Kuwait' 		: 'KWT',
				'Kyrgyzstan' 	: 'KGZ',
				'LAIA NES' 		: '.', 		#Drop
				'Lao P.Dem.R' 	: 'LAO',
				'Latvia' 		: 'LVA',
				'Lebanon' 		: 'LBN',
				'Liberia' 		: 'LBR',
				'Libya' 		: 'LBY',
				'Lithuania' 	: 'LTU',
				'Madagascar' 	: 'MDG',
				'Malawi' 		: 'MWI',
				'Malaysia' 		: 'MYS',
				'Mali' 			: 'MLI',
				'Malta' 		: 'MLT',
				'Mauritania' 	: 'MRT',
				'Mauritius' 	: 'MUS',
				'Mexico' 		: 'MEX',
				'Mongolia' 		: 'MNG',
				'Morocco' 		: 'MAR',
				'Mozambique' 	: 'MOZ',
				'Myanmar' 		: 'MMR',
				'Nepal' 		: 'NPL',
				'Neth.Ant.Aru' 	: 'ANT',
				'Netherlands' 	: 'NLD',
				'Neutral Zone' 	: '.', 		#Drop
				'New Calednia' 	: 'NCL',
				'New Zealand' 	: 'NZL',
				'Nicaragua' 	: 'NIC',
				'Niger' 		: 'NER',
				'Nigeria' 		: 'NGA',
				'Norway' 		: 'NOR',
				'Occ.Pal.Terr' 	: 'OPT',
				'Oman' 			: 'OMN',
				'Oth.Oceania' 	: '.', 		#Drop
				'Pakistan' 		: 'PAK',
				'Panama' 		: 'PAN',
				'Papua N.Guin' 	: 'PNG',
				'Paraguay' 		: 'PRY',
				'Peru' 			: 'PER',
				'Philippines' 	: 'PHL',
				'Poland' 		: 'POL',
				'Portugal' 		: 'PRT',
				'Qatar' 		: 'QAT',
				'Rep Moldova'	: 'MDA',
				'Romania' 		: 'ROU',
				'Russian Fed' 	: 'RUS', 		
				'Rwanda' 		: 'RWA',
				'Samoa' 		: 'WSM',
				'Saudi Arabia' 	: 'SAU',
				'Senegal' 		: 'SEN',
				'Seychelles' 	: 'SYC',
				'Sierra Leone'	: 'SLE',
				'Singapore' 	: 'SGP',
				'Slovakia' 		: 'SVK',
				'Slovenia' 		: 'SVN',
				'Somalia' 		: 'SOM',
				'South Africa' 	: 'ZAF',
				'Spain' 		: 'ESP',
				'Sri Lanka' 	: 'LKA',
				'St.Helena' 	: 'SHN',
				'St.Kt-Nev-An' 	: 'KNA',
				'St.Pierre Mq' 	: 'SPM',
				'Sudan' 		: 'SDN',
				'Suriname' 		: 'SUR',
				'Sweden' 		: 'SWE',
				'Switz.Liecht' 	: 'CHE', 			#Check
				'Syria' 		: 'SYR',
				'TFYR Macedna' 	: 'MKD',
				'Taiwan' 		: 'TWN',
				'Tajikistan' 	: 'TJK',
				'Tanzania' 		: 'TZA',
				'Thailand' 		: 'THA',
				'Togo' 			: 'TGO',
				'Trinidad Tbg' 	: 'TTO',
				'Tunisia' 		: 'TUN',
				'Turkey' 		: 'TUR',
				'Turkmenistan' 	: 'TKM',
				'UK' 			: 'GBR',
				'US NES' 		: '.', 		#Drop
				'USA' 			: 'USA',
				'Uganda' 		: 'UGA',
				'Ukraine' 		: 'UKR',
				'Untd Arab Em' 	: 'ARE',
				'Uruguay' 		: 'URY',
				'Uzbekistan' 	: 'UZB',
				'Venezuela' 	: 'VEN',
				'Viet Nam' 		: 'VNM',
				'World' 		: '.', 		#Should this be WLD? Current Decision is to Drop so it is dropped in country matches
				'Yemen' 		: 'YEM',
				'Yugoslavia'	: 'YUG',
				'Zambia' 		: 'ZMB',
				'Zimbabwe' 		: 'ZWE',
}

#-Script for Converting Dictionary to CSV-#

if __name__ == '__main__':
	from pyeconlab.util import from_dict_to_csv
	from_dict_to_csv(countryname_to_iso3c, fl='countryname_to_iso3c.csv', target_dir='csv/', header=['countryname', 'iso3c'])