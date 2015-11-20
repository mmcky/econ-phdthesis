"""
Concordance for countryname to iso3n
Class: <class 'pyeconlab.trade.dataset.NBERFeenstraWTF.constructor.NBERFeenstraWTFConstructor'>
Years: xrange(1962, 2001)
Complete Dataset: True
Source Last Checked: 2014-07-04

Manual Check: 	05/08/2014

Updates
-------
[1] 'China HK SAR' : '156' 	=> 'China HK SAR' : '344'
[2] 'China MC SAR' : '156' 	=> 'China MC SAR' : '446'
[3] 'Fm Yemen Dm'  : '887' 	=> 'Fm Yemen Dm'  : '720'
[4] 'Fm Yemen Ar'  : '887'	=> 'Fm Yemen Ar'  : '886'
[5] 'Fm Yemen AR'  : '887' 	=> 'Fm Yemen AR'  : '886'
[4] 'Fm Yugoslav'  : '.' 	=> 'Fm Yugoslav'  : '890'
[5] 'Fr.Guiana'    : '226' 	=> 'Fr.Guiana' 	  : '254'
[7] 'Korea D P Rp' : '410' 	=> 'Korea D P Rp' : '408'
[8] 'Neth.Ant.Aru' : '.'	=> 'Neth.Ant.Aru' : '532'
[9] 'New Calednia' : '.'  	=> 'New Calednia' : '540'
[10] 'St.Kt-Nev-An' : '.' 	=> 'St.Kt-Nev-An' : '658'
[11] 'St.Pierre Mq' : '.'	=> 'St.Pierre Mq' : '666'
[13] 'Untd Arab Em' : '.'	=> 'Untd Arab Em' : '784'
[14] 'Yugoslavia'	: '.'	=> 'Yugoslavia'	  : '891'

Check
-----
[1] 'Br.Antr.Terr'		
[2] 'China SC'
[3] 'Czechoslovak'
[4] 'Dominican Rp'
[7] 'Occ.Pal.Terr' 
[8] 'Russian Fed'
[9] 'Switz.Liecht'
[10] 'TFYR Macedna'	

Resolved
--------


Notes
-----
[1] Should these iso3n codes be integers?
"""

countryname_to_iso3n = {
				'Afghanistan' 	: '4.0',
				'Afr.Other NS' 	: '.',
				'Africa N.NES' 	: '.',
				'Albania' 		: '8.0',
				'Algeria' 		: '12.0',
				'Angola' 		: '24.0',
				'Areas NES' 	: '.',
				'Argentina' 	: '32.0',
				'Armenia' 		: '51.0',
				'Asia NES' 		: '.',
				'Asia West NS' 	: '.',
				'Australia' 	: '36.0',
				'Austria' 		: '40.0',
				'Azerbaijan' 	: '31.0',
				'Bahamas' 		: '44.0',
				'Bahrain' 		: '48.0',
				'Bangladesh' 	: '50.0',
				'Barbados' 		: '52.0',
				'Belarus' 		: '112.0',
				'Belgium-Lux' 	: '56.0',
				'Belize' 		: '84.0',
				'Benin' 		: '204.0',
				'Bermuda' 		: '60.0',
				'Bolivia' 		: '68.0',
				'Bosnia Herzg' 	: '70.0',
				'Br.Antr.Terr' 	: '.', 			#Check
				'Brazil' 		: '76.0',
				'Bulgaria' 		: '100.0',
				'Burkina Faso' 	: '854.0',
				'Burundi' 		: '108.0',
				'CACM NES' 		: '.',
				'Cambodia' 		: '116.0',
				'Cameroon' 		: '120.0',
				'Canada' 		: '124.0',
				'Carib. NES' 	: '.',
				'Cent.Afr.Rep' 	: '140.0',
				'Chad' 			: '148.0',
				'Chile'			: '152.0',
				'China' 		: '156.0',
				'China FTZ' 	: '156.0',
				'China HK SAR' 	: '344.0',
				'China MC SAR' 	: '446.0',
				'China SC' 		: '156.0', 		#Check
				'Colombia' 		: '170.0',
				'Congo' 		: '178.0',
				'Costa Rica' 	: '188.0',
				'Cote Divoire' 	: '384.0',
				'Croatia' 		: '191.0',
				'Cuba' 			: '192.0',
				'Cyprus' 		: '196.0',
				'Czech Rep' 	: '203.0',
				'Czechoslovak' 	: '.', 			#Check
				'Dem.Rp.Congo' 	: '.',
				'Denmark' 		: '208.0',
				'Djibouti' 		: '262.0',
				'Dominican Rp' 	: '.', 			#Check
				'E Europe NES' 	: '.',
				'EEC NES' 		: '.',
				'Ecuador' 		: '218.0',
				'Egypt' 		: '818.0',
				'El Salvador' 	: '222.0',
				'Eq.Guinea' 	: '226.0',
				'Estonia' 		: '233.0',
				'Ethiopia' 		: '231.0',
				'Eur. EFTA NS' 	: '.',
				'Eur.Other NE' 	: '.',
				'Falkland Is' 	: '238.0',
				'Fiji' 			: '242.0',
				'Finland' 		: '246.0',
				'Fm German DR' 	: '.',
				'Fm German FR' 	: '.',
				'Fm USSR' 		: '643.0',
				'Fm Yemen AR' 	: '886.0', 		
				'Fm Yemen Ar' 	: '886.0',		
				'Fm Yemen Dm' 	: '720.0',
				'Fm Yugoslav' 	: '890.0',
				'Fr Ind O' 		: '.',
				'Fr.Guiana' 	: '.',
				'France,Monac' 	: '250.0',
				'Gabon' 		: '266.0',
				'Gambia' 		: '270.0',
				'Georgia' 		: '268.0',
				'Germany' 		: '276.0',
				'Ghana' 		: '288.0',
				'Gibraltar' 	: '292.0',
				'Greece' 		: '300.0',
				'Greenland' 	: '304.0',
				'Guadeloupe' 	: '312.0',
				'Guatemala' 	: '320.0',
				'Guinea' 		: '324.0',
				'GuineaBissau' 	: '624.0',
				'Guyana' 		: '328.0',
				'Haiti' 		: '332.0',
				'Honduras' 		: '340.0',
				'Hungary' 		: '348.0',
				'Iceland' 		: '352.0',
				'India' 		: '356.0',
				'Indonesia' 	: '360.0',
				'Int Org' 		: '.',
				'Iran' 			: '364.0',
				'Iraq' 			: '368.0',
				'Ireland' 		: '372.0',
				'Israel' 		: '376.0',
				'Italy' 		: '380.0',
				'Jamaica' 		: '388.0',
				'Japan' 		: '392.0',
				'Jordan' 		: '400.0',
				'Kazakhstan' 	: '398.0',
				'Kenya' 		: '404.0',
				'Kiribati' 		: '296.0',
				'Korea D P Rp' 	: '408.0',
				'Korea Rep.' 	: '410.0',
				'Kuwait' 		: '414.0',
				'Kyrgyzstan' 	: '417.0',
				'LAIA NES' 		: '.',
				'Lao P.Dem.R' 	: '418.0',
				'Latvia' 		: '428.0',
				'Lebanon' 		: '422.0',
				'Liberia' 		: '430.0',
				'Libya' 		: '434.0',
				'Lithuania' 	: '440.0',
				'Madagascar' 	: '450.0',
				'Malawi' 		: '454.0',
				'Malaysia'		: '458.0',
				'Mali' 			: '466.0',
				'Malta' 		: '470.0',
				'Mauritania' 	: '478.0',
				'Mauritius' 	: '480.0',
				'Mexico' 		: '484.0',
				'Mongolia' 		: '496.0',
				'Morocco' 		: '504.0',
				'Mozambique' 	: '508.0',
				'Myanmar' 		: '104.0',
				'Nepal' 		: '524.0',
				'Neth.Ant.Aru' 	: '532.0', 		
				'Netherlands' 	: '528.0',
				'Neutral Zone' 	: '.',
				'New Calednia' 	: '540.0',
				'New Zealand' 	: '554.0',
				'Nicaragua' 	: '558.0',
				'Niger' 		: '562.0',
				'Nigeria' 		: '566.0',
				'Norway' 		: '578.0',
				'Occ.Pal.Terr' 	: '.', 			#Check
				'Oman' 			: '512.0',
				'Oth.Oceania' 	: '.',
				'Pakistan' 		: '586.0',
				'Panama' 		: '591.0',
				'Papua N.Guin' 	: '598.0',
				'Paraguay' 		: '600.0',
				'Peru' 			: '604.0',
				'Philippines' 	: '608.0',
				'Poland' 		: '616.0',
				'Portugal' 		: '620.0',
				'Qatar' 		: '634.0',
				'Rep Moldova' 	: '498.0',
				'Romania' 		: '642.0',
				'Russian Fed' 	: '643.0', 		#Check
				'Rwanda' 		: '646.0',
				'Samoa' 		: '882.0',
				'Saudi Arabia' 	: '682.0',
				'Senegal' 		: '686.0',
				'Seychelles' 	: '690.0',
				'Sierra Leone' 	: '694.0',
				'Singapore' 	: '702.0',
				'Slovakia' 		: '703.0',
				'Slovenia' 		: '705.0',
				'Somalia' 		: '706.0',
				'South Africa' 	: '710.0',
				'Spain' 		: '724.0',
				'Sri Lanka' 	: '144.0',
				'St.Helena' 	: '654.0',
				'St.Kt-Nev-An' 	: '658.0',
				'St.Pierre Mq' 	: '666.0',
				'Sudan' 		: '736.0',
				'Suriname' 		: '740.0',
				'Sweden' 		: '752.0',
				'Switz.Liecht' 	: '.', 			#Check
				'Syria' 		: '760.0',
				'TFYR Macedna' 	: '.', 			#Check
				'Taiwan' 		: '158.0',
				'Tajikistan' 	: '762.0',
				'Tanzania' 		: '834.0',
				'Thailand' 		: '764.0',
				'Togo' 			: '768.0',
				'Trinidad Tbg' 	: '780.0',
				'Tunisia' 		: '788.0',
				'Turkey' 		: '792.0',
				'Turkmenistan' 	: '795.0',
				'UK' 			: '826.0',
				'US NES' 		: '.',
				'USA' 			: '840.0',
				'Uganda' 		: '800.0',
				'Ukraine' 		: '804.0',
				'Untd Arab Em' 	: '784.0',
				'Uruguay' 		: '858.0',
				'Uzbekistan' 	: '860.0',
				'Venezuela' 	: '862.0',
				'Viet Nam' 		: '704.0',
				'World' 		: '.',
				'Yemen' 		: '887.0',
				'Yugoslavia' 	: '891.0',
				'Zambia' 		: '894.0',
				'Zimbabwe' 		: '716.0',
}
