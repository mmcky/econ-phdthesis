"""
ISO3N to CountryName Dictionary for Classification: HS96

Note: This should NOT be used to filter for countries as this dictionary contains NES and other names

Manual Check: 19/03/2015 [Spelling Check]

Changes
-------
Corrections
~~~~~~~~~~~
384 : 'Cte d\'Ivoire' 	=> 	384 : 'Cote d\'Ivoire'
642 : 'Roumania' 		=> 	642 : 'Romania'
807 : 'The former Yugoslav Rep. of Macedonia' => 807 : 'Macedonia, the former Yugoslav Republic of',


Additions
~~~~~~~~~
499 : 'Montenegro',
531 : 'Curaçao',
688 : 'Serbia',
728 : 'South Sudan',
729 : 'Sudan',

Notes
-----
[1] See hs96_iso3n_to_is3c.py for manual additions

"""

iso3n_to_name = {
				4 : 'Afghanistan',
				8 : 'Albania',
				10 : 'Antarctica',
				12 : 'Algeria',
				16 : 'American Samoa',
				20 : 'Andorra',
				24 : 'Angola',
				28 : 'Antigua and Barbuda',
				31 : 'Azerbaijan',
				32 : 'Argentina',
				36 : 'Australia',
				40 : 'Austria',
				44 : 'Bahamas',
				48 : 'Bahrain',
				50 : 'Bangladesh',
				51 : 'Armenia',
				52 : 'Barbados',
				58 : 'Belgium-Luxembourg',
				60 : 'Bermuda',
				64 : 'Bhutan',
				68 : 'Bolivia',
				70 : 'Bosnia and Herzegovina',
				74 : 'Bouvet Island',
				76 : 'Brazil',
				80 : 'British Antarctic Territory',
				84 : 'Belize',
				86 : 'British Indian Ocean Territory',
				90 : 'Solomon Islands',
				92 : 'British Virgin Islands',
				96 : 'Brunei Darussalam',
				100 : 'Bulgaria',
				104 : 'Myanmar',
				108 : 'Burundi',
				112 : 'Belarus',
				116 : 'Cambodia',
				120 : 'Cameroon',
				124 : 'Canada',
				129 : 'Caribbean Nes',
				132 : 'Cape Verde',
				136 : 'Cayman Islands',
				140 : 'Central African Republic',
				144 : 'Sri Lanka',
				148 : 'Chad',
				152 : 'Chile',
				156 : 'China',
				162 : 'Christmas Island',
				166 : 'Cocos (Keeling) Islands',
				170 : 'Colombia',
				174 : 'Comoros',
				178 : 'Congo',
				180 : 'Democratic Republic of the Congo',
				184 : 'Cook Islands',
				188 : 'Costa Rica',
				191 : 'Croatia',
				192 : 'Cuba',
				196 : 'Cyprus',
				203 : 'Czech Republic',
				204 : 'Benin',
				208 : 'Denmark',
				212 : 'Dominica',
				214 : 'Dominican Republic',
				218 : 'Ecuador',
				222 : 'El Salvador',
				226 : 'Equatorial Guinea',
				231 : 'Ethiopia',
				232 : 'Eritrea',
				233 : 'Estonia',
				238 : 'Falkland Islands (Malvinas)',
				239 : 'South Georgia and the South Sandwich Island',
				242 : 'Fiji',
				246 : 'Finland',
				251 : 'France',
				258 : 'French Polynesia',
				260 : 'French Southern Antartic territories',
				262 : 'Djibouti',
				266 : 'Gabon',
				268 : 'Georgia',
				270 : 'Gambia',
				275 : 'State of Palestine',
				276 : 'Germany',
				288 : 'Ghana',
				290 : 'Northern Africa, nes',
				292 : 'Gibraltar',
				296 : 'Kiribati',
				300 : 'Greece',
				304 : 'Greenland',
				308 : 'Grenada',
				316 : 'Guam',
				320 : 'Guatemala',
				324 : 'Guinea',
				328 : 'Guyana',
				332 : 'Haiti',
				334 : 'Heard Island and McDonald Islands',
				336 : 'Holy See (Vatican City State)',
				340 : 'Honduras',
				344 : 'Hong Kong (SARC)',
				348 : 'Hungary',
				352 : 'Iceland',
				360 : 'Indonesia',
				364 : 'Iran (Islamic Republic of)',
				368 : 'Iraq',
				372 : 'Ireland',
				376 : 'Israel',
				381 : 'Italy',
				384 : 'Cote d\'Ivoire',
				388 : 'Jamaica',
				392 : 'Japan',
				398 : 'Kazakstan',
				400 : 'Jordan',
				404 : 'Kenya',
				408 : 'Korea, Dem. People\'s Rep. of',
				410 : 'Korea, Rep. of Korea',
				414 : 'Kuwait',
				417 : 'Kyrgyzstan',
				418 : 'Lao People\'s Democratic Republic',
				422 : 'Lebanon',
				428 : 'Latvia',
				430 : 'Liberia',
				434 : 'Libyan Arab Jamahiriya',
				440 : 'Lithuania',
				446 : 'Macau',
				450 : 'Madagascar',
				454 : 'Malawi',
				458 : 'Malaysia',
				462 : 'Maldives',
				466 : 'Mali',
				470 : 'Malta',
				471 : 'Centr.Amer.Com.Market (CACM) Nes',
				473 : 'LAIA, nes',
				478 : 'Mauritania',
				480 : 'Mauritius',
				484 : 'Mexico',
				490 : 'Taiwan, Province of (China)',
				492 : 'European Union Nes',
				496 : 'Mongolia',
				498 : 'Moldova, Rep.of',
				499 : 'Montenegro', 								#Manually Added 29/03/2015
				500 : 'Montserrat',
				504 : 'Morocco',
				508 : 'Mozambique',
				512 : 'Oman',
				520 : 'Nauru',
				524 : 'Nepal',
				527 : 'Oceania Nes',
				528 : 'Netherlands',
				530 : 'Netherland Antilles',
				531 : 'Curaçao', 									#Manually Added 29/03/2015
				533 : 'Aruba',
				536 : 'Neutral Zone',
				540 : 'New Caledonia',
				548 : 'Vanuatu',
				554 : 'New Zealand',
				558 : 'Nicaragua',
				562 : 'Niger',
				566 : 'Nigeria',
				568 : 'Europe Othr. Nes',
				570 : 'Niue',
				574 : 'Norfolk Island',
				577 : 'Afr. Other Nes',
				579 : 'Norway',
				580 : 'Northern Mariana Islands',
				581 : 'United States Minor Outlying Islands',
				583 : 'Micronesia (Federated States of)',
				584 : 'Marshall Islands',
				585 : 'Palau',
				586 : 'Pakistan',
				591 : 'Panama',
				598 : 'Papua New Guinea',
				600 : 'Paraguay',
				604 : 'Peru',
				608 : 'Philippines',
				612 : 'Pitcairn',
				616 : 'Poland',
				620 : 'Portugal',
				624 : 'Guinea-Bissau',
				626 : 'East Timor',
				634 : 'Qatar',
				636 : 'Rest of America, nes',
				637 : 'North America and Central America, nes',
				642 : 'Romania',
				643 : 'Russian Federation',
				646 : 'Rwanda',
				654 : 'Saint Helena',
				659 : 'Saint Kitts and Nevis',
				660 : 'Anguilla',
				662 : 'Saint Lucia',
				666 : 'St. Pierre and Miquelon',
				670 : 'Saint Vincent and the Grenadines',
				674 : 'San Marino',
				678 : 'Sao Tome and Principe',
				682 : 'Saudi Arabia',
				686 : 'Senegal',
				688 : 'Serbia', 									#Manually Added 29/03/2015
				690 : 'Seychelles',
				694 : 'Sierra Leone',
				699 : 'India',
				702 : 'Singapore',
				703 : 'Slovakia',
				704 : 'Viet Nam',
				705 : 'Slovenia',
				706 : 'Somalia',
				711 : 'South Africa',
				716 : 'Zimbabwe',
				724 : 'Spain',
				728 : 'South Sudan', 								#Manually Added 29/03/2015
				729 : 'Sudan',										#Manually Added 29/03/2015
				732 : 'Western Sahara',
				736 : 'Sudan',
				740 : 'Suriname',
				752 : 'Sweden',
				757 : 'Switzerland',
				760 : 'Syrian Arab Republic',
				762 : 'Tajikistan',
				764 : 'Thailand',
				768 : 'Togo',
				772 : 'Tokelau',
				776 : 'Tonga',
				780 : 'Trinidad and Tobago',
				784 : 'United Arab Emirates',
				788 : 'Tunisia',
				792 : 'Turkey',
				795 : 'Turkmenistan',
				796 : 'Turks and Caicos Islands',
				798 : 'Tuvalu',
				800 : 'Uganda',
				804 : 'Ukraine',
				807 : 'Macedonia, the former Yugoslav Republic of', 	#Manually Updated 29/03/2015
				818 : 'Egypt',
				826 : 'United Kingdom',
				834 : 'Tanzania, United Rep. of',
				837 : 'SHIP STORES AND BUNKERS',
				838 : 'Free Zones',
				839 : 'SPECIAL CATEGORIES',
				842 : 'United States of America',
				849 : 'United States Minor Outlying Islands',
				854 : 'Burkina Faso',
				858 : 'Uruguay',
				860 : 'Uzbekistan',
				862 : 'Venezuela',
				876 : 'Wallis and Futuna',
				879 : 'Western Asia, nes',
				882 : 'Samoa',
				887 : 'Yemen',
				891 : 'Yugoslavia',
				894 : 'Zambia',
				899 : 'Areas, nes',
}
