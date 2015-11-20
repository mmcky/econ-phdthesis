***********************************************************************
*** Generate Test Data using STATA for PyEconLab NBERWTF Validation ***
*** --------------------------------------------------------------- ***
*** This File is to Produce General Tests Data to Support Validation **
***********************************************************************

if c(os) == "MacOSX" {
	global SOURCE_DIR="~/work-data/datasets/36a376e5a01385782112519bddfac85e/"
	global TARGET_DIR="~/work-temp/"
}

if c(os) == "Windows" {
	global SOURCE_DIR="D:/work-data/datasets/36a376e5a01385782112519bddfac85e/"
	global TARGET_DIR="D:/work-temp/"
}

*----------*
*-Settings-*
*----------*

set more off

** Uncomment this for sending output to the testing directory **
** Otherwise this will run from the current directory and can be used for updating the tests data **
//cd $TARGET_DIR
pwd

*** ---> START <---- ***

use "$SOURCE_DIR/wtf62.dta", clear
foreach year of num 63(1)99 {
	append using "$SOURCE_DIR/wtf`year'.dta"
}
append using "$SOURCE_DIR/wtf00.dta"
//save "$SOURCE_DIR/wtf.dta", replace

format value %12.0f

**-----------------------**
** World Level Test Data **
**~~~~~~~~~~~~~~~~~~~~~~~**

** Time Series of World Exports 
** Filename: stata_wtf62-00_WLD_total_export
preserve
keep if exporter == "World"
keep if importer == "World"
collapse (sum) value, by(year exporter)
gen eiso3c = "WLD"
outsheet using "stata_wtf62-00_WLD_total_export.csv", comma nolabel replace
restore

** Time Series of World Imports 
** Filename: stata_wtf62-00_WLD_total_import
preserve
keep if exporter == "World"
keep if importer == "World"
collapse (sum) value, by(year importer)
gen eiso3c = "WLD"
outsheet using "stata_wtf62-00_WLD_total_import.csv", comma nolabel replace
restore


**-----------------------**
**Country Level Test Data**
**~~~~~~~~~~~~~~~~~~~~~~~**

** Time Series of Total Exports from Country (i) to World
** Filename: stata_wtf62-00_???_total_export.csv
**
preserve
keep if exporter == "USA"
keep if importer == "World"
collapse (sum) value, by(year exporter)
gen eiso3c = "USA"
outsheet using "stata_wtf62-00_USA_total_export.csv", comma nolabel replace
restore

preserve
keep if exporter == "New Zealand"
keep if importer == "World"
collapse (sum) value, by(year exporter)
gen eiso3c = "NZL"
outsheet using "stata_wtf62-00_NZL_total_export.csv", comma nolabel replace
restore

preserve
keep if exporter == "Israel"
keep if importer == "World"
collapse (sum) value, by(year exporter)
gen eiso3c = "ISR"
outsheet using "stata_wtf62-00_ISR_total_export.csv", comma nolabel replace
restore

preserve
keep if exporter == "UK"
keep if importer == "World"
collapse (sum) value, by(year exporter)
gen eiso3c = "GBR"
outsheet using "stata_wtf62-00_GBR_total_export.csv", comma nolabel replace
restore

preserve
keep if exporter == "Taiwan"
keep if importer == "World"
collapse (sum) value, by(year exporter)
gen eiso3c = "TWN"
outsheet using "stata_wtf62-00_TWN_total_export.csv", comma nolabel replace
restore

preserve
keep if exporter == "Switz.Liecht"
keep if importer == "World"
collapse (sum) value, by(year exporter)
gen eiso3c = "CHE"
outsheet using "stata_wtf62-00_CHE_total_export.csv", comma nolabel replace
restore


**-----------------------**
**Product Level Test Data**
**~~~~~~~~~~~~~~~~~~~~~~~**

*** SITC Level 4 ***
*** ~~~~~~~~~~~~ ***

** World Product Exports over Time
** Filename: stata_wtf62-00_sitc4_####_total.csv
** Codes
** ~~~~~
** "0421" -> Rice in the husk or husked
** "8924" -> Piture Postcards
** "7431" -> Air Pumps, Vacume Pumps
** "6517" -> Yarn of Regenerated Fibers

**Official Codes**

** "0421" -> Rice in the husk or husked
preserve
keep if exporter == "World"
keep if importer == "World"
keep if sitc4 == "0421"
collapse (sum) value, by(year)
outsheet using "stata_wtf62-00_sitc4_0421_total.csv", comma nolabel replace
restore

** "8924" -> Piture Postcards
preserve
keep if exporter == "World"
keep if importer == "World"
keep if sitc4 == "6517"
collapse (sum) value, by(year)
outsheet using "stata_wtf62-00_sitc4_6517_total.csv", comma nolabel replace
restore

** "7431" -> Air Pumps, Vacume Pumps
preserve
keep if exporter == "World"
keep if importer == "World"
keep if sitc4 == "7431"
collapse (sum) value, by(year)
outsheet using "stata_wtf62-00_sitc4_7431_total.csv", comma nolabel replace
restore

** "6517" -> Yarn of Regenerated Fibers
preserve
keep if exporter == "World"
keep if importer == "World"
keep if sitc4 == "8924"
collapse (sum) value, by(year)
outsheet using "stata_wtf62-00_sitc4_8924_total.csv", comma nolabel replace
restore

**Unofficial Codes**

preserve
keep if exporter == "World"
keep if importer == "World"
keep if sitc4 == "6540"
collapse (sum) value, by(year)
outsheet using "stata_wtf62-00_sitc4_6540_total.csv", comma nolabel replace
restore

*** SITC Level 3 ***
*** ~~~~~~~~~~~~ ***

** World Product Exports over Time
** Filename: stata_wtf62-00_sitc3_###_total.csv
** Codes
** ~~~~~
** "683" -> Nickel
** "268" -> Wool and other animal hair
** "778" -> Electrical Machinery and Apparatus

**Official Codes**

** "683" -> Nickel
preserve
keep if exporter == "World"
keep if importer == "World"
gen sitc3 = substr(sitc4,1,3)
keep if sitc3 == "683"
collapse (sum) value, by(year)
outsheet using "stata_wtf62-00_sitc3_683_total.csv", comma nolabel replace
restore
 
** "268" -> Wool and other animal hair
preserve
keep if exporter == "World"
keep if importer == "World"
gen sitc3 = substr(sitc4,1,3)
keep if sitc3 == "268"
collapse (sum) value, by(year)
outsheet using "stata_wtf62-00_sitc3_268_total.csv", comma nolabel replace
restore

** "778" -> Electrical Machinery and Apparatus
preserve
keep if exporter == "World"
keep if importer == "World"
gen sitc3 = substr(sitc4,1,3)
keep if sitc3 == "778"
collapse (sum) value, by(year)
outsheet using "stata_wtf62-00_sitc3_778_total.csv", comma nolabel replace
restore


**Unofficial Codes**
// TBD


*** SITC Level 2 ***
*** ~~~~~~~~~~~~ ***

** World Product (p) Exports over Time
** Filename: stata_wtf62-00_sitc3_####_total.csv
** Codes
** ~~~~~
** "04" -> Cereals, and Cereal production
** "21" -> Hides, skins and furskins
** "78" -> Road Vehicles

**Official Codes**

** "04" -> Cereals, and Cereal production
preserve
keep if exporter == "World"
keep if importer == "World"
gen sitc2 = substr(sitc4,1,2)
keep if sitc2 == "04"
collapse (sum) value, by(year)
outsheet using "stata_wtf62-00_sitc2_04_total.csv", comma nolabel replace
restore
 
** "21" -> Hides, skins and furskins
preserve
keep if exporter == "World"
keep if importer == "World"
gen sitc2 = substr(sitc4,1,2)
keep if sitc2 == "21"
collapse (sum) value, by(year)
outsheet using "stata_wtf62-00_sitc2_21_total.csv", comma nolabel replace
restore

** "78" -> Road Vehicles
preserve
keep if exporter == "World"
keep if importer == "World"
gen sitc2 = substr(sitc4,1,2)
keep if sitc2 == "78"
collapse (sum) value, by(year)
outsheet using "stata_wtf62-00_sitc2_78_total.csv", comma nolabel replace
restore

**Unofficial Codes**
// TBD


*** SITC Level 1 ***
*** ~~~~~~~~~~~~ ***

** World Product (p) Exports over Time
** Filename: stata_wtf62-00_sitc3_####_total.csv
** Codes
** ~~~~~
** "0" - Food and live animals
** "1" - Beverages and Tobacco
** "2" - Crude Materials, inedible except fuels
** "3" - Minearal fuels, lubricants
** "4" - Animal and Vegetable Oils
** "5" - Chemicals and related products
** "6" - Manufactured goods classified chiefly by materials
** "7" - Machinery and transport equipment
** "8" - Miscellaneous manufactured articles
** "9" - Commodities and transactions not classified elsewhere in the SITC

**Official Codes**
foreach sitc in "0" "1" "2" "3" "4" "5" "6" "7" "8" "9" {
	di "SITC Code: `sitc'"
	preserve
	keep if exporter == "World"
	keep if importer == "World"
	gen sitc1 = substr(sitc4,1,1)
	keep if sitc1 == "`sitc'"
	collapse (sum) value, by(year)
	outsheet using "stata_wtf62-00_sitc1_`sitc'_total.csv", comma nolabel replace
	restore
}


**-----------------------------------**
** Country x Product Level Test Data **
** Generic Tests of Raw Data 	     **
**-----------------------------------**

** EXPORTS **
**~~~~~~~~~**

use "$SOURCE_DIR/wtf62.dta", clear
foreach year of num 63(1)99 {
	append using "$SOURCE_DIR/wtf`year'.dta"
}
append using "$SOURCE_DIR/wtf00.dta"

format value %12.0f

keep year importer exporter sitc4 value

** SITC Level 4 Data **
** ~~~~~~~~~~~~~~~~~ **

** Country (i) x Product (P) Exports to World over Time
** Filename: stata_wtf62-00_???_sitc4_####_total_export.csv
** Codes
** ~~~~~
** DNK, "0620","Sugar confectionery and preparations, non-chocolate"
** ESP, "8973","Precious jewellery, goldsmiths' or silversmiths' wares"

** Denmark Exports of "0620","Sugar confectionery and preparations, non-chocolate"
preserve
keep if exporter == "Denmark"
keep if importer == "World"
keep if sitc4 == "0620"
collapse (sum) value, by(year sitc4)
gen eiso3c = "DNK"
outsheet using "stata_wtf62-00_DNK_sitc4_0620_total_export.csv", comma nolabel replace
restore

** Spain Exports of "8973","Precious jewellery, goldsmiths' or silversmiths' wares"
preserve
keep if exporter == "Spain"
keep if importer == "World"
keep if sitc4 == "8973"
collapse (sum) value, by(year sitc4)
gen eiso3c = "ESP"
outsheet using "stata_wtf62-00_ESP_sitc4_8973_total_export.csv", comma nolabel replace
restore


** SITC Level 3 Data **
** ~~~~~~~~~~~~~~~~~ **

**Split Codes to THREE DIGIT
gen sitc3 = substr(sitc4,1,3)
drop sitc4
collapse (sum) value, by(year importer exporter sitc3)


** Country (i) x Product (p) Exports to World over Time
** Filename: stata_wtf62-00_???_sitc3_####_total_export.csv
** Codes
** ~~~~~
** DNK, "062","Sugar confectionery and preparations, non-chocolate"
** ESP, "897","Gold, silver ware, jewelry and articles of precious materials, nes"

** Denmark exports of "062","Sugar confectionery and preparations, non-chocolate"
preserve
keep if exporter == "Denmark"
keep if importer == "World"
keep if sitc3 == "062"
collapse (sum) value, by(year sitc3)
gen eiso3c = "DNK"
outsheet using "stata_wtf62-00_DNK_sitc3_062_total_export.csv", comma nolabel replace
restore

** Spain exports of "897","Gold, silver ware, jewelry and articles of precious materials, nes"
preserve
keep if exporter == "Spain"
keep if importer == "World"
keep if sitc3 == "897"
collapse (sum) value, by(year sitc3)
gen eiso3c = "ESP"
outsheet using "stata_wtf62-00_ESP_sitc3_897_total_export.csv", comma nolabel replace
restore


** IMPORTS **
**~~~~~~~~~**

use "$SOURCE_DIR/wtf62.dta", clear
foreach year of num 63(1)99 {
	append using "$SOURCE_DIR/wtf`year'.dta"
}
append using "$SOURCE_DIR/wtf00.dta"

format value %12.0f

keep year importer exporter sitc4 value

** SITC Level 4 Data **
** ~~~~~~~~~~~~~~~~~ **

** Country (i) x Product (P) Imports From World over Time
** Filename: stata_wtf62-00_???_sitc4_####_total_import.csv
** Codes
** ~~~~~
** DNK, "0620","Sugar confectionery and preparations, non-chocolate"
** ESP, "8973","Precious jewellery, goldsmiths' or silversmiths' wares"

** Denmark Imports of "0620","Sugar confectionery and preparations, non-chocolate"
preserve
keep if exporter == "World"
keep if importer == "Denmark"
keep if sitc4 == "0620"
collapse (sum) value, by(year sitc4)
gen iiso3c = "DNK"
outsheet using "stata_wtf62-00_DNK_sitc4_0620_total_import.csv", comma nolabel replace
restore

** Spain Imports of "8973","Precious jewellery, goldsmiths' or silversmiths' wares"
preserve
keep if exporter == "World"
keep if importer == "Spain"
keep if sitc4 == "8973"
collapse (sum) value, by(year sitc4)
gen iiso3c = "ESP"
outsheet using "stata_wtf62-00_ESP_sitc4_8973_total_import.csv", comma nolabel replace
restore


** SITC Level 3 Data **
** ~~~~~~~~~~~~~~~~~ **

**Split Codes to THREE DIGIT
gen sitc3 = substr(sitc4,1,3)
drop sitc4
collapse (sum) value, by(year importer exporter sitc3)

** Country (i) x Product (p) Imports to World over Time
** Filename: stata_wtf62-00_???_sitc3_####_total_import.csv
** Codes
** ~~~~~
** DNK, "062","Sugar confectionery and preparations, non-chocolate"
** ESP, "897","Gold, silver ware, jewelry and articles of precious materials, nes"

** Denmark imports of "062","Sugar confectionery and preparations, non-chocolate"
preserve
keep if exporter == "Denmark"
keep if importer == "World"
keep if sitc3 == "062"
collapse (sum) value, by(year sitc3)
gen iiso3c = "DNK"
outsheet using "stata_wtf62-00_DNK_sitc3_062_total_import.csv", comma nolabel replace
restore

** Spain imports of "897","Gold, silver ware, jewelry and articles of precious materials, nes"
preserve
keep if exporter == "World"
keep if importer == "Spain"
keep if sitc3 == "897"
collapse (sum) value, by(year sitc3)
gen iiso3c = "ESP"
outsheet using "stata_wtf62-00_ESP_sitc3_897_total_import.csv", comma nolabel replace
restore



*** ------------------------------------> SPECIAL CASE TEST DATA <-------------------------------------------- ***


** ------------------------------------------- **
** Tests data for constructor_dataset_sitcr2() **
** ------------------------------------------- **

** SITC Level 4 **
** ~~~~~~~~~~~~ **

use "$SOURCE_DIR/wtf62.dta", clear
foreach year of num 63(1)99 {
	append using "$SOURCE_DIR/wtf`year'.dta"
}
append using "$SOURCE_DIR/wtf00.dta"

format value %12.0f

keep year importer exporter sitc4 value

** Option: DropAX **
** ~~~~~~~~~~~~~~ **

preserve
gen sitc2 = substr(sitc4,1,2)
keep if year == 1998 & importer == "Bulgaria" & exporter == "World" & sitc2 == "51"
gen sitc3 = substr(sitc4,1,3)
gen iiso3c = "BGR"
drop importer
gen eiso3c = "World"
drop exporter
order year iiso3c eiso3c sitc4 value
outsheet using "stata_wtf98_BGR_sitc4_51##_WORLD_import.csv", comma nolabel replace
restore

** Option: SITCR2 **
** ~~~~~~~~~~~~~~ **

//Not Applicable - Test outside of Stata

** Option: drop_nonsitcr2 **
** ~~~~~~~~~~~~~~~~~~~~~~ **

// Not Applicable - Test outside of Stata

** Option: adjust_hk **
** ~~~~~~~~~~~~~~~~~ **

//Import Hong Kong Adjustment Data //
use "$SOURCE_DIR/china_hk88.dta", clear
foreach year of num 89(1)99 {
	append using "$SOURCE_DIR/china_hk`year'.dta"
}
append using "$SOURCE_DIR/china_hk00.dta"

gen marker = 0
//HONG KONG
replace marker = 1 if year == 1990 & importer == "Oman" & exporter == "China HK SAR" & sitc4 == "6522"
replace marker = 2 if year == 1990 & importer == "Japan" & exporter == "China HK SAR" & sitc4 == "8748"
replace marker = 3 if year == 1990 & importer == "Thailand" & exporter == "China HK SAR" & sitc4 == "5989"
replace marker = 4 if year == 1990 & importer == "Netherlands" & exporter == "China HK SAR" & sitc4 == "8982"
replace marker = 5 if year == 1990 & importer == "China MC SAR" & exporter == "China HK SAR" & sitc4 == "6115"
//CHINA
replace marker = 1 if year == 1990 & importer == "Oman" & exporter == "China" & sitc4 == "6522"
replace marker = 2 if year == 1990 & importer == "Japan" & exporter == "China" & sitc4 == "8748"
replace marker = 3 if year == 1990 & importer == "Thailand" & exporter == "China" & sitc4 == "5989"
replace marker = 4 if year == 1990 & importer == "Netherlands" & exporter == "China" & sitc4 == "8982"
replace marker = 5 if year == 1990 & importer == "China MC SAR" & exporter == "China" & sitc4 == "6115"
keep if marker != 0
gen iiso3c = "WLD" if importer == "World"
replace iiso3c = "JPN" if importer == "Japan"
replace iiso3c = "OMN" if importer == "Oman"
replace iiso3c = "THA" if importer == "Thailand"
replace iiso3c = "MAC" if importer == "China MC SAR"
replace iiso3c = "NLD" if importer == "Netherlands"
gen eiso3c = "CHN" if exporter == "China"
replace eiso3c = "HKG" if exporter == "China HK SAR"
sort marker iiso3c
keep year iiso3c eiso3c sitc4 value value_adj
order year iiso3c eiso3c sitc4
outsheet using "stata_wtf90_hk_china_adjust_sitc4_check_sample.csv", comma nolabel replace

** Option: intertemp_cntrycode **
** ~~~~~~~~~~~~~~~~~~~~~~~~~~~ **

// Not Applicable - Test outside of Stata

** Option: drop_incp_cntrycode **
** ~~~~~~~~~~~~~~~~~~~~~~~~~~~ **

// Not Applicable - Test outside of Stata
