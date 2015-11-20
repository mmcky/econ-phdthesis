*** --------------------------------------------------------------------***
*** Generate ATLAS Stata Dataset 					***
***									***
*** Datasets 								***
*** -------- 								***
*** raw/   	 Fully Converted Datasets Included NES, WLD etc. 	***
*** ./		 Datasets only containing Countries 			***
*** --------------------------------------------------------------------***

set more off 
//set trace on
//set tracedepth 1
 
di "Loading settings for environment: " c(os)

if c(os) == "MacOSX" | c(os) == "Unix" {
	// Sources //
	global SOURCE="~/work-data/datasets/2d48c79173719bd41eb5e192fb4470b6"
	global META = "~/repos/pyeconlab/pyeconlab/trade/dataset/CIDATLAS/meta" 			//Hard Coded For Now
	global METACLASS = "~/repos/pyeconlab/pyeconlab/trade/classification/meta" 			//Hard Coded For Now
	// Targets //
	global WORKINGDIR="~/work-temp-local"
}

/*
if c(os) == "Windows" {
	global SOURCE="D:\work-data\datasets\2d48c79173719bd41eb5e192fb4470b6"
	global META = "C:\Users\Matt-Work\work\repos\pyeconlab\pyeconlab\trade\dataset\NBERWTF\meta" 		//Hard Coded For Now
	global METACLASS = "C:\Users\Matt-Work\work\repos\pyeconlab\pyeconlab\trade\classification\meta" 	//Hard Coded For Now
	// Targets //
	global WORKINGDIR="D:/work-temp"
}
*/

log using "cidatlas_dataset.log", replace

cd $WORKINGDIR

mkdir "raw"

***---CONTROL---***

global META = 1
global SITCDATA = 1
global SITCCOUNTRYDATA = 1
global HSDATA = 1
global HSCOUNTRYDATA = 1

**--------**
**--META--**
**--------**

if $META == 1 {
	di "Compiling META DATA ..."
	
	*-Country-*
	insheet using "$SOURCE/country.tsv", names tab clear
	rename id iso3c
	replace iso3c = upper(iso3c)
	//Check CountryCodes//
	gen marker = 1
	global NOTCOUNTRIES "XXA XXB XXC XXD XXE XXF XXG XXH XXI WLD" 	//Source: pyeconlab cidatlas/meta/csv
	foreach item in $NOTCOUNTRIES {
		replace marker = 0 if iso3c == "`item'"
	}
	sort iso3c
	save "cidatlas_country_meta.dta", replace
	//Country Only List//
	keep if marker == 1
	drop marker
	sort iso3c
	// Add eiso3c and iiso3c //
	gen eiso3c = iso3c
	gen iiso3c = iso3c
	gen marker = 1
	save "cidatlas_countryonly_meta.dta", replace
	
	*-SITC Products-*
	insheet using "$SOURCE/sitc4.tsv", names tab clear
	rename id sitc4
	//Fix SITC4 Codes//
	tostring(sitc4), replace
	gen length = strlen(sitc4)
	codebook length
	replace sitc4="0"+sitc4 if length == 3
	replace sitc4="00"+sitc4 if length == 2
	replace sitc4="000"+sitc4 if length == 1
	drop length
	//Description//
	rename name description
	sort sitc4
	save "cidatlas_sitc4_meta.dta", replace
	
	*-HS Products-*
	insheet using "$SOURCE/hs4.tsv", names tab clear
	rename id hs4
	//Fix HS4 Codes//
	tostring(hs4), replace
	gen length = strlen(hs4)
	codebook length
	replace hs4="0"+hs4 if length == 3
	replace hs4="00"+hs4 if length == 2
	replace hs4="000"+hs4 if length == 1
	drop length
	//Description//
	rename name description
	sort hs4
	save "cidatlas_hs4_meta.data", replace
}

**--------**
**--SITC--**
**--------**

** Conversion of Atlas of Complexity Datasets with Harmonised Variables Names and Formats **

if $SITCDATA == 1 {

	***----------------***
	***---Trade DATA---***
	***----------------***

	di "Compiling SITC Trade DATA ..."

	insheet using "$SOURCE/year_origin_destination_sitc.tsv", tab clear

	//Fix SITC4 Codes//
	tostring(sitc4), replace
	gen length = strlen(sitc4)
	codebook length
	replace sitc4="0"+sitc4 if length == 3
	replace sitc4="00"+sitc4 if length == 2
	replace sitc4="000"+sitc4 if length == 1
	drop length

	//Year Coverage//
	codebook year

	//Country Codes//
	rename origin eiso3c
	replace eiso3c = upper(eiso3c)
	rename destination iiso3c
	replace iiso3c = upper(iiso3c)

	//Check Values//
	gen CHECK = 1 if export_val == 0 & import_val == 0
	codebook CHECK 				 			//Should be no overlap all missing values
	drop CHECK
	gen CHECK = 1 if export_val == . & import_val == . 		//Should be no overlap all missing values
	codebook CHECK
	di "These need to be investigated as . or 0 values?"
	drop CHECK

	/// Current Issues with RESHAPE and Uniqueness ///
	duplicates tag year eiso3c iiso3c sitc4, generate(dup)
	codebook year if dup != 0
	codebook eiso3c if dup != 0
	codebook iiso3c if dup != 0
	codebook sitc4 if dup != 0
	sort year eiso3c iiso3c sitc4 dup
	list if dup != 0
	//Remove Duplicates by collapsing//
	collapse (sum) export_val import_val, by(year eiso3c iiso3c sitc4)

	count
	rename export_val val_export
	rename import_val val_import
	reshape long val_, i(year eiso3c iiso3c sitc4) j(dot) string
	count

	rename val_ value

	save "raw/cidatlas_sitcr2l4_trade_1962to2012.dta", replace

	// Trade Data for Levels 3,2,1 //
	foreach level in 3 2 1 {
		di "Producing trade datasets for Level: `level'"
		// Trade //
		use "raw/cidatlas_sitcr2l4_trade_1962to2012.dta", clear
		gen sitc`level' = substr(sitc4,1,`level')
		collapse (sum) value, by(year eiso3c iiso3c sitc`level' dot)
		save "raw/cidatlas_sitcr2l`level'_trade_1962to2012.dta", replace
	}


	***----------------------------***
	***---Export and Import DATA---***
	***----------------------------***

	di "Compiling SITC EXPORT and IMPORT DATA ..."

	insheet using "$SOURCE/year_origin_sitc.tsv", tab clear

	//Fix SITC4 Codes//
	tostring(sitc4), replace
	gen length = strlen(sitc4)
	codebook length
	replace sitc4="0"+sitc4 if length == 3
	replace sitc4="00"+sitc4 if length == 2
	replace sitc4="000"+sitc4 if length == 1
	drop length

	//Year Coverage//
	codebook year

	//Exports Dataset//
	preserve
	rename origin eiso3c 		
	replace eiso3c = upper(eiso3c)
	rename export_val value
	keep year eiso3c sitc4 value
	sort year eiso3c sitc4 value
	save "raw/cidatlas_sitcr2l4_export_1962to2012.dta", replace
	restore

	//Export RCA Dataset//
	preserve
	rename origin eiso3c 		
	replace eiso3c = upper(eiso3c)
	rename export_rca rca
	keep year eiso3c sitc4 rca
	sort year eiso3c sitc4 rca
	save "raw/cidatlas_sitcr2l4_export_rca_1962to2012.dta", replace
	restore

	//Import Dataset//
	preserve
	rename origin iiso3c
	replace iiso3c = upper(iiso3c)
	rename import_val value
	keep year iiso3c sitc4 value
	sort year iiso3c sitc4 value
	save "raw/cidatlas_sitcr2l4_import_1962to2012.dta", replace
	restore

	//Import RCA Dataset//
	preserve
	rename origin iiso3c
	replace iiso3c = upper(iiso3c)
	rename import_rca rca
	keep year iiso3c sitc4 rca
	sort year iiso3c sitc4 rca
	save "raw/cidatlas_sitcr2l4_import_rca_1962to2012.dta", replace
	restore

	//Export and Import Data for Levels 3,2,1 //
	foreach level in 3 2 1 {
		di "Producing export and import datasets for Level: `level'"
		// Export //
		use "raw/cidatlas_sitcr2l4_export_1962to2012.dta", clear
		gen sitc`level' = substr(sitc4,1,`level')
		collapse (sum) value, by(year eiso3c sitc`level')
		save "raw/cidatlas_sitcr2l`level'_export_1962to2012.dta", replace
		// Import //
		use "raw/cidatlas_sitcr2l4_import_1962to2012.dta", clear
		gen sitc`level' = substr(sitc4,1,`level')
		collapse (sum) value, by(year iiso3c sitc`level')
		save "raw/cidatlas_sitcr2l`level'_import_1962to2012.dta", replace
	}

}

** -- SITC Datasets that Contain Countries Only -- **

if $SITCCOUNTRYDATA == 1 {
	
	di "Compiling Country ONLY SITC Datasets ..."
	
	di "[WARNING] This requires SITCDATA to have been compiled in raw/ ..."
	
	***----------------***
	***---Trade DATA---***
	***----------------***
	
	use "raw/cidatlas_sitcr2l4_trade_1962to2012.dta", clear
	merge m:1 eiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	merge m:1 iiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	save "cidatlas_sitcr2l4_trade_1962to2012.dta", replace
	
	// Trade Data for Levels 3,2,1 //
	foreach level in 3 2 1 {
		di "Producing trade datasets for Level: `level'"
		// Trade //
		use "cidatlas_sitcr2l4_trade_1962to2012.dta", clear
		gen sitc`level' = substr(sitc4,1,`level')
		collapse (sum) value, by(year eiso3c iiso3c sitc`level' dot)
		save "cidatlas_sitcr2l`level'_trade_1962to2012.dta", replace
	}
	
	***----------------------------***
	***---Export and Import DATA---***
	***----------------------------***
	
	//Export//
	use "raw/cidatlas_sitcr2l4_export_1962to2012.dta", clear
	merge m:1 eiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	save "cidatlas_sitcr2l4_export_1962to2012.dta", replace

	//Export RCA//
	use "raw/cidatlas_sitcr2l4_export_rca_1962to2012.dta", clear
	merge m:1 eiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	save "cidatlas_sitcr2l4_export_rca_1962to2012.dta", replace

	//Import//
	use "raw/cidatlas_sitcr2l4_import_1962to2012.dta", clear
	merge m:1 iiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	save "cidatlas_sitcr2l4_import_1962to2012.dta", replace

	//Import RCA//
	use "raw/cidatlas_sitcr2l4_import_rca_1962to2012.dta", clear
	merge m:1 iiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	save "cidatlas_sitcr2l4_import_rca_1962to2012.dta", replace
	
	//Export and Import Data for Levels 3,2,1 //
	foreach level in 3 2 1 {
		di "Producing export and import datasets for Level: `level'"
		// Export //
		use "cidatlas_sitcr2l4_export_1962to2012.dta", clear
		gen sitc`level' = substr(sitc4,1,`level')
		collapse (sum) value, by(year eiso3c sitc`level')
		save "cidatlas_sitcr2l`level'_export_1962to2012.dta", replace
		// Import //
		use "cidatlas_sitcr2l4_import_1962to2012.dta", clear
		gen sitc`level' = substr(sitc4,1,`level')
		collapse (sum) value, by(year iiso3c sitc`level')
		save "cidatlas_sitcr2l`level'_import_1962to2012.dta", replace
	}

}

**------**
**--HS--**
**------**

if $HSDATA == 1 {
	
	di "Compiling HS TRADE DATA ..."

	***----------------***
	***---TRADE DATA---***
	***----------------***

	insheet using "$SOURCE/year_origin_destination_hs.tsv", tab clear
	
	//Fix HS4 Codes//
	tostring(hs4), replace
	gen length = strlen(hs4)
	codebook length
	replace hs4="0"+hs4 if length == 3
	replace hs4="00"+hs4 if length == 2
	replace hs4="000"+hs4 if length == 1
	drop length

	//Year Coverage//
	codebook year

	//Country Codes//
	rename origin eiso3c
	replace eiso3c = upper(eiso3c)
	rename destination iiso3c
	replace iiso3c = upper(iiso3c)
	
	//Values//
	count
	gen CHECK = 1 if export_val == 0 & import_val == 0
	codebook CHECK 				 			//Should be no overlap all missing values
	drop CHECK
	gen CHECK = 1 if export_val == . & import_val == . 		//Should be no overlap all missing values
	codebook CHECK
	drop CHECK

	/// Current Issues with RESHAPE and Uniqueness ///
	duplicates tag year eiso3c iiso3c hs4, generate(dup)
	codebook year if dup != 0
	codebook eiso3c if dup != 0
	codebook iiso3c if dup != 0
	codebook hs4 if dup != 0
	sort year eiso3c iiso3c hs4 dup
	list if dup != 0
	//Remove Duplicates by collapsing//
	collapse (sum) export_val import_val, by(year eiso3c iiso3c hs4)	
	
	//Reshape Data//
	count
	rename export_val val_export
	rename import_val val_import
	reshape long val_, i(year eiso3c iiso3c hs4) j(dot) string
	count

	rename val_ value

	save "raw/cidatlas_hs92l4_trade_1995to2012.dta", replace
	
	// Trade Data for Levels 3,2,1 //
	foreach level in 3 2 1 {
		di "Producing trade datasets for Level: `level'"
		// Trade //
		use "raw/cidatlas_hs92l4_trade_1995to2012.dta", clear
		gen hs`level' = substr(hs4,1,`level')
		collapse (sum) value, by(year eiso3c iiso3c hs`level' dot)
		save "raw/cidatlas_hs92l`level'_trade_1995to2012.dta", replace
	}
	
	***----------------------------***
	***---EXPORT and IMPORT DATA---***
	***----------------------------***	
	
	di "Compiling HS EXPORT and IMPORT DATA ..."
	
	insheet using "$SOURCE/year_origin_hs.tsv", tab clear

	//Fix SITC4 Codes//
	tostring(hs4), replace
	gen length = strlen(hs4)
	codebook length
	replace hs4="0"+hs4 if length == 3
	replace hs4="00"+hs4 if length == 2
	replace hs4="000"+hs4 if length == 1
	drop length

	//Year Coverage//
	codebook year

	//Exports Dataset//
	preserve
	rename origin eiso3c 		
	replace eiso3c = upper(eiso3c)
	rename export_val value
	keep year eiso3c hs4 value
	save "raw/cidatlas_hs92l4_export_1995to2012.dta", replace
	restore

	//Export RCA Dataset//
	preserve
	rename origin eiso3c 		
	replace eiso3c = upper(eiso3c)
	rename export_rca rca
	keep year eiso3c hs4 rca
	save "raw/cidatlas_hs92l4_export_rca_1995to2012.dta", replace
	restore

	//Import Dataset//
	preserve
	rename origin iiso3c
	replace iiso3c = upper(iiso3c)
	rename import_val value
	keep year iiso3c hs4 value
	save "raw/cidatlas_hs92l4_import_1995to2012.dta", replace
	restore

	//Import RCA Dataset//
	preserve
	rename origin iiso3c
	replace iiso3c = upper(iiso3c)
	rename import_rca rca
	keep year iiso3c hs4 rca
	save "raw/cidatlas_hs92l4_import_rca_1995to2012.dta", replace
	restore

	//Export and Import Data for Levels 3,2,1 //
	foreach level in 3 2 1 {
		di "Producing export and import datasets for Level: `level'"
		// Export //
		use "raw/cidatlas_hs92l4_export_1995to2012.dta", clear
		gen hs`level' = substr(hs4,1,`level')
		collapse (sum) value, by(year eiso3c hs`level')
		save "raw/cidatlas_hs92l`level'_export_1995to2012.dta", replace
		// Import //
		use "raw/cidatlas_hs92l4_import_1995to2012.dta", clear
		gen hs`level' = substr(hs4,1,`level')
		collapse (sum) value, by(year iiso3c hs`level')
		save "raw/cidatlas_hs92l`level'_import_1995to2012.dta", replace
	}
	
}

** -- HS Countries Only Dataset -- **

if $HSCOUNTRYDATA == 1 {

	***----------------***
	***---TRADE DATA---***
	***----------------***
	use "raw/cidatlas_hs92l4_trade_1995to2012.dta", clear
	merge m:1 eiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	merge m:1 iiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	save "cidatlas_hs92l4_trade_1995to2012.dta", replace
	
	// Trade Data for Levels 3,2,1 //
	foreach level in 3 2 1 {
		di "Producing trade datasets for Level: `level'"
		// Trade //
		use "cidatlas_hs92l4_trade_1995to2012.dta", clear
		gen hs`level' = substr(hs4,1,`level')
		collapse (sum) value, by(year eiso3c iiso3c hs`level' dot)
		save "cidatlas_hs92l`level'_trade_1995to2012.dta", replace
	}


	***----------------------------***
	***---EXPORT and IMPORT DATA---***
	***----------------------------***
	
	//Exports Dataset//
	use "raw/cidatlas_hs92l4_export_1995to2012.dta", clear
	merge m:1 eiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	save "cidatlas_hs92l4_export_1995to2012.dta", replace
	
	//Exports RCA Dataset//
	use "raw/cidatlas_hs92l4_export_rca_1995to2012.dta", clear
	merge m:1 eiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	save "cidatlas_hs92l4_export_rca_1995to2012.dta", replace
	
	//Import Dataset//
	use "raw/cidatlas_hs92l4_import_1995to2012.dta", clear
	merge m:1 iiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	save "cidatlas_hs92l4_import_1995to2012.dta", replace
	
	//Imports RCA Dataset//
	use "raw/cidatlas_hs92l4_import_rca_1995to2012.dta", clear
	merge m:1 iiso3c using "cidatlas_countryonly_meta.dta", keepusing(marker)
	keep if marker == 1
	drop marker _merge
	save "cidatlas_hs92l4_import_rca_1995to2012.dta", replace
	
	//Export and Import Data for Levels 3,2,1 //
	foreach level in 3 2 1 {
		di "Producing export and import datasets for Level: `level'"
		// Export //
		use "cidatlas_hs92l4_export_1995to2012.dta", clear
		gen hs`level' = substr(hs4,1,`level')
		collapse (sum) value, by(year eiso3c hs`level')
		save "cidatlas_hs92l`level'_export_1995to2012.dta", replace
		// Import //
		use "cidatlas_hs92l4_import_1995to2012.dta", clear
		gen hs`level' = substr(hs4,1,`level')
		collapse (sum) value, by(year iiso3c hs`level')
		save "cidatlas_hs92l`level'_import_1995to2012.dta", replace
	}
}

log close
