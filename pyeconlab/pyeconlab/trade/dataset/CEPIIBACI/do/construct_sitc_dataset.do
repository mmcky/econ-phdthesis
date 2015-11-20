*** --------------------------------------------------------------------***
*** Generate Basic SITC Datasets for PyEconLab (CEPII DATA) HS96 	***
*** --------------------------------------------------------------------***

*** Notes
*** -----
*** [1] Currently this requires MANUAL adjustment for directories based on REPO Locations etc
*** [2] Manually set the appropriate flags to produce Datasets A

*** Future Work
*** -----------
*** 1. Add in a level option

set more off 
//set trace on
//set tracedepth 1
 
di "Loading settings for environment: " c(os)

if c(os) == "MacOSX" | c(os) == "Unix" {
	// Sources //
	global SOURCE="~/work-data/datasets/e988b6544563675492b59f397a8cb6bb"
	global META = "~/repos/pyeconlab/pyeconlab/trade/dataset/CEPIIBACI/meta" 				//Hard Coded For Now
	global METACLASS = "~/repos/pyeconlab/pyeconlab/trade/classification/meta" 				//Hard Coded For Now
	global METACONCORD = "~/repos/pyeconlab/pyeconlab/trade/concordance/data" 				//Hard Coded For Now
	// Targets //
	global WORKINGDIR="~/work-temp"
}

if c(os) == "Windows" {
	global SOURCE="D:\work-data\datasets\e988b6544563675492b59f397a8cb6bb"
	global META = "C:\Users\Matt-Work\work\repos\pyeconlab\pyeconlab\trade\dataset\CEPIIBACI\meta" 		//Hard Coded For Now
	global METACLASS = "C:\Users\Matt-Work\work\repos\pyeconlab\pyeconlab\trade\classification\meta" 	//Hard Coded For Now
	global METACONCORD = "C:\Users\Matt-Work\work\repos\pyeconlab\pyeconlab\trade\concordance\data" 	//Hard Coded For Now
	// Targets //
	global WORKINGDIR="D:/work-temp"
}

cd $WORKINGDIR

** Datasets 
** --------
** A: check_concordance=True; adjust_units= False; concordance_institution='un'

** Settings **
global DATASETS "A"
global SETUP_CACHE 0

if $SETUP_CACHE == 1 {
	di "WARNING ... Setup Cache Flag is set to 1"
}

foreach DATASET of global DATASETS {
	
	capture log close
	local logfl = "cepiibaci_stata_sitcl3_data_"+"`DATASET'"+".log"
	log using `logfl', replace

	di "Processing Option: `DATASET'"
	
	if "`DATASET'" == "A" {
		global check_concordance = 1
		global adjust_units = 0
		global intertemporal_cntry_recode = 0 		//Future Use
		global incomplete_cntry_recode = 0 		//Future Use
	}
	else {
		di "Option `DATASET' not valid"
		exit
	}

	** ------------------ ** 
	** Write Concordances **
	** ------------------ ** 
	 
	**Concord to ISO3C**
	**/meta/csv/countrycodes_to_iso3c.csv contains this listing**
	**Copied: 27-08-2014
	**Note: This is not intertemporally consistent**
	insheet using "$META/csv/hs96_iso3n_to_iso3c.csv", clear names
	rename iso3n i
	rename iso3c eiso3c
	save "i_to_eiso3c.dta", replace
	insheet using "$META/csv/hs96_iso3n_to_iso3c.csv", clear names
	rename iso3n j
	rename iso3c iiso3c
	save "j_to_iiso3c.dta", replace
	
	**Compare pyeconlab meta with baci country tables**
	insheet using "$SOURCE/country_code_baci96_adjust.csv", clear names
	merge 1:1 i using "i_to_eiso3c.dta", keepusing(eiso3c)
	gen j = i 								//For Merging with IISO3C
	list if _merge == 1
	list if _merge == 2
	drop _merge
	save "country_code_baci96_adjust.dta", replace

	** HS6 to SITC Classification **
	infix str hs6 1-6 str sitc 8-12 using "$METACONCORD/un/HS1996_to_SITCR2.csv", clear 
	drop in 1/1 //Drop Variable Names
	save "hs96_hs6_to_sitc.dta", replace

	**SITC Revision 2 Level 2 Indicator Codes**
	**classification/meta/SITC-R2-L3-codes.csv contains this listing**
	**Source: UN
	**Copied: 28-08-2014
	infix using "$METACLASS/SITC-R2-L3-codes.dct", using("$METACLASS/SITC-R2-L3-codes.csv") clear
	save "MARKER_SITC-R2-L3-codes.dta", replace

	** ----------------- **
	** Convert RAW Files **
	** ----------------- **
	if $SETUP_CACHE == 1 {
		di "Setting Up Cache Files ..."
		foreach year of num 1998(1)2012 {
			di "Processing: `year' ..."
			insheet using "$SOURCE/baci96_`year'.csv", clear names
			tostring(hs6), replace
			gen fixleadingzero = 6 - length(hs6)
			codebook fixleadingzero
			replace hs6 = "0"+hs6 if fixleadingzero == 1
			drop fixleadingzero
			save "$SOURCE/cache/baci96_`year'.dta", replace
		}
	}

	*** -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- **
	 
	 
	******************************************
	**Dataset #1: Bilateral TRADE Data 	**
	******************************************

	// Compile WTF Source Files //
	use "$SOURCE/cache/baci96_1998.dta", clear
	foreach year of num 1999(1)2012 {
		append using "$SOURCE/cache/baci96_`year'.dta"
	}
	//save "$SOURCE/cache/baci96.dta", replace
	
	** Concord Country ISO3n to ISO3c **
	//Exporter Codes//
	di "Importing EISO3C ..."
	merge m:1 i using "i_to_eiso3c.dta", keepusing(eiso3c)
	preserve
	keep if _merge == 1
	keep i
	duplicates drop
	merge 1:1 i using "country_code_baci96_adjust.dta", keepusing(name_english iso3 iso2)
	keep if _merge == 3
	di "The Following List will be dropped!"
	list
	restore
	keep if _merge == 3 	//Keep Only Matched Items
	drop _merge
	//Importer Codes//
	di "Importing IISO3C ..."
	merge m:1 j using "j_to_iiso3c.dta", keepusing(iiso3c)
	preserve
	keep if _merge == 1
	keep j
	duplicates drop
	merge 1:1 j using "country_code_baci96_adjust.dta", keepusing(name_english iso3 iso2)
	keep if _merge == 3
	di "The Following List will be dropped!"
	list
	restore
	keep if _merge == 3 	//Keep Only Matched Items
	drop _merge
	
	//Double Check//
	list if eiso3c == ""
	list if eiso3c == "."
	list if iiso3c == ""
	list if iiso3c == "."
	
	rename t year
	rename i eiso3n
	rename j iiso3n
	rename v value
	rename q quantity

	format value %12.0f 			//Same as NBER

	//Log Check
	**Record Total Value by Year in Log**
	preserve
	collapse (sum) value, by(year)
	list
	restore

	//Year, Exporters, Importers
	codebook year
	codebook eiso3n
	codebook iiso3n

	** Concord Classification System **
	merge m:1 hs6 using "hs96_hs6_to_sitc.dta", keepusing(sitc)
	list if _merge == 1
	list if _merge == 2
	drop if _merge == 2 	// Remove Unused Codes
	drop _merge
	
	gen sitc3 = substr(sitc,1,3)

	// SITC Revision 3 Level 3 Dataset //
	collapse (sum) value, by(year eiso3c iiso3c sitc3)

	preserve
	keep sitc3
	duplicates drop
	count
	sort sitc3
	list
	restore
	

	// The Below require eiso3c_intertemporal_recoda.dta" from nber stata file or some other intertemporal coding
	if $intertemporal_cntry_recode == 1{
		merge m:1 eiso3c using "eiso3c_intertemporal_recodes.dta"
		list if _merge == 2 	
		drop if _merge == 2 										//Drop unmatched from recodes file
		replace eiso3c = _recode_ if _merge == 3
		drop _merge _recode_
		merge m:1 iiso3c using "iiso3c_intertemporal_recodes.dta"
		list if _merge == 2 	
		drop if _merge == 2 										//Drop unmatched from recodes file
		replace iiso3c = _recode_ if _merge == 3
		drop _merge _recode_
		collapse (sum) value, by(year eiso3c iiso3c sitc3)
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

	if $incomplete_cntry_recode == 1{
		merge m:1 eiso3c using "eiso3c_incomplete_recodes.dta"
		list if _merge == 2
		drop if _merge == 2  	//Drop unmatched from recodes file
		drop if _merge == 3 	//Drop Matching Countries
		drop _merge _recode_
		merge m:1 iiso3c using "iiso3c_incomplete_recodes.dta"
		list if _merge == 2
		drop if _merge == 2 	//Drop unmatched from recodes file
		drop if _merge == 3 	//Drop Matching Countries
		drop _merge _recode_
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}


	order year eiso3c iiso3c sitc3 value
	sort year eiso3c iiso3c sitc3
	local fl = "bacihs96_stata_trade_sitcr2l3_1998to2012_"+"`DATASET'"+".dta"
	save `fl', replace


	*** -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- **


	****************************						
	**Dataset #2: Export Data **
	****************************

	// Compile WTF Source Files //
	use "$SOURCE/cache/baci96_1998.dta", clear
	foreach year of num 1999(1)2012 {
		append using "$SOURCE/cache/baci96_`year'.dta"
	}
	//save "$SOURCE/cache/baci96.dta", replace
	
	** Concord Country ISO3n to ISO3c **
	** ------------------------------ **
	//Exporter Codes//
	di "Importing EISO3C ..."
	merge m:1 i using "i_to_eiso3c.dta", keepusing(eiso3c)
	preserve
	keep if _merge == 1
	keep i
	duplicates drop
	merge 1:1 i using "country_code_baci96_adjust.dta", keepusing(name_english iso3 iso2)
	keep if _merge == 3
	di "The Following List will be dropped!"
	list
	restore
	keep if _merge == 3 	//Keep Only Matched Items
	drop _merge

	rename t year
	rename i eiso3n
	rename j iiso3n
	rename v value
	rename q quantity

	format value %12.0f 			//Same as NBER

	//Collapse to Export Data//
	collapse (sum) value, by(year eiso3n eiso3c hs6)

	//Log Check
	**Record Total Value by Year in Log**
	preserve
	collapse (sum) value, by(year)
	list
	restore

	//Year, Exporters
	codebook year
	codebook eiso3n

	** Concord Classification System **
	** ----------------------------- **
	merge m:1 hs6 using "hs96_hs6_to_sitc.dta", keepusing(sitc)
	list if _merge == 1
	list if _merge == 2
	drop if _merge == 2 	// Remove Unused Codes
	drop _merge
	
	gen sitc3 = substr(sitc,1,3)

	// SITC Revision 3 Level 3 Dataset //
	collapse (sum) value, by(year eiso3c sitc3)

	**Parse Options for Export Files**

	if $intertemporal_cntry_recode == 1{
		merge m:1 eiso3c using "eiso3c_intertemporal_recodes.dta"
		list if _merge == 2
		drop if _merge == 2 	//Drop unmatched from recodes file
		replace eiso3c = _recode_ if _merge == 3
		drop _merge _recode_
		collapse (sum) value, by(year eiso3c sitc3)
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

	if $incomplete_cntry_recode == 1{
		merge m:1 eiso3c using "eiso3c_incomplete_recodes.dta"
		list if _merge == 2
		drop if _merge == 2 	//Drop unmatched from recodes file
		drop if _merge == 3 	//Drop Matching Countries
		drop _merge _recode_
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

	order year eiso3c sitc3 value
	sort year eiso3c sitc3
	local fl = "bacihs96_stata_export_sitcr2l3_1998to2012_"+"`DATASET'"+".dta"
	save `fl', replace

	*** -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- **


	************************************ 						
	**Dataset #3: Country Import Data **
	************************************ 

	// Compile WTF Source Files //
	use "$SOURCE/cache/baci96_1998.dta", clear
	foreach year of num 1999(1)2012 {
		append using "$SOURCE/cache/baci96_`year'.dta"
	}
	//save "$SOURCE/cache/baci96.dta", replace
	
	** Concord Country ISO3n to ISO3c **
	** ------------------------------ **
	//Importer Codes//
	di "Importing IISO3C ..."
	merge m:1 j using "j_to_iiso3c.dta", keepusing(iiso3c)
	preserve
	keep if _merge == 1
	keep j
	duplicates drop
	merge 1:1 j using "country_code_baci96_adjust.dta", keepusing(name_english iso3 iso2)
	keep if _merge == 3
	di "The Following List will be dropped!"
	list
	restore
	keep if _merge == 3 	//Keep Only Matched Items
	drop _merge

	rename t year
	rename i eiso3n
	rename j iiso3n
	rename v value
	rename q quantity

	format value %12.0f 			//Same as NBER

	//Collapse to Import Data//
	collapse (sum) value, by(year iiso3n iiso3c hs6)

	//Log Check
	**Record Total Value by Year in Log**
	preserve
	collapse (sum) value, by(year)
	list
	restore

	//Year, Importers
	codebook year
	codebook iiso3n

	** Concord Classification System **
	** ----------------------------- **
	merge m:1 hs6 using "hs96_hs6_to_sitc.dta", keepusing(sitc)
	list if _merge == 1
	list if _merge == 2
	drop if _merge == 2 	// Remove Unused Codes
	drop _merge
	
	gen sitc3 = substr(sitc,1,3)

	// SITC Revision 3 Level 3 Dataset //
	collapse (sum) value, by(year iiso3c sitc3)

	**Parse Options for Import Files**

	if $intertemporal_cntry_recode == 1{
		merge m:1 iiso3c using "iiso3c_intertemporal_recodes.dta"
		list if _merge == 2
		drop if _merge == 2		//Drop unmatched from recodes file
		replace iiso3c = _recode_ if _merge == 3
		drop _merge _recode_
		collapse (sum) value, by(year iiso3c sitc3)
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

	if $incomplete_cntry_recode == 1{
		merge m:1 iiso3c using "iiso3c_incomplete_recodes.dta"
		list if _merge == 2
		drop if _merge == 2		//Drop unmatched from recodes file
		drop if _merge == 3 	//Drop Matching Countries
		drop _merge _recode_
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

	order year iiso3c sitc3 value
	sort year iiso3c sitc3
	local fl = "bacihs96_stata_import_sitcr2l3_1998to2012_"+"`DATASET'"+".dta"
	save `fl', replace

	log close
	
}

