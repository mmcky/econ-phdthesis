*** --------------------------------------------------------------------***
*** Generate Basic SITC Level 3 Country Datasets for PyEconLab 			***
*** ---------------------------------------------------------- 			***
*** Output
***	------
*** [1] nberfeenstrawtf_do_stata_basic_country_data_bilateral.dta
*** [2] nberfeenstrawtf_do_stata_basic_country_data_exports.dta
*** [3] nberfeenstrawtf_do_stata_basic_country_data_imports.dta

*** Notes
*** -----
*** [1] Currently this requires MANUAL adjustment for directories based on REPO Locations etc
*** [2] Manually set the appropriate flags to produce Datasets A - D

*** Future Work
*** 1. Add Level Option

set more off 
//set trace on
//set tracedepth 1
 
di "Loading settings for environment: " c(os)

if c(os) == "MacOSX" | c(os) == "Unix" {
	// Sources //
	global SOURCE="~/work-data/datasets/36a376e5a01385782112519bddfac85e"
	global META = "~/repos/pyeconlab/pyeconlab/trade/dataset/NBERWTF/meta" 				//Hard Coded For Now
	global METACLASS = "~/repos/pyeconlab/pyeconlab/trade/classification/meta" 			//Hard Coded For Now
	// Targets //
	global WORKINGDIR="~/work-temp"
}

if c(os) == "Windows" {
	global SOURCE="D:\work-data\datasets\36a376e5a01385782112519bddfac85e"
	global META = "C:\Users\Matt-Work\work\repos\pyeconlab\pyeconlab\trade\dataset\NBERWTF\meta" 		//Hard Coded For Now
	global METACLASS = "C:\Users\Matt-Work\work\repos\pyeconlab\pyeconlab\trade\classification\meta" 	//Hard Coded For Now
	// Targets //
	global WORKINGDIR="D:/work-temp"
}

cd $WORKINGDIR

** Datasets 
** --------
** [A] AX=True, dropAX=False, sitcr2=True, drop_nonsitcr2=False, adjust_hk=False, intertemp_productcode=False, intertemp_cntrycode=False, drop_incp_cntrycode=False
** [B] AX=True, dropAX=False, sitcr2=True, drop_nonsitcr2=False, adjust_hk=True,  intertemp_productcode=False, intertemp_cntrycode=False, drop_incp_cntrycode=False
** [C] AX=True, dropAX=True,  sitcr2=True, drop_nonsitcr2=False, adjust_hk=True,  intertemp_productcode=False, intertemp_cntrycode=False, drop_incp_cntrycode=False
** [D] AX=True, dropAX=True,  sitcr2=True, drop_nonsitcr2=True,  adjust_hk=True,  intertemp_productcode=False, intertemp_cntrycode=False, drop_incp_cntrycode=False
** [E] AX=True, dropAX=True,  sitcr2=True, drop_nonsitcr2=False, adjust_hk=True,  intertemp_productcode=True,  intertemp_cntrycode=False, drop_incp_cntrycode=False
** [F] AX=True, dropAX=True,  sitcr2=True, drop_nonsitcr2=False, adjust_hk=True,  intertemp_productcode=True,  intertemp_cntrycode=True,  drop_incp_cntrycode=False
** [G] AX=True, dropAX=True,  sitcr2=True, drop_nonsitcr2=True,  adjust_hk=True,  intertemp_productcode=False, intertemp_cntrycode=True,  drop_incp_cntrycode=False
** !! -- EXPERIMENTAL -- !!
** [H] AX=True, dropAX=True,  sitcr2=True, drop_nonsitcr2=False, adjust_hk=True,  intertemp_productcode=True,  intertemp_cntrycode=True,  drop_incp_cntrycode=True
** [I] AX=True, dropAX=Ture,  sitcr2=True, drop_nonsitcr2=True,  adjust_hk=True,  intertemp_productcode=False, intertemp_cntrycode=False, drop_incp_cntrycode=True

** Settings **
//global DATASET "A"
//global LEVEL 3   			//NotImplemented

global DATASETS "A B C D E F" 	//G H I 

global DATASETS "G" //Run Single DATASET

foreach item of global DATASETS {
	
	global DATASET="`item'"
	
	capture log close
	local fl = "nberwtf_stata_sitcl3_data_"+"$DATASET"+".log"
	log using `fl', replace

	di "Processing Option: $DATASET"
	
	if "$DATASET" == "A" {
		global dropAX 	= 0
		global dropNonSITCR2 = 0 
		global adjust_hk = 0
		global intertemporal_prod_recode = 0
		global intertemporal_cntry_recode = 0
		global incomplete_cntry_recode = 0
	}
	else if "$DATASET" == "B" {
		global dropAX 	= 0
		global dropNonSITCR2 = 0
		global adjust_hk = 1
		global intertemporal_prod_recode = 0
		global intertemporal_cntry_recode = 0
		global incomplete_cntry_recode = 0
	}
	else if "$DATASET" == "C" {
		global dropAX 	= 1
		global dropNonSITCR2 = 0 
		global adjust_hk = 1
		global intertemporal_prod_recode = 0
		global intertemporal_cntry_recode = 0
		global incomplete_cntry_recode = 0
	}
	// MAJOR //
	else if "$DATASET" == "D" {
		global dropAX 	= 1
		global dropNonSITCR2 = 1 
		global adjust_hk = 1
		global intertemporal_prod_recode = 0
		global intertemporal_cntry_recode = 0
		global incomplete_cntry_recode = 0
	}
	// MAJOR //
	else if "$DATASET" == "E" {
		global dropAX 	= 1
		global dropNonSITCR2 = 0 
		global adjust_hk = 1
		global intertemporal_prod_recode = 1
		global intertemporal_cntry_recode = 0
		global incomplete_cntry_recode = 0
	} 
	// MAJOR //
	else if "$DATASET" == "F" {
		global dropAX 	= 1
		global dropNonSITCR2 = 0 
		global adjust_hk = 1
		global intertemporal_prod_recode = 1
		global intertemporal_cntry_recode = 1
		global incomplete_cntry_recode = 0
	}
	else if "$DATASET" == "G" {
		global dropAX 	= 1
		global dropNonSITCR2 = 1 
		global adjust_hk = 1
		global intertemporal_prod_recode = 0
		global intertemporal_cntry_recode = 1
		global incomplete_cntry_recode = 0
	} 
/*
	else if "$DATASET" == "H" {
		global dropAX 	= 1
		global dropNonSITCR2 = 0 
		global adjust_hk = 1
		global intertemporal_prod_recode = 1
		global intertemporal_cntry_recode = 1
		global incomplete_cntry_recode = 1
	} 
	else if "$DATASET" == "I" {
		global dropAX 	= 1
		global dropNonSITCR2 = 1 
		global adjust_hk = 1
		global intertemporal_prod_recode = 0
		global intertemporal_cntry_recode = 0
		global incomplete_cntry_recode = 1
	}
*/ 	
	else {
		di "Option %DATASET not valid"
		exit
	}

	//Cleanup of Method1,Method2 Files
	global cleanup 	= 1
	 
	** -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- **

	** ----------------------------- **
	** Pre-Compile HK-China Datasets **
	** ----------------------------- **
	use "$SOURCE/china_hk88.dta", clear
	foreach year of num 89(1)99 {
		append using "$SOURCE/china_hk`year'.dta"
	}
	append using "$SOURCE/china_hk00.dta"
	save "china_hk.dta", replace

	** ------------------ ** 
	** Write Concordances **
	** ------------------ ** 
	 
	**Concord to ISO3C**
	**/meta/csv/countrycodes_to_iso3c.csv contains this listing**
	**Copied: 27-08-2014
	**Note: This is not intertemporally consistent**
	insheet using "$META/csv/countryname_to_iso3c.csv", clear names
	gen exporter = countryname
	drop countryname
	rename iso3c eiso3c
	save "exporter_to_eiso3c.dta", replace
	insheet using "$META/csv/countryname_to_iso3c.csv", clear names
	gen importer = countryname
	drop countryname
	rename iso3c iiso3c
	save "importer_to_iiso3c.dta", replace

	**Concord to SITC Revision 2 Level 2 Codes**
	**classification/meta/SITC-R2-L3-codes.csv contains this listing**
	**Copied: 28-08-2014
	infix using "$METACLASS/SITC-R2-L3-codes.dct", using("$METACLASS/SITC-R2-L3-codes.csv") clear
	save "SITC-R2-L3-codes.dta", replace

	**Concordance for Intertemporal ProductCode Adjustments for 1962 to 2000**
	**A concordance to AGGREGATE product groups to improve consistency over time between 1962 to 2000**
	**meta/csv/intertemporal_sitc3_for_1962_2000.csv**
	**Copied: 24-04-2014
	**Note: This requires "ssc install chewfile"
	chewfile using "$META/csv/intertemporal_sitc3_for_1962_2000_drop.csv", clear parse(",")
	drop in 1/1 //Get rid of "drop"
	rename var1 sitc3
	save "sitc3_intertemporal_drop.dta", replace
	chewfile using "$META/csv/intertemporal_sitc3_for_1962_2000_collapse.csv", clear parse(",")
	drop in 1/1 //get rid of "collapse"
	rename var1 sitc3
	gen sitc2 = substr(sitc3,1,2)
	drop sitc3
	duplicates drop
	save "sitc3_intertemporal_collapse.dta", replace
	chewfile using "$META/csv/intertemporal_sitc3_for_1962_2000_recode.csv", clear parse(",")
	drop in 1/1
	rename var1 sitc3
	rename var2 target
	save "sitc3_intertemporal_recode.dta", replace

	**Concordance for Intertemporal Country Adjustments for 1962 to 2000**
	**A concordance to AGGREGATE country groups to be consistent over time between 1962 to 2000**
	**/meta/csv/intertemporal_iso3c_for_1962_2000.csv contains this listing**
	**Copied: 29-08-2014
	insheet using "$META/csv/intertemporal_iso3c_for_1962_2000.csv", clear names
	rename iso3c eiso3c
	save "eiso3c_intertemporal_recodes.dta", replace
	insheet using "$META/csv/intertemporal_iso3c_for_1962_2000.csv", clear names
	rename iso3c iiso3c
	save "iiso3c_intertemporal_recodes.dta", replace

	**Concordance for Incomplete ISO3c for 1962 to 2000**
	**A concordance of countries to DROP from the dataset due to being intertemporally incomplete**
	**/meta/csv/intertemporal_iso3c_for_1962_2000.csv contains this listing**
	**Copied: 29-08-2014
	insheet using "$META/csv/incomplete_iso3c_for_1962_2000.csv", clear names
	rename iso3c eiso3c
	save "eiso3c_incomplete_recodes.dta", replace
	insheet using "$META/csv/incomplete_iso3c_for_1962_2000.csv", clear names
	rename iso3c iiso3c
	save "iiso3c_incomplete_recodes.dta", replace

	*** -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- **
	 
	 
	******************************************
	**Dataset #1: Bilateral TRADE Data  	**
	******************************************

	// Compile WTF Source Files //
	use "$SOURCE/wtf62.dta", clear
	foreach year of num 63(1)99 {
		append using "$SOURCE/wtf`year'.dta"
	}
	append using "$SOURCE/wtf00.dta"
	//save "$SOURCE/wtf.dta", replace

	format value %12.0f

	//Log Check
	**Record Total Value by Year in Log**
	preserve
	collapse (sum) value, by(year)
	list
	restore

	if $adjust_hk == 1{
		// Values //
		merge 1:1 year icode importer ecode exporter sitc4 unit dot using "china_hk.dta", keepusing(value_adj)
		replace value = value_adj if _merge == 3 | _merge == 2 													// Replace value with adjusted values if _matched Note: Only Adjustment in Values not Quantity
		drop _merge value_adj
	}

	//Special Drops from NBER FAQ //
	foreach year of num 1962(1)1964 {
		drop if year == `year' & (exporter == "Malawi" | importer == "Malawi")
		drop if year == `year' & (exporter == "Zimbabwe" | importer == "Zimbabwe")
	}

	**Split Codes to THREE DIGIT**
	gen sitc3 = substr(sitc4,1,3)
	drop sitc4
	collapse (sum) value, by(year importer exporter sitc3)
	drop if sitc3 == "" 			//Bad Data

	// Leave Country Partners Only //
	drop if importer == "World"
	drop if exporter == "World"

	merge m:1 exporter using "exporter_to_eiso3c.dta", keepusing(eiso3c)
	list if _merge == 1
	list if _merge == 2
	keep if _merge == 3 	//Keep Only Matched Items
	drop _merge
	//drop exporter
	drop if eiso3c == "."
	merge m:1 importer using "importer_to_iiso3c.dta", keepusing(iiso3c)
	list if _merge == 1
	list if _merge == 2
	keep if _merge == 3 	//Keep Only Matched Items
	drop _merge
	//drop importer
	drop if iiso3c == "."

	collapse (sum) value, by(year eiso3c iiso3c sitc3)

	if $dropAX == 1{
		gen marker = regexm(sitc3, "[AX]")
		drop if marker == 1
		drop marker
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

	if $intertemporal_prod_recode == 1{
		// Drop Products
		merge m:1 sitc3 using "sitc3_intertemporal_drop.dta"
		list if _merge == 2
		drop if _merge == 2
		drop if _merge == 3 //Drop Data Matches
		drop _merge
		// Collapse Products
		gen sitc2 = substr(sitc3,1,2)
		merge m:1 sitc2 using "sitc3_intertemporal_collapse.dta"
		replace sitc3 = sitc2+"0" if _merge == 3
		drop _merge sitc2
		collapse (sum) value, by(year eiso3c iiso3c sitc3)
		// Recode Products 
		merge m:1 sitc3 using "sitc3_intertemporal_recode.dta", keepusing(target)
		replace sitc3 = target if _merge == 3
		drop target _merge
		collapse (sum) value, by(year eiso3c iiso3c sitc3)
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

	if $dropNonSITCR2 == 1{
		merge m:1 sitc3 using "SITC-R2-L3-codes.dta", keepusing(marker)
		drop _merge
		keep if marker == 1
		drop marker
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

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
	local fl = "nberwtf_stata_trade_sitcr2l3_1962to2000_"+"$DATASET"+".dta"
	save `fl', replace


	*** -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- **


	****************************						
	**Dataset #2: Export Data **
	****************************

	**---------------------------------**
	**Method#1: Keep Country => "World"**
	**Note: these aggregations will capture NES as they are exports to the world **
	**---------------------------------**

	use "$SOURCE/wtf62.dta", clear
	foreach year of num 63(1)99 {
		append using "$SOURCE/wtf`year'.dta"
	}
	append using "$SOURCE/wtf00.dta"
	//save "$SOURCE/wtf.dta", replace

	format value %12.0f

	if $adjust_hk == 1{
		// Values //
		merge 1:1 year icode importer ecode exporter sitc4 unit dot using "china_hk.dta", keepusing(value_adj)
		replace value = value_adj if _merge == 3 | _merge == 2 													// Replace value with adjusted values if _matched Note: Only Adjustment in Values not Quantity
		drop _merge value_adj
	}

	//Special Drops from NBER FAQ //
	foreach year of num 1962(1)1964 {
		drop if year == `year' & (exporter == "Malawi" | importer == "Malawi")
		drop if year == `year' & (exporter == "Zimbabwe" | importer == "Zimbabwe")
	}

	**Split Codes to THREE DIGIT**
	gen sitc3 = substr(sitc4,1,3)
	drop sitc4
	collapse (sum) value, by(year importer exporter sitc3)
	drop if sitc3 == "" 			//Bad Data

	drop if exporter == "World"
	keep if importer == "World"
	drop importer

	merge m:1 exporter using "exporter_to_eiso3c.dta", keepusing(eiso3c)
	list if _merge == 1
	list if _merge == 2
	keep if _merge == 3 	//Keep Only Matched Items
	drop _merge
	//drop exporter
	drop if eiso3c == "." //Drops NES

	collapse (sum) value, by(year eiso3c sitc3)
	rename value value_m1

	local fl = "nberwtf_stata_export_sitcr2l3_1962to2000_"+"$DATASET"+"_method1.dta"
	save `fl', replace


	**------------------------------------------**
	**Method#2: Keep Country Pairs and Aggregate**
	**------------------------------------------**

	use "$SOURCE/wtf62.dta", clear
	foreach year of num 63(1)99 {
		append using "$SOURCE/wtf`year'.dta"
	}
	append using "$SOURCE/wtf00.dta"
	//save "$SOURCE/wtf.dta", replace

	format value %12.0f

	if $adjust_hk == 1{
		// Values //
		merge 1:1 year icode importer ecode exporter sitc4 unit dot using "china_hk.dta", keepusing(value_adj)
		replace value = value_adj if _merge == 3 | _merge == 2 													// Replace value with adjusted values if _matched Note: Only Adjustment in Values not Quantity
		drop _merge value_adj
	}
	
	//Special Drops from NBER FAQ //
	foreach year of num 1962(1)1964 {
		drop if year == `year' & (exporter == "Malawi" | importer == "Malawi")
		drop if year == `year' & (exporter == "Zimbabwe" | importer == "Zimbabwe")
	}

	**Split Codes to THREE DIGIT**
	gen sitc3 = substr(sitc4,1,3)
	drop sitc4
	collapse (sum) value, by(year importer exporter sitc3)
	drop if sitc3 == "" 			//Bad Data

	drop if exporter == "World"
	drop if importer == "World"

	merge m:1 exporter using "exporter_to_eiso3c.dta", keepusing(eiso3c)
	list if _merge == 1
	list if _merge == 2
	keep if _merge == 3 	//Keep Only Matched Items
	drop _merge
	//drop exporter
	drop if eiso3c == "."

	collapse (sum) value, by(year eiso3c sitc3)
	rename value value_m2

	local fl = "nberwtf_stata_export_sitcr2l3_1962to2000_"+"$DATASET"+"_method2.dta"
	save `fl', replace

	**
	** Compare Methods
	**
	local fl = "nberwtf_stata_export_sitcr2l3_1962to2000_"+"$DATASET"+"_method1.dta"
	merge 1:1 year sitc3 eiso3c using `fl'
	gen diff = value_m1 - value_m2
	codebook diff
	list if diff != 0

	**Note: Check These Methods are Equivalent**
	local fl = "nberwtf_stata_export_sitcr2l3_1962to2000_"+"$DATASET"+"_method2.dta"
	use `fl', clear
	//rename value_m1 value
	rename value_m2 value

	**Parse Options for Export Files**

	if $dropAX == 1{
		gen marker = regexm(sitc3, "[AX]")
		drop if marker == 1
		drop marker
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

	if $intertemporal_prod_recode == 1{
		// Drop Products
		merge m:1 sitc3 using "sitc3_intertemporal_drop.dta"
		list if _merge == 2
		drop if _merge == 2
		drop if _merge == 3 //Drop Data Matches
		drop _merge
		// Collapse Products
		gen sitc2 = substr(sitc3,1,2)
		merge m:1 sitc2 using "sitc3_intertemporal_collapse.dta"
		replace sitc3 = sitc2+"0" if _merge == 3
		drop _merge
		collapse (sum) value, by(year eiso3c sitc3)
		// Recode Products 
		merge m:1 sitc3 using "sitc3_intertemporal_recode.dta", keepusing(target)
		replace sitc3 = target if _merge == 3
		drop target _merge
		collapse (sum) value, by(year eiso3c sitc3)
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

	if $dropNonSITCR2 == 1{
		merge m:1 sitc3 using "SITC-R2-L3-codes.dta", keepusing(marker)
		drop _merge
		keep if marker == 1
		drop marker
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

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
	local fl = "nberwtf_stata_export_sitcr2l3_1962to2000_"+"$DATASET"+".dta"
	save `fl', replace

	if $cleanup == 1{
		local fl = "nberwtf_stata_export_sitcr2l3_1962to2000_"+"$DATASET"+"_method1.dta"
		rm `fl'
		local fl = "nberwtf_stata_export_sitcr2l3_1962to2000_"+"$DATASET"+"_method2.dta"
		rm `fl'
	}

	*** -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- **


	************************************ 						
	**Dataset #3: Country Import Data **
	************************************ 

	**---------------------------------**
	**Method#1: Keep Country => "World"**
	**---------------------------------**

	use "$SOURCE/wtf62.dta", clear
	foreach year of num 63(1)99 {
		append using "$SOURCE/wtf`year'.dta"
	}
	append using "$SOURCE/wtf00.dta"
	//save "$SOURCE/wtf.dta", replace

	format value %12.0f

	if $adjust_hk == 1{
		// Values //
		merge 1:1 year icode importer ecode exporter sitc4 unit dot using "china_hk.dta", keepusing(value_adj)
		replace value = value_adj if _merge == 3 | _merge == 2 													// Replace value with adjusted values if _matched Note: Only Adjustment in Values not Quantity
		drop _merge value_adj
	}

	//Special Drops from NBER FAQ //
	foreach year of num 1962(1)1964 {
		drop if year == `year' & (exporter == "Malawi" | importer == "Malawi")
		drop if year == `year' & (exporter == "Zimbabwe" | importer == "Zimbabwe")
	}

	**Split Codes to THREE DIGIT**
	gen sitc3 = substr(sitc4,1,3)
	drop sitc4
	collapse (sum) value, by(year importer exporter sitc3)
	drop if sitc3 == "" 			//Bad Data

	keep if exporter == "World"
	drop if importer == "World"
	drop exporter

	merge m:1 importer using "importer_to_iiso3c.dta", keepusing(iiso3c)
	list if _merge == 1
	list if _merge == 2
	keep if _merge == 3 	//Keep Only Matched Items
	drop _merge
	//drop exporter
	drop if iiso3c == "." //Drops NES

	collapse (sum) value, by(year iiso3c sitc3)
	rename value value_m1

	local fl = "nberwtf_stata_import_sitcr2l3_1962to2000_"+"$DATASET"+"_method1.dta"
	save `fl', replace


	**------------------------------------------**
	**Method#2: Keep Country Pairs and Aggregate**
	**------------------------------------------**

	use "$SOURCE/wtf62.dta", clear
	foreach year of num 63(1)99 {
		append using "$SOURCE/wtf`year'.dta"
	}
	append using "$SOURCE/wtf00.dta"
	//save "$SOURCE/wtf.dta", replace

	format value %12.0f

	if $adjust_hk == 1{
		// Values //
		merge 1:1 year icode importer ecode exporter sitc4 unit dot using "china_hk.dta", keepusing(value_adj)
		replace value = value_adj if _merge == 3 | _merge == 2 													// Replace value with adjusted values if _matched Note: Only Adjustment in Values not Quantity
		drop _merge value_adj
	}
	
	//Special Drops from NBER FAQ //
	foreach year of num 1962(1)1964 {
		drop if year == `year' & (exporter == "Malawi" | importer == "Malawi")
		drop if year == `year' & (exporter == "Zimbabwe" | importer == "Zimbabwe")
	}
	
	**Split Codes to THREE DIGIT**
	gen sitc3 = substr(sitc4,1,3)
	drop sitc4
	collapse (sum) value, by(year importer exporter sitc3)
	drop if sitc3 == "" 			//Bad Data

	drop if exporter == "World"
	drop if importer == "World"

	merge m:1 importer using "importer_to_iiso3c.dta", keepusing(iiso3c)
	list if _merge == 1
	list if _merge == 2
	keep if _merge == 3 	//Keep Only Matched Items
	drop _merge
	//drop exporter
	drop if iiso3c == "."

	collapse (sum) value, by(year iiso3c sitc3)
	rename value value_m2

	local fl = "nberwtf_stata_import_sitcr2l3_1962to2000_"+"$DATASET"+"_method2.dta"
	save `fl', replace

	**
	** Compare Methods
	**
	local fl = "nberwtf_stata_import_sitcr2l3_1962to2000_"+"$DATASET"+"_method1.dta"
	merge 1:1 year sitc3 iiso3c using `fl'
	gen diff = value_m1 - value_m2
	codebook diff
	list if diff != 0

	**Note: Check These Methods are Equivalent
	local fl = "nberwtf_stata_import_sitcr2l3_1962to2000_"+"$DATASET"+"_method2.dta"
	use `fl', clear
	//rename value_m1 value
	rename value_m2 value

	**Parse Options**

	if $dropAX == 1{
		gen marker = regexm(sitc3, "[AX]")
		drop if marker == 1
		drop marker
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

	if $intertemporal_prod_recode == 1{
		// Drop Products
		merge m:1 sitc3 using "sitc3_intertemporal_drop.dta"
		list if _merge == 2
		drop if _merge == 2
		drop if _merge == 3 //Drop Data Matches
		drop _merge
		// Collapse Products
		gen sitc2 = substr(sitc3,1,2)
		merge m:1 sitc2 using "sitc3_intertemporal_collapse.dta"
		replace sitc3 = sitc2+"0" if _merge == 3
		drop _merge
		collapse (sum) value, by(year iiso3c sitc3)
		// Recode Products 
		merge m:1 sitc3 using "sitc3_intertemporal_recode.dta", keepusing(target)
		replace sitc3 = target if _merge == 3
		drop target _merge
		collapse (sum) value, by(year iiso3c sitc3)
		//Log Check
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

	if $dropNonSITCR2 == 1{
		merge m:1 sitc3 using "SITC-R2-L3-codes.dta", keepusing(marker)
		drop _merge
		keep if marker == 1
		drop marker
		preserve
		collapse (sum) value, by(year)
		list
		restore
	}

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
	local fl = "nberwtf_stata_import_sitcr2l3_1962to2000_"+"$DATASET"+".dta"
	save `fl', replace

	if $cleanup == 1{

	save `fl', replace
		local fl = "nberwtf_stata_import_sitcr2l3_1962to2000_"+"$DATASET"+"_method1.dta"
		rm `fl'
		local fl = "nberwtf_stata_import_sitcr2l3_1962to2000_"+"$DATASET"+"_method2.dta"
		rm `fl'
	}

log close
	
}

