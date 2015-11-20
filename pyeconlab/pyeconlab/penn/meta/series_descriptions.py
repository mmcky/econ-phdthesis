"""
Series Definitions for PWT 8.1

Internally Consistency
---------------------
"countrycode" 	-> "iso3c"
"country" 		-> "countryname"

"""

series_description = {
	"iso3c"			:	r"3-letter ISO country code",
	"countryname"	:	r"Country name",
	"currency_unit"	:	r"Currency unit",
	"year"			:	r"Year",
	"rgdpe"			:	r"Expenditure-siderealGDP atchained PPPs(in mil. 2005US$)",
	"rgdpo"			:	r"Output-siderealGDP atchained PPPs(in mil. 2005US$)",
	"pop"			:	r"Population (in millions)",
	"emp"			:	r"Number of persons engaged (in millions)",
	"avh"			:	r"Average annual hours worked by persons engaged",
	"hc"			:	r"Index of human capital per person, based on years of schooling (Barro/Lee, 2012) and returns to education (Psacharopoulos, 1994)",
	"ccon"			:	r"Real consumption of households and government, at current PPPs (in mil. 2005US$)",
	"cda"			:	r"Real domestic absorption, (real consumption plus investment), at current PPPs (in mil. 2005US$)",
	"cgdpe"			:	r"Expenditure-side real GDP at current PPPs (in mil. 2005US$)",
	"cgdpo"			:	r"Output-side real GDP at current PPPs (in mil. 2005US$)",
	"ck"			:	r"Capital stock at currentPPPs(in mil. 2005US$)",
	"ctfp"			:	r"TFP level at currentPPPs(USA=1)",
	"cwtfp"			:	r"Welfare-relevant TFP levels at current PPPs (USA=1)",
	"rgdpna"		:	r"RealGDP at constant 2005 national prices (in mil. 2005US$)",
	"rconna"		:	r"Real consumption at constant 2005 national prices (in mil. 2005US$)",
	"rdana"			:	r"Real domestic absorption at constant 2005 national prices (in mil. 2005US$)",
	"rkna"			:	r"Capital stock at constant 2005 national prices (in mil. 2005US$)",
	"rtfpna"		:	r"TFP at constant national prices (2005=1)",
	"rwtfpna"		:	r"Welfare-relevant TFP at constant national prices (2005=1)",
	"labsh"			:	r"Share of labour compensation in GDP at current national prices",
	"delta"			:	r"Average depreciation rate of the capital stock",
	"xr"			:	r"Exchange rate, national currency/USD (market+estimated)",
	"pl_con"		:	r"Price level of CCON (PPP/XR), price level of USA GDPo in 2005=1",
	"pl_da"			:	r"Price level of CDA (PPP/XR), price level of USA GDPo in 2005=1",
	"pl_gdpo"		:	r"Price level of CGDPo (PPP/XR),  price level of USA GDPo in 2005=1",
	"i_cig"			:	r"0/1/2: relative price data for consumption, investment and government is extrapolated (0), benchmark (1) or interpolated (2)",
	"i_xm"			:	r"0/1/2: relative price data for exports and imports is extrapolated (0), benchmark (1) or interpolated (2)",
	"i_xr"			:	r"0/1: the exchange rate is market-based (0) or estimated (1)",
	"i_outlier"		:	r"0/1: the observation on pl_gdpe or pl_gdpo is not an outlier (0) or an outlier (1)",
	"cor_exp"		:	r"Correlation between expenditure shares of the country and the US (benchmark observations only)",
	"statcap"		:	r"Statistical capacity indicator (source: World Bank, developing countries only)",
	"csh_c"			:	r"Share of household consumption at current PPPs",
	"csh_i"			:	r"Share of gross capital formation at current PPPs",
	"csh_g"			:	r"Share of government consumption at current PPPs",
	"csh_x"			:	r"Share of merchandise exports at current PPPs",
	"csh_m"			:	r"Share of merchandise imports at current PPPs",
	"csh_r"			:	r"Share of residual trade and GDP statistical discrepancy at current PPPs",
	"pl_c"			:	r"Price level of household consumption,  price level of USA GDPo in 2005=1",
	"pl_i"			:	r"Price level of capital formation,  price level of USA GDPo in 2005=1",
	"pl_g"			:	r"Price level of government consumption,  price level of USA GDPo in 2005=1",
	"pl_x"			:	r"Price level of exports, price level of USA GDPo in 2005=1",
	"pl_m"			:	r"Price level of imports, price level of USA GDPo in 2005=1",
	"pl_k"			:	r"Price level of the capital stock, price level of USA in 2005=1",
}