"""
Meta Subpackage for WDI
"""


class WDISeriesCodes():
	"""
	Container for Easy to Remember WDI Series Codes (World Bank)
	"""
	Population = r'SP.POP.TOTL'
	#-GDP-#
	GDP = r'NY.GDP.MKTP.CD'
	#-GDP Per Capita-#
	GDPPC = r'NY.GDP.PCAP.CD'
	GDPPCPPP = r'NY.GDP.PCAP.PP.CD'
	GDPPCPPPConst = r'NY.GDP.PCAP.PP.KD'
	#-GDP Growth-#
	GDPGrowth = r'NY.GDP.MKTP.KD.ZG'
	GDPPCGrowth = r'NY.GDP.PCAP.KD.ZG'
	#-GNI-#
	GNIAtlas = r'NY.GNP.ATLS.CD'
	GNIPPP = r'NY.GNP.MKTP.PP.CD'
	#-GNI Per Capita-#
	GNIPCAtlas = r'NY.GNP.PCAP.CD'
	#-Terms of Trade-#
	NetBarterTot = r'TT.PRI.MRCH.XD.WD'
	#-Infrastructure-#
	AirDepartures = r'IS.AIR.DPRT'
	RailLinesKm = r'IS.RRS.TOTL.KM'
	ElectricityUsePC = r'EG.USE.ELEC.KH.PC'
	#-Geography-#
	LandArea = r'AG.LND.TOTL.K2'

CodeToName = {
	#-Population-#
	r'SP.POP.TOTL' 			: 'TotalPop',
	#-GDP-#
	r'NY.GDP.MKTP.CD' 		: 'GDP',
	r'NY.GDP.PCAP.CD' 		: 'GDPPC',
	r'NY.GDP.PCAP.KD'		: 'GDPPCConst',
	r'NY.GDP.MKTP.KD.ZG' 	: 'GDPGrowth',
	r'NY.GDP.PCAP.KD.ZG' 	: 'GDPPCGrowth',
	r'NY.GDP.PCAP.PP.CD' 	: 'GDPPCPPP',
	r'NY.GDP.PCAP.PP.KD'	: 'GDPPCPPPConst',
	#-GNP-#
	r'NY.GNP.ATLS.CD' 		: 'GNIAtlas',
	r'NY.GNP.MKTP.PP.CD' 	: 'GNIPPP',
	r'NY.GNP.PCAP.CD' 		: 'GNIPCAtlas',
	#-Terms of Trade-#
	r'TT.PRI.MRCH.XD.WD' 	: 'NetBarterTot',
	#-Infrastructure-#
	r'IS.AIR.DPRT'			: 'AirDepartures',
	r'IS.RRS.TOTL.KM' 		: 'RailLinesKm',
	r'EG.USE.ELEC.KH.PC' 	: 'ElectricityUsePC',
	#-Geography-#
	r'AG.LND.TOTL.K2' 		: 'TotalLandArea',

}