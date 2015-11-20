"""
Concordance for ISO3C to CountryName

Note
----
[1] This is derived from the countryname_to_iso3c dictionary data. 
	WARNING: Therefore '.' coded items are NOT represented in this dictionary. 
"""

from .countryname_to_iso3c import countryname_to_iso3c

def iso3c_to_countryname():
	"""
	Invert countryname_to_iso3c concordance and drop the 1:m items (i.e. '.')
	"""
	iso3c_to_countryname = {v:k for k,v in countryname_to_iso3c.items()}
	del iso3c_to_countryname['.']
	return iso3c_to_countryname 

iso3c_to_countryname = iso3c_to_countryname() 