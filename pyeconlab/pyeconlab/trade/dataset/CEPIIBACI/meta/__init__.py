"""
Subpackage: CEPIBACI Meta Data
"""

from .hs96_iso3n_to_name import iso3n_to_name as hs96_iso3n_to_name
from .hs02_iso3n_to_name import iso3n_to_name as hs02_iso3n_to_name

from .hs96_iso3n_to_iso3c import iso3n_to_iso3c as hs96_iso3n_to_iso3c
from .hs02_iso3n_to_iso3c import iso3n_to_iso3c as hs02_iso3n_to_iso3c

#Build a HS Classification Dict#

iso3n_to_iso3c = {
	'HS96' : hs96_iso3n_to_iso3c,
	'HS02' : hs02_iso3n_to_iso3c,
	}

iso3n_to_name = {
	'HS96' : hs96_iso3n_to_name,
	'HS02' : hs02_iso3n_to_name,
}