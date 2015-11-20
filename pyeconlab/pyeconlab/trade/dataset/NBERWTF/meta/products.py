"""
NBERWTF ProductCode Meta Data
=============================

This file contains Meta Data relevant to Product Codes in NBERWTF

These decisions are informed by information found in xlsx/
and supplemented by files kept outside of the repo due to their size (phdthesis/output/results/nber/intertemporal-productcodes-sitcl4/)

"""

class intertemporal(object):
	"""
	Intertemporal / Dynamically Consistent Product Code Information

	Deletions
	---------
	0021 	[Associated only with Malta]
	0023 	[Various Eastern European Countries and Austria]
	0024 	[?]
	0025	[?]
	0031	[?]
	0035	[?]
	0039	[?]

	Recodes
	-------

	"""

	#-This doesn't list 'A' and 'X' Codes-#
	sitc4_deletions = [
		"0010",
		"0019",
		#-IN-WORK-#
	]

	sitc4_recodes = {

	}