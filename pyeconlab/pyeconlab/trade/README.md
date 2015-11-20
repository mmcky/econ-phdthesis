Subpackage: Trade
=================

Usage:
------

NBERFeenstra dataset object

from pyeconlab.trade import NBERFeenstra

Organisation:
------------

trade
	__init__.py
	classification 					General Classifications (SITC, HS)
		iso3c.py
	concordance 					General Concordances 	(SITC to HS)
		__init__.py 				Promote to concordance level
		un

		usitc
		wits
	datasets 						Trade Datasets
		__init__.py 				Promote Basic Dataset Objects 
		NBERFeenstraWTF
			tests/
			data/ 					Contains Meta Data (i.e. Countries)
			meta.py 				Contains Meta Data
 			concordance.py 			Contains Special / Unique Concordances
			constructor.py 			Contains Dataset Constructor of Object
			dataset.py 				Contains Dataset Object
		BACI
			tests/
			data/
			...

