Country SubPackage
==================

Packages Routines for Country Specific Data (Naming Conventions, Geographic Aggregations etc)

Data in this package is arranged by source_institution. ('un' contains UN Generated DATA)

Base Objects are contained in base_files.py

External Packages:
-----------------

[1] countrycode
	
	Description: 	
	------------
		Python Package for Converting Country Codes to Other Formats.

	GitHub: 		https://github.com/vincentarelbundock/pycountrycode

	Rating: Useful

	Notes:
	------
	This Module Looks like a Simple Package that brings together some good functionality when converting between different naming conventions etc. 

	Forked: 		https://github.com/sanguineturtle/pycountrycode


[2] pycountry

	Description
	-----------
		ISO country, subdivision, language, currency and script definitions and their translations
		Draws it's data from standard ISO files

		639
		    Languages
		3166
		    Countries
		3166-3
		    Deleted countries
		3166-2
		    Subdivisions of countries
		4217
		    Currencies
		15924
		    Scripts 

	PyPI: 	https://pypi.python.org/pypi/pycountry

	BitBucket: https://bitbucket.org/gocept/pycountry

	Notes
	-----
	[1] I have used this package as an experiment with network models that contain country nodes as pycountry objects