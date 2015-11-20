PyEconLab 
===========

Note: 	This is a static copy of `pyeconlab` used to support thesis work.
		Futher development will occur in the pyeconlab repository
		This is placed here for reference purposes only

Python Economics Laboratory
---------------------------

This package contains methods and routines for conducting research with a primary focus on the field of Economics. The current primary focus is on trade and most develpment is occuring in the trade supackage. However in the future other subpackages can be added as research agenda develops. 

Import Convention: ``import pyeconlab as el``

Usage
-----

	from pyeconlab.trade import CountryLevelExportSystem
	c = CountryLevelExportSystem(source_dir='data/', fl='somedata.csv')
	c.countries
	... etc


Organisation
------------

The basic organisation of the project is:

	pyecontrade
		tests/ 							Tests for Package
		country/ 						Country Subpackge
			__init__.py
			aggregates/
				ldc.py
				...
			iso3c.py
			iso2c.py
			iso3n.py
			...
		trade/ 							Subpackage: International Trade
			classification/ 			Subpackage: Product Information include classification tables of HS, SITC, HH Communities
				data/	 				Raw Data for Classifications
				__init__.py
				classification.py
				sitc.py
				hs.py
			/concordance 				Subpackage: Concordances and Correlation Tables
				__init__.py 			Promote BEST one to top level
				un/
				wits/
				usitc/
				concordance.py
			/dataset     	 			DataSet Constructors & Compilation from RAW data		
				__init__.py 				Promote Basic Dataset Objects 
				NBERFeenstraWTF
					__init__.py
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
			/tests						Tests for SubModule	
			__init__.py
			CountryLevelExportSystem.py
			CountryLevelExportNetwork.py
			ProductLevelExportSystem.py
			ProductLevelExportNetwork.py

		?? - FUTURE - ??

			/cplevel 						Country, Product Level Trade Systems
				ProductLevelExportSystem.py
				ProductLevelExportNetwork.py
				... etc
			/clevel 						Country Level Trade Systems
				CountryLevelExportSystem.py
				CountryLevelExportNetwork.py
		/wdi
			/data
			__init__.py
			WDI.py
		__init__.py
		setup.py
		README.md

Data
----

This project only currently supports RAW data that resides outside of the package (due to the large size of most datasets)however data such as Concordances & Aggregations are included (typically found in the relevant subpackage as a csv file). Data files should be encoded in (.csv) to be tool neutral, relatively efficient, and simple!

References
----------

This is a collection of useful references:

Python Standards

  1. [Docstring Conventions - PEP 257](https://www.python.org/dev/peps/pep-0257/)
  1. [Style Guide for Python Code - PEP 8](https://www.python.org/dev/peps/pep-0008/)

Scientific Python

  1. [NumPy Documentation](https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt)

Code Checking

  1. [Flake8 (pyflake, pep8)](https://pypi.python.org/pypi/flake8)