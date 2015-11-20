"""
Setup File for PyEconLab

Future Work
-----------
[1] Add Install Dependancies
"""

from distutils.core import setup

setup(
  name = 'pyeconlab',
  packages = [ 'pyeconlab',
               'pyeconlab.country',
  			       'pyeconlab.util',
  			       'pyeconlab.trade',
  			       'pyeconlab.trade.dataset',
               'pyeconlab.trade.dataset.CEPIIBACI',
               'pyeconlab.trade.dataset.CEPIIBACI.meta',
               'pyeconlab.trade.dataset.NBERWTF',
               'pyeconlab.trade.dataset.NBERWTF.meta', 
               'pyeconlab.trade.dataset.CIDATLAS',
               'pyeconlab.trade.dataset.CIDATLAS.meta',
               'pyeconlab.trade.dataset.meta',
               'pyeconlab.trade.classification',
               'pyeconlab.trade.concordance',
               'pyeconlab.trade.systems',
               'pyeconlab.trade.util',
               'pyeconlab.trade.research',
  			       'pyeconlab.wdi',
               'pyeconlab.wdi.meta',
               'pyeconlab.penn',
               'pyeconlab.penn.meta',
  			  ], 
  package_data =  { 'pyeconlab.country' : ['data/*.xls', 'data/*.xml'],
                    'pyeconlab.trade.dataset.CEPIIBACI.meta' : ['meta/*.py', 'meta/curated/*.csv'],
                    'pyeconlab.trade.dataset.NBERWTF' : ['data/*.py'],
                    'pyeconlab.trade.dataset.NBERWTF.meta' : ['meta/*.py', 'meta/csv/*.csv'],
                    'pyeconlab.trade.classification'  :   ['data/un/*.txt', 'data/wits/*.xls'], 
                    'pyeconlab.trade.concordance'      :  ['data/un/*.csv'],  
                  },
  version = '0.1-alpha',
  description = 'Python Package For Economists',
  author = 'Matthew McKay',
  author_email = 'mamckay@gmail.com',
  url = 'https://github.com/sanguineturtle/pyeconlab',
  download_url = 'https://github.com/sanguineturtle/pyeconlab/tarball/0.1-alpha', 
  keywords = ['quantitative', 'economics', 'international trade', 'complexity'],
  #install_requires = ['countrycode', 'rarfile'],
)
