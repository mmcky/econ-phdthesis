"""
Generic Country Code Classes and Methods
"""

__docformat__ = 'reStructuredText'

import pandas as _pd

class CountryCodes(object):
    """
    Generic for CountryCode Datasets (such as in un.py) and provides a base class of Properties and Methods for CountryCode Objects

    Notes
    -----
    #. Interface -> data attribute must contain ['iso3c', 'iso3n', 'countryname']
    #. For country data that isn't standard (like iso3c, iso3n etc.) then individual classes or methods will need to be implemented in a Child Object.

    ..  Future Work
        -----------
        1. Improve Error Handling
        2. Improve interface enforcement
    """

    data = _pd.DataFrame
    interface = ['iso3c', 'iso3n', 'countryname']

    #-Properties-#
    @property 
    def num_iso3c(self):
        """ Number of ISO3C Codes """
        return len(self.data['iso3c'].dropna())

    #-Series Properties-#

    @property 
    def iso3c(self):
        """ List of ISO3C Codes """
        return list(self.data['iso3c'].dropna())            #Some Countries don't have official iso3c codes in some incomplete datasets

    @property
    def iso3n(self):
        """ List of ISO3N Codes """
        return list(self.data['iso3n'].dropna())

    #-Generate Concordance Dictionaries-#

    @property 
    def name_to_iso3c(self):
        """ Concordance of Name to ISO3C """
        concord = self.data[['iso3c', 'countryname']].dropna().set_index('countryname')     #Drop Codes with No Pair
        return concord['iso3c'].to_dict()

    @property 
    def iso3c_to_name(self):
        """ Concordance of ISO3C to Name """
        concord = self.data[['iso3c', 'countryname']].dropna().set_index('iso3c')           #Drop Codes with No Pair
        return concord['countryname'].to_dict()

    @property 
    def iso3n_to_iso3c(self):
        """ Concordance of ISO3N to ISO3C """
        concord = self.data[['iso3c', 'iso3n']].dropna().set_index('iso3n')                 #Drop Codes with No Pair
        return concord['iso3c'].to_dict()

    @property 
    def iso3c_to_iso3n(self):
        """ Concordance of ISO3C to ISO3N """
        concord = self.data[['iso3c', 'iso3n']].dropna().set_index('iso3c')                 #Drop Codes with No Pair
        return concord['iso3n'].to_dict()

    @property 
    def name_to_iso3n(self):
        """ Concordance of Name to ISO3N """
        concord = self.data[['iso3n', 'countryname']].dropna().set_index('countryname')     #Drop Codes with No Pair
        return concord['iso3n'].to_dict()

    @property 
    def iso3n_to_name(self):
        """ Concordance of ISO3N to Name """
        concord = self.data[['iso3n', 'countryname']].dropna().set_index('iso3n')           #Drop Codes with No Pair
        return concord['countryname'].to_dict()