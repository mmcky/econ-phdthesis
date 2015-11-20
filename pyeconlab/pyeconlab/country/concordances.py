"""
Country Concordances
====================

Simple Wrappers for Quickly Obtaining a Country Concordance

Available:
---------

#. iso3c_to_iso3n
#. iso3n_to_iso3c
#. iso3n_to_countryname

Future Work
-----------

#. Write a Decorator Function to Sort out source_institution (?)
#. Maybe ``source_institution`` is unnecessary and there should just be a lot of specific concordances

    - un_iso3c_to_un_countryname
    - un_iso3c_to_countryname
    - iso3c_to_iso3n is reserved for ISO 3166.xml File

"""

from .un import UNCountryCodes

#-Obtain Data Functions-#

def concordance_data(source_institution, verbose='False'):
    """
    Select source_institution and return data object

    Parameters
    ----------
    source_institution  :   string
                            A string which describes the source_institution as these lists can vary between source institutions
    verbose             :   string, option(default='False')

    Returns
    -------
    CountryCodeObject   :   Object

    """
    if source_institution == 'un':
            return UNCountryCodes(verbose=verbose)
    else:
        raise ValueError("source_institution: only 'un' is currently implemented")

#-Functions-#

def iso3c_to_iso3n(source_institution='un'):
    """
    Obtain an ISO3C to ISO3N Mapping Dictionary

    Parameters
    ----------
    source_institution  :   string, optional(default='un')
                            A string which describes the source_institution as these lists can vary between source institutions

    Returns
    -------
    iso3c_to_iso3n      :   dict(string : int)
                            A dictionary indexed by iso3c and returns iso3n

    Notes  
    -----
    This will reimport the data from the package xls file but makes a concordance very accessible. 
    If using a lot it might be better to import the UNCountryCodes object instead and query that object

    Examples
    --------

    >>> import pyeconlab
    >>> c = pyeconlab.country.concordances.iso3c_to_iso3n()
    >>> print c['AUS']
    36 

    ..  Future Work
        -----------
        [1] Have the Option to Import from a meta/iso3c_to_iso3n.py file? Current Execution time is ~20ms
    """
    return concordance_data(source_institution, verbose=False).iso3c_to_iso3n

def iso3c_to_name(source_institution='un'):
    """
    Obtain an ISO3N to ISO3C Mapping Dictionary

    Parameters
    ----------
    source_institution  :   string, optional(default='un')
                            A string which describes the source_institution as these lists can vary between source institutions

    Returns
    -------
    iso3c_to_name       :   dict(string : string)
                            A dictionary indexed by iso3c and returns a country name
    
    Notes  
    -----
    This will reimport the data from the package xls file  but makes a concordance very accessible. 
    If using a lot it might be better to import the UNCountryCodes object instead and query that object

    """
    return concordance_data(source_institution, verbose=False).iso3c_to_name

def iso3n_to_iso3c(source_institution='un'):
    """
    ISO3N to ISO3C Mapping Dictionary
    
    Parameters
    ----------
    source_institution  :   string, optional(default='un')
                            A string which describes the source_institution as these lists can vary between source institutions

    Returns
    -------
    iso3n_to_iso3c      :   dict(int : string)
                            A dictionary indexed by iso3n and returns iso3c

    Notes  
    -----
    This will reimport the data from the package xls file  but makes a concordance very accessible.
    If using a lot it might be better to import the UNCountryCodes object instead and query that object

    """
    return concordance_data(source_institution, verbose=False).iso3n_to_iso3c

def iso3n_to_name(source_institution='un'):
    """
    ISO3N to ISO3C Mapping Dictionary
    
    Parameters
    ----------
    source_institution  :   string, optional(default='un')
                            A string which describes the source_institution as these lists can vary between source institutions

    Returns
    -------
    iso3n_to_name      :    dict(int : string)
                            A dictionary indexed by iso3n and returns a country name

    Notes  
    -----
    This will reimport the data from the package xls file  but makes a concordance very accessible. 
    If using a lot it might be better to import the UNCountryCodes object instead

    """
    return concordance_data(source_institution, verbose=False).iso3n_to_name
