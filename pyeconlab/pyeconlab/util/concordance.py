"""
Concordance Utilities
=====================

A set of Concordance Utilities
"""

import pandas as _pd
import numpy as _np

import countrycode as _cc       #This is the external: PyCountryCode Project

def countryname_concordance(data, concord_vars=('countryname', 'iso3c'), rtn_type='series', verbose=False):
    """
    Compute a Country Name Concordance using package: ``pycountrycode``
    
    Parameters
    ----------
    data            :   list(string)
                        List of Country Names
    concord_vars    :   tuple(string, string), optional(default=('countryname', iso3c'))
                        Specify variables to Concord
    rtn_type        :   str, optional(default='series')
                        Return Type (dict or indexed Pandas Series)

    Notes
    ------
    ``pycountrycode`` is going through a re-write so this will most likely break                  

    ..  Future Work
        -----------
        [1] Build Internal CountryCode Routines
    """
    def replace_nonstring(items):
        for idx, code in enumerate(items):
            if type(code) != str:
                items[idx] = '.'
        return items

    def reject_non3digit(items):
        for idx, code in enumerate(items):
            if len(code) != 3:
                items[idx] = '.'
        return items

    if type(data) != list:
        raise TypeError("data: needs to be a list")
    #-Find Set Of Countries-#
    if type(data) == list:
        countrynames = list(set(data))
    if concord_vars[1] == 'iso3c':
        iso3c = _cc.countrycode(codes=countrynames, origin='country_name', target='iso3c')
        iso3c = replace_nonstring(iso3c)                                                    #Could use lambda functions
        iso3c = reject_non3digit(iso3c)                                                     #Could use lambda functions
        concord = _pd.Series(iso3c, index=countrynames, name='iso3c')
        concord.index.name = 'countryname'
        concord.sort()
    if concord_vars[1] == 'iso3n':
        iso3n = _cc.countrycode(codes=countrynames, origin='country_name', target='iso3n')
        iso3n = replace_nonstring(iso3n)
        iso3n = reject_non3digit(iso3n)
        concord = _pd.Series(iso3n, index=countrynames, name='iso3n')
        concord.index.name = 'countryname'
        concord.sort()
    #-Parse rtn_type-#
    if rtn_type == 'series':
        return concord
    else:
        return concord.to_dict()


def concord_data(concordance, item, issue_error=True, exception=False):
    """
    Function for Parsing Concordance Dictionaries
    
    Parameters
    ----------
    concordance     :   dict
                        A concordance dictionary object
    item            :   dict key
                        Some key contained in the dictionary
    issue_error     :   bool or value, optional(default=True)
                        Can set a Flag to Issue an Error or Not or Supply a value to return if an error occurs.
    exception       :   bool, optional(default=False)
                        Flag to Issue an Exception or Not. 

    Raises
    ------
    ValueError
        if option is specified and an error is found when looking for a matching key in the concordance
    
    Notes
    -----
    #. If error is set to False, then ``np.nan`` is used if key is not found in the concordance        

    """
    try:
        return concordance[item]
    except:
        if exception == True:
            raise ValueError('[Error] %s is not found in the concordance' % item)
        if issue_error == True:
            return _np.nan
        elif issue_error == False:
            return item
        else:
            return issue_error  # Can Specify a Return Code