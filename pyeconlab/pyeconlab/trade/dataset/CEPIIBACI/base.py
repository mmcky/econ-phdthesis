"""
BACI Dataset Base Metadata Class
================================

Provides meta data for the BACI data object

"""

import numpy as np

class BACI(object):
    """
    BACI Meta Class for Trade, Export and Import Objects

    This can be inherited by Constructor and Dataset Classes to Import Class Metadata 

    Attributes
    ----------
    see below

    Notes
    -----
    1. Years Available: ('HS92' : 1995 to 2011, 'HS96' : 1998 to 2011, 'HS02' : 2003 to 2011)
    2. Classification Revisions: {'HS92' : '1992', 'HS96' : '1996', 'HS02' : '2002'}  
    3. Updatable attributes allow easy updating to new dataset releases from CEPII
    4. Supplementary Data is available: country_concordance and product_classification
    """

    #-Updatable Attributes-#
    END_YEAR = { 'HS92' : 2012, 'HS96' : 2012, 'HS02' : 2011 } 

    #-Source Attributes-#
    source_name                     = u'CEPII BACI Trade Dataset'
    source_classification           = u"HS"
    source_revision                 = {'HS92' : '1992', 'HS96' : '1996', 'HS02' : '2002'} 
    source_level                    = 6
    source_available_years          = {'HS92' : xrange(1995, END_YEAR['HS92']+1, 1), 'HS96' : xrange(1998, END_YEAR['HS96']+1, 1), 'HS02' : xrange(2003, END_YEAR['HS02']+1,1)} 
    source_available_classification = ['HS92', 'HS96', 'HS02'] 
    source_web                      = u"http://www.cepii.fr/cepii/en/bdd_modele/presentation.asp?id=1"
    source_last_checked             = np.datetime64('2014-09-03')
    source_units_value              = 1000
    source_units_value_str          = "US$1000's"
    source_interface                = {'t' : 'year', 'i' : 'eiso3n', 'j' : 'iiso3n', 'v' : 'value', 'q' : 'quantity'}
    source_deletions                = {'HS92' : '', 'HS96' : '', 'HS02' : 'a'} 

    #-Meta Data-#

    #-Dataset Country Meta Files-#
    country_data_fn = {
        'HS92'  : 'country_code_baci92.csv',
        'HS96'  : 'country_code_baci96.csv',
        'HS02'  : 'country_code_baci02.csv'
    }
    #-Dataset Product Meta Files-#
    product_data_fn = {
        'HS92'  : 'product_code_baci92.csv',
        'HS96'  : 'product_code_baci96.csv',
        'HS02'  : 'product_code_baci02.csv'
    }

    #-HDF File Versions-#
    raw_data_hdf_fn = {
        'HS92' : 'baci92_1995_%s.h5' % (END_YEAR['HS92']),
        'HS96' : 'baci96_1998_%s.h5' % (END_YEAR['HS96']),
        'HS02' : "baci02_2003_%s.h5"%(END_YEAR['HS02'])
    }
    raw_data_hdf_yearindex_fn = {
        'HS92' : 'baci92_1995_%s_yearindex.h5' % (END_YEAR['HS92']),
        'HS96' : 'baci96_1998_%s_yearindex.h5' % (END_YEAR['HS96']),
        'HS02' : "baci02_2003_%s_yearindex.h5"%(END_YEAR['HS02'])
    }

    #-Deletions to Remove Non-Country Entries by ISO3C-#
    country_only_iso3c_deletions = {
        'HS92' : ['NTZ'],               #Check this!
        'HS96' : ['NTZ'],               #Check this!        
        'HS02' : ['NTZ']                #Neutral Zone, Documented meta/hs02_iso3n_to_iso3c.py
    }

    #-Deletions to Remove Non-Country Entries by ISO3N-#
    country_only_iso3n_deletions = {
        'HS92' : [536],                 #Check this!
        'HS96' : [536],                 #Check this!
        'HS02' : [536],                 #Neutral Zone, Documented meta/hs02_iso3n_to_iso3c.py
    }

    #-Documented Adjustments for Official to BACI HS6 ProductCodes-#
    adjust_officialhs_to_hs6 = {
        'HS02' :    {   '271011' : '271000',    #Oil Collapsed in BACI Data
                        '271019' : '271000',    #Oil Collapsed in BACI Data
                        '271091' : '271000',    #Oil Collapsed in BACI Data
                        '271099' : '271000',    #Oil Collapsed in BACI Data
                        '710820' : '.',         #Not found in BACI
                        '711890' : '.',         #Not found in BACI
                        '999999' : '.',         #Not found in BACI
                    }
    }

    #-Documented Adjustments for hs6 to sitc3 (level 3)-#
    #-Note: update to different levels-#
    adjust_hs6_to_sitc = {
        'HS92' :    {'271000' : '334'},
        'HS96' :    {'271000' : '334'},
        'HS02' :    {'271000' : '334'},
    }