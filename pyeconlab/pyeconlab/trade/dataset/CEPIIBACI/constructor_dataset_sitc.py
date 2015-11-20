"""
BACI (Self Contained) SITC Dataset Function
===========================================

STATUS: IN-TESTING

TODO => check_concordance

An SITC Dataset Constructor for the BACI Constructor Class

Testing
-------
1. TBD

"""

import pandas as pd
import numpy as np
from pyeconlab.util import concord_data


def construct_sitc(data, data_classification, data_type, level, revision, check_concordance=True, adjust_units=False, concordance_institution="un", multiindex=False, verbose=True):
    """
    A Self Contained Function for Producing SITC Datasets from the BACI Dataset
    **Note:** Self Contained methods reduce the Need to Debug other routines and methods.
    The other constructor methods are however useful to diagnose issues and to understand properties of the dataset

    Parameters
    ----------
    data            :   pd.DataFrame 
                        Pandas DataFrame that contains RAW BACI HS data
    data_type       :   str
                        Specify data type: 'trade', 'export', 'import'
                        'export' will include values from a country to any region (including NES, and Nuetral Zone etc.)
                        'import' will include values to a country from any region (including NES, and Nuetral Zone etc.)
    level           :   int 
                        Specify SITC Chapter Level (1,2,3,4 or 5)
    revision        :   int 
                        Specify which SITC Revision to use (Revision 1,2,3, or 4)
    check_concordance : bool, optional(default=True)
                        Check Concordance for Full Matches to ensure no orphaned data
    adjust_units    :   bool, optional(default=False)
                        Adjust units to $'s from 1000 of $'s. Default is to keep the base dataset values in 1000's
    concordance_institution     :   str, optional(default="un")
                                    Specify which institution to use for product concordance information
    multiindex      :   bool, optional(default=False)
                        Return dataset with a multi-index object

    Notes
    -----
    1. Will need to consider the source BACI dataset HS96 etc. to fetch the correct Concordance
    2. When joining with other datasets, global merge attributes should be considered externally to this method
    3. Currently this excludes Quantity Information

    """

    #-Helper Functions-#
    def merge_iso3c_and_replace_iso3n(data, cntry_data, column):
        " Merge ISO3C and Replace match on column (i.e. eiso3n)"
        data = data.merge(cntry_data, how='left', left_on=[column], right_on=['iso3n'])
        del data['iso3n']
        del data[column]
        data.rename(columns={'iso3c' : column[0:-1]+'c'}, inplace=True)
        return data

    def dropna_iso3c(data, column):
        " Drop iiso3c or eiso3c isnull() values "
        if column == 'iiso3c':
            data.drop(data.loc[(data.iiso3c.isnull())].index, inplace=True)
        elif column == 'eiso3c':
            data.drop(data.loc[(data.eiso3c.isnull())].index, inplace=True)
        return data

    def check_concordance_helper(data, level):
        check = data.loc[data['sitc%s'%level].isnull()]
        if len(check) > 0:
            raise ValueError("Concordance doesn't provide match for the following products: %s" % (check.hs6.unique()))

    #-Obtain Key Index Variables-#
    data.rename(columns={'t' : 'year', 'i' : 'eiso3n', 'j' : 'iiso3n', 'v' : 'value', 'q': 'quantity'}, inplace=True)   #'hs6' is unchanged
    #-Exclude Quantity-#
    del data['quantity']
    #-Import Country Codes to ISO3C-#
    from pyeconlab.trade.dataset.CEPIIBACI.meta import hs96_iso3n_to_iso3c          #This doesn't include np.nan - is this going to be an issue?
    hs96_iso3n_to_iso3c = {int(k):v for k,v in hs96_iso3n_to_iso3c.items()}
    cntry_data = pd.Series(hs96_iso3n_to_iso3c).to_frame().reset_index()
    cntry_data.columns = ['iso3n', 'iso3c']
    cntry_data = cntry_data.sort(columns=['iso3n'])
    #-Import Product Concordance-#
    from pyeconlab.trade.concordance import HS_To_SITC
    concordance = HS_To_SITC(hs=data_classification, sitc="SITCR%s"%revision, hs_level=6, sitc_level=level, source_institution=concordance_institution, verbose=verbose).concordance
    #-Add Special Cases to the concordance-#
    
    # from .base import BACI
    # for k,v in BACI.adjust_hs6_to_sitc[data_classification].items():  #This Needs a Level Consideration
    #     concordance[k] = v
    
    #-Parse Options-#
    #-Change Value Units-#
    if adjust_units:
        data['value'] = data['value']*1000                     
    #-Collapse Trade Data based on data option-#
    if data_type == "trade":
        #-Merge in ISO3C-#
        data = merge_iso3c_and_replace_iso3n(data, cntry_data, column='eiso3n')
        data = merge_iso3c_and_replace_iso3n(data, cntry_data, column='iiso3n')
        print "[WARNING] Dropping Countries where iso3c has null() values will remove country export/import from NES, and other regions!"
        data = dropna_iso3c(data, column='eiso3c')
        data = dropna_iso3c(data, column='iiso3c')
        #-Merge in SITCR2 Level 3-#
        data['sitc%s'%level] = data['hs6'].apply(lambda x: concord_data(concordance, x, issue_error=np.nan))
        if check_concordance:
            check_concordance_helper(data, level)
        del data['hs6']
        data = data.groupby(['year', 'eiso3c', 'iiso3c', 'sitc%s'%level]).sum()
        print "[Returning] BACI HS96 Source => TRADE data for SITCR%sL%s with ISO3C Countries" % (revision, level)
    elif data_type == "export" or data_type == "exports":
        #-Export Level-#
        del data['iiso3n']
        data = data.groupby(['year', 'eiso3n', 'hs6']).sum().reset_index()
        #-Merge in ISO3C-#
        data = merge_iso3c_and_replace_iso3n(data, cntry_data, column='eiso3n')
        data = dropna_iso3c(data, column='eiso3c')
        #-Merge in SITCR2 Level 3-#
        data['sitc%s'%level] = data['hs6'].apply(lambda x: concord_data(concordance, x, issue_error=np.nan))
        if check_concordance:
            check_concordance_helper(data, level)
        del data['hs6']
        data = data.groupby(['year', 'eiso3c', 'sitc%s'%level]).sum()
        print "[Returning] BACI HS96 Source => EXPORT data for SITCR%sL%s with ISO3C Countries" % (revision, level)
    elif data_type == "import" or data_type == "imports":
        #-Import Level-#
        del data['eiso3n']
        data = data.groupby(['year', 'iiso3n', 'hs6']).sum().reset_index()
        #-Merge in ISO3C-#
        data = merge_iso3c_and_replace_iso3n(data, cntry_data, column='iiso3n')
        data = dropna_iso3c(data, column='iiso3c')
        #-Merge in SITCR2 Level 3-#
        data['sitc%s'%level] = data['hs6'].apply(lambda x: concord_data(concordance, x, issue_error=np.nan))
        if check_concordance:
            check_concordance_helper(data, level)
        del data['hs6']
        data = data.groupby(['year', 'iiso3c', 'sitc%s'%level]).sum()
        print "[Returning] BACI HS96 Source => IMPORT data for SITCR%sL%s with ISO3C Countries" % (revision, level)
    else:
        raise ValueError("'data' must be 'trade', 'export', or 'import'")
    #-Data-#
    if not multiindex:
        data = data.reset_index()
    return data
