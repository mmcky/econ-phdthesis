"""
NBER (Self Contained) Dataset Functions [SITC Revision 2 Level 1]

Source Dataset: SITC R2 (Quasi) Level 4

Testing
-------
1.  Compare with Generalised Version of this Function
    tests/test_constructor_dataset_sitcr2

Notes
-----
1. Self Contained Compilation Reduces the Need to Debug many other routines. 
2. Use the Constructor class to explore the raw data 

"""

#-Library Imports-#
import re
#-Package Imports-#
from pyeconlab.trade.classification import SITC
from pyeconlab.util import concord_data, merge_columns
#-Relative Imports-#
from .meta import countryname_to_iso3c, iso3c_recodes_for_1962_2000, incomplete_iso3c_for_1962_2000 
            

#-Dataset Constructors-#

#-SITC Revision 2 Level 1-#

def construct_sitcr2l1(df, data_type, dropAX=True, sitcr2=True, drop_nonsitcr2=True, adjust_hk=(False, None), intertemp_cntrycode=False, drop_incp_cntrycode=False, adjust_units=False, source_institution='un', verbose=True):
        """
        Construct a Self Contained (SC) Direct Action Dataset for Countries at the SITC Revision 2 Level 1
        
        There are no checks on the incoming dataframe to ensure data integrity.
        This is your responsibility

        Parameters
        ----------
        df                  :   DataFrame
                                Pandas DataFrame containing the raw data
        data_type           :   str
                                Specify what type of data 'trade', 'export', 'import'
        dropAX              :   bool, optional(default=True)
                                Drop AX Codes 
        sitcr2              :   bool, optional(default=True)
                                Add SITCR2 Indicator
        drop_nonsitcr2      :   bool, optional(default=True)
                                Drop non-standard SITC2 Codes
        adjust_hk           :   Tuple(bool, df), optional(default=(False, None))
                                Adjust the Hong Kong Data using NBER supplemental files which needs to be supplied as a dataframe
        intertemp_cntrycode :   bool, optional(default=False)
                                Generate Intertemporal Consistent Country Units (from meta)
        drop_incp_cntrycode :   bool, optional(default=False)
                                Drop Incomplete Country Codes (from meta)
        adjust_units        :   bool, optional(default=False)
                                Adjust units by a factor of 1000 to specify in $'s
        source_institution  :   str, optional(default='un')
                                which institutions SITC classification to use

        Notes
        -----
        1. Operations ::

            [1] Adjust Hong Kong and China Data
            [2] Drop SITC4 to SITC2 Level (for greater intertemporal consistency)
            [3] Import ISO3C Codes as Country Codes
            [4] Drop Errors in SITC2 codes ["" Codes]
            
            Optional:
            ---------
            [A] Drop sitc1 codes that contain 'A' and 'X' codes [Default: True]
            [B] Drop Non-Standard SITC3 Codes [i.e. Aren't in the Classification]
            [C] Adjust iiso3c, eiso3c country codes to be intertemporally consistent
            [D] Drop countries with incomplete data across 1962 to 2000 (strict measure)
  

        2. This makes use of countryname_to_iso3c in the meta data subpackage

        ..  Future Work
            -----------
            1. Check SITC Revision 2 Official Codes
            2. Add in a Year Filter
        """
        
        #-Operations Requiring RAW SITC Level 4-#
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

        idx = [u'year', u'icode', u'importer', u'ecode', u'exporter', u'sitc4', u'unit', u'dot']

        #-Hong Kong China Data Adjustment Option-#
        if adjust_hk[0]:
            if verbose: print "[INFO] Adjusting Hong Kong and China Values"
            hkdata = adjust_hk[1]
            #-Values-#
            raw_value = df[idx+['value']].rename(columns={'value' : 'value_raw'})
            try:
                adjust_value = hkdata[idx+['value_adj']]
            except:
                raise ValueError("[ERROR] China/Hong Kong Data has not been passed in properly!")
            #-Note: Current merge_columns utility merges one column set at a time-#
            df = merge_columns(raw_value, adjust_value, idx, collapse_columns=('value_raw', 'value_adj', 'value'), dominant='right', output='final', verbose=verbose)
            #-Note: Adjust Quantity has not been implemented. See NBERWTF constructor -#

        #-Set Data-#
        idx = ['year', 'exporter', 'importer', 'sitc4']
        df = df.loc[:,idx + ['value']]

        #-Adjust to SITC Level 2-#
        if verbose: print "[INFO] Collapsing to SITC Level 1 Data"
        df['sitc1'] = df.sitc4.apply(lambda x: x[0:1])
        df = df.groupby(['year', 'exporter', 'importer', 'sitc1']).sum()['value'].reset_index()
        
        #-Operations at SITC Level 3-#
        #~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

        #-Countries Only Adjustment-#
        if verbose: print "[INFO] Removing 'World' values from the dataset to be country only data"
        df = df.loc[(df.exporter != "World") & (df.importer != "World")]
        
        #-Add Country ISO Information-#
        #-Exports (can include NES on importer side)-#
        if data_type == 'export' or data_type == 'exports':
            if verbose: print "[INFO] Adding eiso3c using nber meta data"
            df['eiso3c'] = df.exporter.apply(lambda x: countryname_to_iso3c[x])
            df = df.loc[(df.eiso3c != '.')]
            df = df.groupby(['year', 'eiso3c', 'sitc1']).sum()['value'].reset_index()
        #-Imports (can include NES on importer side)-#
        elif data_type == 'import' or data_type == 'imports':
            if verbose: print "[INFO] Adding iiso3c using nber meta data"
            df['iiso3c'] = df.importer.apply(lambda x: countryname_to_iso3c[x])
            df = df.loc[(df.iiso3c != '.')]
            df = df.groupby(['year','iiso3c', 'sitc1']).sum()['value'].reset_index()
        #-Trade-#
        else: 
            if verbose: print "[INFO] Adding eiso3c and iiso3c using nber meta data"
            df['iiso3c'] = df.importer.apply(lambda x: countryname_to_iso3c[x])
            df['eiso3c'] = df.exporter.apply(lambda x: countryname_to_iso3c[x])
            df = df.loc[(df.iiso3c != '.') & (df.eiso3c != '.')]
            df = df.groupby(['year', 'eiso3c', 'iiso3c', 'sitc1']).sum()['value'].reset_index()
        
        #-Remove Product Code Errors in Dataset-#
        df = df.loc[(df.sitc1 != "")]                                                                   #Does this need a reset_index?
        #-dropAX-#
        if dropAX:
            if verbose: print "[INFO] Dropping SITC Codes with 'A' or 'X'"
            df['AX'] = df.sitc1.apply(lambda x: 1 if re.search("[AX]", x) else 0)
            df = df.loc[df.AX != 1]
            del df['AX']               #No Longer Required
        
        #-Official SITCR2 Codes-#
        if sitcr2:
            if verbose: print "[INFO] Adding SITCR2 Indicator"
            sitc = SITC(revision=2, source_institution=source_institution)
            codes = sitc.get_codes(level=1)
            df['sitcr2'] = df['sitc1'].apply(lambda x: 1 if x in codes else 0)
            if drop_nonsitcr2:
                if verbose: print "[INFO] Dropping Non Standard SITCR2 Codes"
                df = df.loc[(df.sitcr2 == 1)]
                del df['sitcr2']                #No Longer Needed
        
        #-Adjust Country Codes to be Intertemporally Consistent-#
        if intertemp_cntrycode:
            #-Export-#
            if data_type == 'export' or data_type == 'exports':
                if verbose: print "[INFO] Imposing dynamically consistent eiso3c recodes across 1962-2000"
                df['eiso3c'] = df['eiso3c'].apply(lambda x: concord_data(iso3c_recodes_for_1962_2000, x, issue_error=False))    #issue_error = false returns x if no match
                df = df[df['eiso3c'] != '.']
                df = df.groupby(['year', 'eiso3c', 'sitc1']).sum().reset_index()
            #-Import-#
            elif data_type == 'import' or data_type == 'imports':
                if verbose: print "[INFO] Imposing dynamically consistent iiso3c recodes across 1962-2000"
                df['iiso3c'] = df['iiso3c'].apply(lambda x: concord_data(iso3c_recodes_for_1962_2000, x, issue_error=False))    #issue_error = false returns x if no match
                df = df[df['iiso3c'] != '.']
                df = df.groupby(['year', 'iiso3c', 'sitc1']).sum().reset_index()
            #-Trade-#
            else:
                if verbose: print "[INFO] Imposing dynamically consistent iiso3c and eiso3c recodes across 1962-2000"
                df['iiso3c'] = df['iiso3c'].apply(lambda x: concord_data(iso3c_recodes_for_1962_2000, x, issue_error=False))    #issue_error = false returns x if no match
                df['eiso3c'] = df['eiso3c'].apply(lambda x: concord_data(iso3c_recodes_for_1962_2000, x, issue_error=False))    #issue_error = false returns x if no match
                df = df[df['iiso3c'] != '.']
                df = df[df['eiso3c'] != '.']
                df = df.groupby(['year', 'eiso3c', 'iiso3c', 'sitc1']).sum().reset_index()
        
        #-Drop Incomplete Country Codes-#
        if drop_incp_cntrycode:
            if verbose: print "[INFO] Dropping countries with incomplete data across 1962-2000"
            #-Export-#
            if data_type == 'export' or data_type == 'exports':
                df['eiso3c'] = df['eiso3c'].apply(lambda x: concord_data(incomplete_iso3c_for_1962_2000, x, issue_error=False))     #issue_error = false returns x if no match
                df = df[df['eiso3c'] != '.']
            #-Import-#
            elif data_type == 'import' or data_type == 'imports':
                df['iiso3c'] = df['iiso3c'].apply(lambda x: concord_data(incomplete_iso3c_for_1962_2000, x, issue_error=False))     #issue_error = false returns x if no match
                df = df[df['iiso3c'] != '.']
            #-Trade-#
            else:
                df['iiso3c'] = df['iiso3c'].apply(lambda x: concord_data(incomplete_iso3c_for_1962_2000, x, issue_error=False))     #issue_error = false returns x if no match
                df['eiso3c'] = df['eiso3c'].apply(lambda x: concord_data(incomplete_iso3c_for_1962_2000, x, issue_error=False))     #issue_error = false returns x if no match
                df = df[df['iiso3c'] != '.']
                df = df[df['eiso3c'] != '.']
            df = df.reset_index()
            del df['index']
       
        #-Adjust Units from 1000's to $'s-#
        if adjust_units:
            if verbose: print "[INFO] Adjusting 'value' units to $'s"
            df['value'] = df['value']*1000         #Default: Keep in 1000's
        
        #-Return Dataset-#
        if verbose: print "[INFO] Finished Computing Dataset (%s) ..." % (data_type) 
        return df
