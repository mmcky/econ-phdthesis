"""
NBER DATASET CONSTRUCT OPTIONS
"""

#-Dataset Configuration-#
#~~~~~~~~~~~~~~~~~~~~~~~#

#-Future Work: Check this is Consistent with pyeconlab definitions-#

DATA_DESCRIPTION = {
    #-Country Datasets-#
    'A' :   u"A basic dataset that incudes AX and SITCR2 indicators and collapses data to a specified level maintaining initial countrycodes and productcodes as in the raw dataset, removes NES",
    'B' :   u"[A] except corrects HK-CHINA data from nber correction files",
    'C' :   u"A dataset that does not contain AX, adjusts HK-CHINA data, but does not adjust products or countries for intertemporal consistency",
    'D' :   u"A Dataset that does not contain AX or any non standard SITCR2 codes, adjusts HK-CHINA data, but does not adjust products or countries for intertemporal consistency",
    'E' :   u"A Dataset that does not contain AX and updates productcodes to be more intertemporally consisted, adjusts HK-CHINA data, but does not adjust countries for intertemporal consistency",
    'F' :   u"A dataset that does not contain AX and updates productcodes to be more intertemporally consisted, adjusts HK-CHINA data, and adjusts countries for intertemporal consistency",
    'G' :   u"A dataset that does not contain AX or any non standard SITCR2 codes, adjusts HK-CHINA data, and adjusts country codes for intertemporal consistency",
    # 'H' :   u"A dataset that does not contain AX and udpates productcodes to be more intertemporally consistent, adjusts HK-CHINA data, and adjusts country codes for intertemporaly consistency and drops non-complete countries (EXPERIMENTAL)",
    # 'I' :   u"A dataset that does not contain AX or any non standard SITCR2 codes, adjusts HK-CHINA data, and drops countries that are not intertemporally complete (EXPERIMENTAL)",
} 

RAW_DATA_DESCRIPTION = {
    #-Raw Dataset Descriptions-#
    'RAW1' : u"Basic RAW dataset with iso3c countrycodes included and collapsed quantity disaggregation",
    'RAW2' : u"Basic RAW dataset with iso3c countrycodes included, collapsed quantity disaggregation, and adjusts HK-CHINA data",
}

#-Data Option Definitions-#

DATA_OPTIONS = {
    'A' :   {   
                #-ProductCode Adjustments-#
                'AX'     : True,                      #Add a Marker for 'A' and 'X' Codes
                'dropAX' : False,                     #Drops Products where Codes have 'A' or 'X'
                'sitcr2' : True,                      #Adds an Official SITC Revision 2 Indicator
                'drop_nonsitcr2' : False,             #Removes Non-Official SITC Revision 2 Codes From the Dataset
                'adjust_hk' : False,                  #Adjust Data to incorporate Honk Kong Adjusments provided by NBER
                'intertemp_productcode' : False,      #Compute an Intertemporal ProductCode
                #-CountryCode Adjustments-#
                'intertemp_cntrycode' : False,        #Recode Country Codes to be Intertemporally Consistent
                'drop_incp_cntrycode' : False,        #Drop Incomplete Intertemporal Countries
                #-Other Adjustments-#
                'adjust_units' : False,
                'source_institution' : 'un',
                'verbose' : True,
            },
    'B' :   {   
                'AX'     : True,                      #Add a Marker for 'A' and 'X' Codes
                'dropAX' : False,                     #Drops Products where Codes have 'A' or 'X'
                'sitcr2' : True,                      #Adds an Official SITC Revision 2 Indicator
                'drop_nonsitcr2' : False,             #Removes Non-Official SITC Revision 2 Codes From the Dataset
                'adjust_hk' : True,                   #Adjust Data to incorporate Honk Kong Adjusments provided by NBER
                'intertemp_productcode' : False,      #Compute an Intertemporal ProductCode
                'intertemp_cntrycode' : False,        #Recode Country Codes to be Intertemporally Consistent
                'drop_incp_cntrycode' : False,        #Drop Incomplete Intertemporal Countries
                'adjust_units' : False,
                'source_institution' : 'un',
                'verbose' : True,
            },
    'C' :   {   
                'AX'     : True,                     #Add a Marker for 'A' and 'X' Codes
                'dropAX' : True,                     #Drops Products where Codes have 'A' or 'X'
                'sitcr2' : True,                     #Adds an Official SITC Revision 2 Indicator
                'drop_nonsitcr2' : False,            #Removes Non-Official SITC Revision 2 Codes From the Dataset
                'adjust_hk' : True,                  #Adjust Data to incorporate Honk Kong Adjusments provided by NBER
                'intertemp_productcode' : False,     #Compute an Intertemporal ProductCode
                'intertemp_cntrycode' : False,       #Recode Country Codes to be Intertemporally Consistent
                'drop_incp_cntrycode' : False,       #Drop Incomplete Intertemporal Countries
                'adjust_units' : False,
                'source_institution' : 'un',
                'verbose' : True,
            }, 
    'D' :   {                                        #-!!-MAJOR-!!-# 
                'AX'     : True,                     #Add a Marker for 'A' and 'X' Codes
                'dropAX' : True,                     #Drops Products where Codes have 'A' or 'X'
                'sitcr2' : True,                     #Adds an Official SITC Revision 2 Indicator
                'drop_nonsitcr2' : True,             #Removes Non-Official SITC Revision 2 Codes From the Dataset
                'adjust_hk' : True,                  #Adjust Data to incorporate Honk Kong Adjusments provided by NBER
                'intertemp_productcode' : False,     #Compute an Intertemporal ProductCode
                'intertemp_cntrycode' : False,       #Recode Country Codes to be Intertemporally Consistent
                'drop_incp_cntrycode' : False,       #Drop Incomplete Intertemporal Countries
                'adjust_units' : False,
                'source_institution' : 'un',
                'verbose' : True,
            },       
    'E' :   {                                        #-!!-MAJOR-!!-# 
                'AX'     : True,                     #Add a Marker for 'A' and 'X' Codes
                'dropAX' : True,                     #Drops Products where Codes have 'A' or 'X'
                'sitcr2' : True,                     #Adds an Official SITC Revision 2 Indicator
                'drop_nonsitcr2' : False,            #Removes Non-Official SITC Revision 2 Codes From the Dataset
                'adjust_hk' : True,                  #Adjust Data to incorporate Honk Kong Adjusments provided by NBER
                'intertemp_productcode' : True,      #Compute an Intertemporal ProductCode
                'intertemp_cntrycode' : False,       #Recode Country Codes to be Intertemporally Consistent
                'drop_incp_cntrycode' : False,       #Drop Incomplete Intertemporal Countries
                'adjust_units' : False,
                'source_institution' : 'un',
                'verbose' : True,
            },           
   'F' :   {                                         #-!!-MAJOR-!!-# 
                'AX'     : True,                     #Add a Marker for 'A' and 'X' Codes
                'dropAX' : True,                     #Drops Products where Codes have 'A' or 'X'
                'sitcr2' : True,                     #Adds an Official SITC Revision 2 Indicator
                'drop_nonsitcr2' : False,            #Removes Non-Official SITC Revision 2 Codes From the Dataset
                'adjust_hk' : True,                  #Adjust Data to incorporate Honk Kong Adjusments provided by NBER
                'intertemp_productcode' : True,      #Compute an Intertemporal ProductCode
                'intertemp_cntrycode' : True,        #Recode Country Codes to be Intertemporally Consistent
                'drop_incp_cntrycode' : False,       #Drop Incomplete Intertemporal Countries
                'adjust_units' : False,
                'source_institution' : 'un',
                'verbose' : True,
            },     
    'G' :   {   
                'AX'     : True,                     #Add a Marker for 'A' and 'X' Codes
                'dropAX' : True,                     #Drops Products where Codes have 'A' or 'X'
                'sitcr2' : True,                     #Adds an Official SITC Revision 2 Indicator
                'drop_nonsitcr2' : True,             #Removes Non-Official SITC Revision 2 Codes From the Dataset
                'adjust_hk' : True,                  #Adjust Data to incorporate Honk Kong Adjusments provided by NBER
                'intertemp_productcode' : False,     #Compute an Intertemporal ProductCode
                'intertemp_cntrycode' : True,        #Recode Country Codes to be Intertemporally Consistent
                'drop_incp_cntrycode' : False,       #Drop Incomplete Intertemporal Countries
                'adjust_units' : False,
                'source_institution' : 'un',
                'verbose' : True,
            },
    # 'H' :   {                                        #-!!-EXPERIMENTAL-!!-#
    #             'AX'     : True,                     #Add a Marker for 'A' and 'X' Codes
    #             'dropAX' : True,                     #Drops Products where Codes have 'A' or 'X'
    #             'sitcr2' : True,                     #Adds an Official SITC Revision 2 Indicator
    #             'drop_nonsitcr2' : False,            #Removes Non-Official SITC Revision 2 Codes From the Dataset
    #             'adjust_hk' : True,                  #Adjust Data to incorporate Honk Kong Adjusments provided by NBER
    #             'intertemp_productcode' : True,      #Compute an Intertemporal ProductCode
    #             'intertemp_cntrycode' : True,        #Recode Country Codes to be Intertemporally Consistent
    #             'drop_incp_cntrycode' : True,        #Drop Incomplete Intertemporal Countries
    #             'adjust_units' : False,
    #             'source_institution' : 'un',
    #             'verbose' : True,
    #         },
    # 'I' :   {                                        #-!!-EXPERIMENTAL-!!-#
    #             'AX'     : True,                     #Add a Marker for 'A' and 'X' Codes
    #             'dropAX' : True,                     #Drops Products where Codes have 'A' or 'X'
    #             'sitcr2' : True,                     #Adds an Official SITC Revision 2 Indicator
    #             'drop_nonsitcr2' : True,             #Removes Non-Official SITC Revision 2 Codes From the Dataset
    #             'adjust_hk' : True,                  #Adjust Data to incorporate Honk Kong Adjusments provided by NBER
    #             'intertemp_productcode' : False,     #Compute an Intertemporal ProductCode
    #             'intertemp_cntrycode' : False,       #Recode Country Codes to be Intertemporally Consistent
    #             'drop_incp_cntrycode' : True,        #Drop Incomplete Intertemporal Countries
    #             'adjust_units' : False,
    #             'source_institution' : 'un',
    #             'verbose' : True,
    #         },
}


RAW_DATA_OPTIONS = {
#-RAW includes NES, World etc. and Undertakes a Minimum of Changes to the Data to make it Comparable-#
    'RAW1' : { 
                'adjust_hk'      : False,           #Adjust Hong Kong Data
                'harmonised_raw' : True,            #Construct Harmonised RAW Data File (No Quantity Disaggregation, Common Names)
                #-Required Due to Script Logic Below-#
                'intertemp_productcode' : False,
            },
    'RAW2' : { 
                'adjust_hk'      : True,            #Adjust Hong Kong Data
                'harmonised_raw' : True,            #Construct Harmonised RAW Data File (No Quantity Disaggregation, Common Names)
                #-Required Due to Script Logic Below-#
                'intertemp_productcode' : False,
            },
}