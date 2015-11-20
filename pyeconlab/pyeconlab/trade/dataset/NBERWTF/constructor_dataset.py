"""
NBER (Self Contained) Dataset Functions
=======================================

Type: {{ Configuration File }}

This family of files holds functions that Generate Specific Datasets that are Self Contained. 
The benefit of being self contained is that they are easier to debug then using the Constructor Class 
rather than test all permutations of the Constructor methods. 

Therefore, it is best to explore using the Constructor and then develop an SC dataset constructor.

Note: The below configurations are avaialable as Trade, Export or Import. 

Notes
-----
1. This reduces the size of the Constructor Class and allows for direct external access to methods that compile datasets if desired. 
2. Independantly verifiable functions for testing (without the complexity of the Constructor class)

Future Work
-----------
1. Should the Dataset Descriptions be moved to the relevant constructor file and remove this file?
2. Should I write hierarchical code (i.e. use SITC4 to conduct majority of work and then collapse?) 
This complicates some issues such as dropAX as this should be done AFTER the collapse to maximise Level 3 information
[For Now] Write Independant Functions (despite code duplication)
3. Update this for full set of Datasets

"""

#------------------------------#
#-Dataset Configuration Values-#
#------------------------------#

#-Level Independant SITC Options-#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

SITC_DATASET_DESCRIPTION = {
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

SITC_DATASET_OPTIONS = {
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
    #         },
}

