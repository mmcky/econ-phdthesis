"""
BACI Recodes Meta Data
======================

This Module contains custom recodes for BACI Dataset

Notes
-----
[1] Many of these concordances are HAND constructed by looking at supporting meta data contained in ``xlsx`` etc.

Future Work 
----------- 
[1] Countries with '.' should they remain in the dataset under the assumption that years with no reports indicate 0 trade?
[2] Document '.' decisions clearly in a within repo file

References
----------
http://en.wikipedia.org/wiki/List_of_world_map_changes#1990s
1. Why is East Timor Data available before 2002?
2. 

"""

from pyeconlab.util import from_dict_to_csv

#-----------------------#
#-Intertemporal Recodes-#
#-----------------------#

class intertemporal(object):
    """ 
    Class Containing Intertemporal / Dynamic Consistent Recodes
    """

    #-Joint Importer / Exporter Recodes-#
    #-----------------------------------#

    #-Contains ISO3C to SPECIAL CODES Required to Make 1998 to 2012 Dynamically Consistent (Strict)-#
    #-Moved '.' recodes to incomplete_iso3c_for_2000_2012-#
    iso3c_for_1998_2012 = {
        #'YUG' -> There is No Serbia in this dataset so cannot join intertemporally
    }


    iso3c_for_1998_2012_definitions = {
       #NA
    }

    incomplete_iso3c_for_1998_2012 = {
            'ASM'   : '.',                  #1998, 1999
            'GUM'   : '.',                  #1998, 1999
            'ANT'   : '.',                  #2011, 2012 [#2005, 2008, 2010, 2011]
            'NTZ'   : '.',                  #2011, 2012
            'SMR'   : '.',                  #1998, 1999
            'SDN'   : '.',                  #2012
            'UMI'   : '.',                  #2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012
            'YUG'   : '.',                  #2006, 2007, 2008, 2009, 2010, 2011, 2012

    }

    def incomplete_iso3c_for_1962_2000_csv(self, fl='incomplete_iso3c_for_1998_2012.csv', target_dir='csv/'):
        """
        Simple Utility for writing incomplete_iso3c_for_1998_2012 to csv file
        """
        from_dict_to_csv(self.incomplete_iso3c_for_1998_2012, header=['iso3c', '_recode_'], fl=fl, target_dir=target_dir)

