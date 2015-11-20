"""
Trade Concordances
==================

This package includes trade concordances

Available Concordances
----------------------
1. HS02 to SITCR2

Notes
-----
1. Any Special Concordances or Alterations are located in dataset/<dataset_name>/meta
"""

import os
import copy
import pandas as pd

from pyeconlab.util import check_directory

# - Data in data/ - #
this_dir, this_filename = os.path.split(__file__)
DATA_PATH = check_directory(os.path.join(this_dir, "data"))

#---------------#
#-Product Codes-#
#---------------#

class HS_To_SITC(object):
    """ 
    Concordance from HS to SITC

    Parameters
    ----------
    hs          :   str
                    Specify HS Code ('HS92', 'HS96', 'HS02')
    sitc        :   str 
                    Specify SITC Code ('SITCR1', 'SITCR2', 'SITCR2', 'SITCR4')
    source_institution  :   str, optional(default='un')
                            Specify Source Institution for Concordance Information

    Future Work
    -----------
    Add Optional Level Arguments for Automatic Conversion
    hs_level    :   int
                    Specify Chapter Level for HS (1,2,3,4,5, or 6)
    sitc_level  :   int 
                    Specify Chapter Level for SITC (1,2,3,4, or 5)
    """

    def __init__(self, hs, sitc, hs_level=6, sitc_level=5, source_institution="un", verbose=True):
        self.hs_revision = hs
        self.hs_level = 6
        self.sitc_revision = sitc 
        self.sitc_level = 5
        #-Fetch Data-#
        if source_institution == "un":
            self.source_web = u"http://unstats.un.org/unsd/trade/conversions/HS%20Correlation%20and%20Conversion%20tables.htm"
            self.sitc_header = self.sitc_revision
            if self.hs_revision in ["HS92", "HS96"]:
                self.hs_header = "HS19%s"%self.hs_revision[2:] 
            else:
                self.hs_header = "HS20%s"%self.hs_revision[2:] 
            fl = self.hs_header + "_to_" + self.sitc_header + ".csv"
            if verbose: print "[INFO] Retrieving information from file: %s" % fl
            self.__data = pd.read_csv(DATA_PATH + "un/" + fl, dtype={self.hs_header : str, self.sitc_header : str}).set_index(self.hs_header)
        else:
            raise NotImplementedError("'un' is the only source institution that has been Implimented")
        #-Adjust Levels-#
        if hs_level != 6:
            self.to_hs_level(level=hs_level, verbose=verbose)
        if sitc_level != 5:
            self.to_sitc_level(level=sitc_level, verbose=verbose)

    def __str__(self):
        return "Concordance between %s (L:%s) and %s (L:%s)" % (self.hs_revision, self.hs_level, self.sitc_revision, self.sitc_level)

    @property 
    def data(self):
        return self.__data.copy(deep=True)      #-Should this Return COPY or View?-#

    @property 
    def concordance(self):
        """ 
        Return the Concordance Dictionary
        """
        return self.__data['SITCR2'].to_dict()

    def to_hs_level(self, level, verbose=True):
        """ 
        Convert HS to Another Level
        
        Parameters
        ----------
        level   :   int (1,2,3,4,5)
                    Select HS Chapter Level
                    
        """
        if level >= 6:
            raise ValueError("HS Level must be between 1 and 5")
        #-Core-#
        data = self.__data.reset_index()
        init_numobs = data.shape[0]
        data[self.hs_header] = data[self.hs_header].apply(lambda x: x[0:level])
        data.drop_duplicates(inplace=True)
        data.set_index(self.hs_header, inplace=True)
        if verbose: print "[DROPPING] %s duplicate pairs" % (init_numobs - data.shape[0])
        #-Save Results To Object-#
        self.hs_level = level
        self.__data = data                                                  

    def to_sitc_level(self, level, verbose=True):
        """ 
        Convert SITC to Another Level         
        
        Parameters
        ----------
        level   :   int (1,2,3,4)
                    Select SITC Chapter Level
        """
        if level >= 5:
            raise ValueError("SITC Level must be between 1 and 4")
        #-Core-#
        data = self.__data.reset_index()
        init_numobs = data.shape[0]
        data[self.sitc_header] = data[self.sitc_header].apply(lambda x: x[0:level])
        data.drop_duplicates(inplace=True)
        data.set_index(self.hs_header, inplace=True)
        if verbose: print "[DROPPING] %s duplicate pairs" % (init_numobs - data.shape[0])
        #-Save Results To Object-#
        self.sitc_level = level
        self.__data = data


## What is the best way to construct HS2002_To_SITCR2 using General Class Above ##

# class HS1996_To_SITCR2(HS_To_SITC):
#     def __init__(self, hs_level=6, sitc_level=5, source_institution='un', verbose=True):
#         super(HS_To_SITC, self).__init__()

#     def __init__(self, hs_level=6, sitc_level=5, source_institution='un', verbose=True):
#         self = HS_To_SITC("HS96", "SITCR2", hs_level, sitc_level, source_institution=source_institution, verbose=verbose)


### ----> DEPRECATED <---- ###


class HS1996_To_SITCR2(object):
    """
    Concordance for HS 1996 to SITC Revision 2

    Parameters
    ----------
    hs_level    :   int, optional(default=6)
                    Specify HS Level (1 to 6)
    sitc_level  :   int, optional(default=5)
                    Specify SITC Level (1 to 5)
    source_institution  :   str, optional(default='un')
                            Specify Source Institution for Concordance Information

    Notes
    -----
    1. Come up with a package wide standard for sitc_revision?

    ..  Future Work
        -----------
        1. Construct a base class for generalised inheritance
    """
    def __init__(self, hs_level=6, sitc_level=5, source_institution='un', verbose=True):
        #-Attributes-#
        self.hs_revision = "HS96"
        self.hs_level = 6
        self.sitc_revision = "SITCR2"
        self.sitc_level = 5
        self.source_institution = "un"
        #-Fetch Data-#
        if source_institution == "un":
            self.source_web = u"http://unstats.un.org/unsd/trade/conversions/HS%20Correlation%20and%20Conversion%20tables.htm"
            self.__data = pd.read_csv(DATA_PATH + "un/" + "HS1996_to_SITCR2.csv", dtype={'HS1996' : str, 'SITCR2' : str}).set_index('HS1996')
        else:
            raise NotImplementedError("'un' is the only source institution that has been Implimented")
        #-Parse Options-#
        if hs_level != 6:
            self.to_hs_level(hs_level, verbose=verbose)
        if sitc_level != 5:
            self.to_sitc_level(sitc_level, verbose=verbose)

    @property 
    def data(self):
        return self.__data.copy(deep=True)      #-Should this Return COPY or View?-#

    @property 
    def concordance(self):
        """ 
        Return the Concordance Dictionary HS2002 => SITCR2
        """
        return self.__data['SITCR2'].to_dict()

    def to_hs_level(self, level, verbose=True):
        """ 
        Convert HS2002 to Another Level
        
        Parameters
        ----------
        level   :   int (1,2,3,4,5)
                    Select HS Chapter Level
                    
        """
        if level >= 6:
            raise ValueError("HS Level must be between 1 and 5")
        #-Core-#
        data = self.__data.reset_index()
        init_numobs = data.shape[0]
        data['HS1996'] = data['HS1996'].apply(lambda x: x[0:level])
        data.drop_duplicates(inplace=True)
        data.set_index('HS1996', inplace=True)
        if verbose: print "[DROPPING] %s observations" % (init_numobs - data.shape[0])
        #-Save Results To Object-#
        self.hs_level = level
        self.__data = data                                                  

    def to_sitc_level(self, level, verbose=True):
        """ 
        Convert SITCR2 to Another Level         
        
        Parameters
        ----------
        level   :   int (1,2,3,4)
                    Select SITC Chapter Level
        """
        if level >= 5:
            raise ValueError("SITC Level must be between 1 and 4")
        #-Core-#
        data = self.__data.reset_index()
        init_numobs = data.shape[0]
        data['SITCR2'] = data['SITCR2'].apply(lambda x: x[0:level])
        data.drop_duplicates(inplace=True)
        data.set_index('HS1996', inplace=True)
        if verbose: print "[DROPPING] %s observations" % (init_numobs - data.shape[0])
        #-Save Results To Object-#
        self.sitc_level = level
        self.__data = data


class HS2002_To_SITCR2(object):
    """
    Concordance for HS 2002 to SITC Revision 2

    Parameters
    ----------
    hs_level    :   int, optional(default=6)
                    Specify HS Level (1 to 6)
    sitc_level  :   int, optional(default=5)
                    Specify SITC Level (1 to 5)
    source_institution  :   str, optional(default='un')
                            Specify Source Institution for Concordance Information

    Notes
    -----
    1. Should this be Level Specific?
    2. Come up with a package wide standard for sitc_revision?

    ..  Future Work
        -----------
        1. Construct a base class for generalised inheritance
    """
    def __init__(self, hs_level=6, sitc_level=5, source_institution='un', verbose=True):
        #-Attributes-#
        self.hs_revision = "HS02"
        self.hs_level = 6
        self.sitc_revision = "SITCR2"
        self.sitc_level = 5
        self.source_institution = "un"
        #-Fetch Data-#
        if source_institution == "un":
            self.source_web = u"http://unstats.un.org/unsd/trade/conversions/HS%20Correlation%20and%20Conversion%20tables.htm"
            self.__data = pd.read_csv(DATA_PATH + "un/" + "HS2002_to_SITCR2.csv", dtype={'HS2002' : str, 'SITCR2' : str}).set_index('HS2002')
        else:
            raise NotImplementedError("'un' is the only source institution that has been Implimented")
        #-Parse Options-#
        if hs_level != 6:
            self.to_hs_level(hs_level, verbose=verbose)
        if sitc_level != 5:
            self.to_sitc_level(sitc_level, verbose=verbose)

    @property 
    def data(self):
        return self.__data.copy(deep=True)      #-Should this Return COPY or View?-#

    @property 
    def concordance(self):
        """ 
        Return the Concordance Dictionary HS2002 => SITCR2
        """
        return self.__data['SITCR2'].to_dict()

    def to_hs_level(self, level, verbose=True):
        """ 
        Convert HS2002 to Another Level
        
        Parameters
        ----------
        level   :   int (1,2,3,4,5)
                    Select HS Chapter Level
                    
        """
        if level >= 6:
            raise ValueError("HS Level must be between 1 and 5")
        #-Core-#
        data = self.__data.reset_index()
        init_numobs = data.shape[0]
        data['HS2002'] = data['HS2002'].apply(lambda x: x[0:level])
        data.drop_duplicates(inplace=True)
        data.set_index('HS2002', inplace=True)
        if verbose: print "[DROPPING] %s observations" % (init_numobs - data.shape[0])
        #-Save Results To Object-#
        self.hs_level = level
        self.__data = data                                                  

    def to_sitc_level(self, level, verbose=True):
        """ 
        Convert SITCR2 to Another Level         
        
        Parameters
        ----------
        level   :   int (1,2,3,4)
                    Select SITC Chapter Level
        """
        if level >= 5:
            raise ValueError("SITC Level must be between 1 and 4")
        #-Core-#
        data = self.__data.reset_index()
        init_numobs = data.shape[0]
        data['SITCR2'] = data['SITCR2'].apply(lambda x: x[0:level])
        data.drop_duplicates(inplace=True)
        data.set_index('HS2002', inplace=True)
        if verbose: print "[DROPPING] %s observations" % (init_numobs - data.shape[0])
        #-Save Results To Object-#
        self.sitc_level = level
        self.__data = data


### ---> END DEPRECATION <--- ###

