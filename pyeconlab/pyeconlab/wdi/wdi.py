"""
WDI
===

Library for working with the World Development Indicator (WDI) dataset
Handles the interaction with WDI Data Files

Future Work:
------------
#. Integrate ``MyDatasets`` Library for Dataset Management
#. Incorporate Pickle Functionality for Quicker Retrieval of Data, rather than fetching the object all the time from the file. 
#. Check all functionality has been migrated from previous standalone WDI library
#. Allow fetching using World Bank API? [Current focus is on working with the downloadable WDI file]

"""

### --- Standard Library Imports --- ###

import os
import sys
import re
import pandas as pd
import itertools as it
import pprint
import warnings

from .meta import WDISeriesCodes, CodeToName
codes = WDISeriesCodes()

### --- WDI Data Class --- ###

# source_dir="D:/work-data/datasets/70146f20cf40f818e6733d552c6cabb5/" (current local address)

class WDI(object):
    """
    World Development Indicators (Dataset) Object

    Parameters
    ----------
    source_dir  :   string
                    Specify the path to the dataset file. 

    Notes
    -----
    #. Data Structure: ::

        Core Data Structure is Wide pd.DataFrame as it is more memory efficient
            (iso3c, series_code)    |   < ..... years .... >

    #. How should I apply year filter? ::

        **> (a) As a Year Filter on Wide and Long Data: def year_filter(years=(syear, eyear), dta_struc='wide'/'long')
            (b) Build into each relevant function

    #. **Key Assumption**: ::
        
        WDI_data.csv is the file from the WDI and format hasn't changed

    ..  Future Work:
            [1] Make more robust to changes in the source dataset (Declare File Interface Settings)
            [2] Allow a source to be the WDI file rather than just source_ds
            [3] Integrate pprint objects for better output

    Examples
    --------

    >>> import pyeconlab
    >>> wdi = pyeconlab.wdi.WDI(source_dir="<path_to_file>")
    >>> wdi.lookup_series(regexp="GDP")
    [('NE.CON.GOVT.ZS',
      'General government final consumption expenditure (% of GDP)'),
     ('NY.GDP.PCAP.KD.ZG', 'GDP per capita growth (annual %)'),
    ...
    >>> wdi.series(series_code="NY.GDP.PCAP.KD.ZG", cntry="AUS")
    year
    1960         NaN
    1961    0.465882
    1962   -1.063828
    1963    4.257714
    1964    4.944559
    1965    3.928663
    1966    0.024405 
    ...
    """

    #-Source Directory-#
    source_dir = "" 
    ## -- WDI Data -- ##
    data = None                   #Data is by default Wide for Efficient Storage
    country_codes = None
    country_names = dict() 
    series_codes = None         
    series_descriptions = dict()
    ## -- Year Attributes -- ##
    start_year = None
    end_year = None
    #- Simple Codes -#
    codes = WDISeriesCodes()                            #-Populates the Object with Series Codes-#

    ## -- Setup and Initialise -- ##

    def __init__(self, source_dir, verbose=True):       #Default - Verbosely setup WDI Object
        self.source_dir = source_dir
        ## -- Load Data -- ##
        self.data = pd.read_csv(self.source_dir + 'WDI_Data.csv', dtype={'year' : int})                         #Assume Relative Reference to File given as FN
        self.from_df(self.data)
        self.start_year = self.data.columns[0]
        self.end_year = self.data.columns[-1]
        if verbose: print "\n[INFO] Setup of WDI() is complete!\n"
        
    ## -- Object Information -- ##

    def info(self):
        """ Provide Summary Information of the WDI Object """
        print "\nWDI Object Summary Information\n"
        print "\twdi.data (Rows: %s, Columns: %s)" % self.data.shape
        print "\twdi.data.index.names: %s" % self.data.index.names
        print "\twdi.data.columns.names: %s" % self.data.columns.names
        print "\twdi.start_year: %s" % self.start_year
        print "\twdi.end_year: %s" % self.end_year
        print

    ## -- IO -- ##

    def from_df(self, df, verbose=False):
        """ 
        Setup WDI Object from ``pd.DataFrame``
        
        Parameters
        ----------
        df      :       pd.DataFrame
                        Incoming DataFrame should be Wide Rows (see warning)

        Warning
        -------
        This assumes the incoming file has the following interface ::

            name, iso3c, series_description, series_code, years(t), ... ,years(T)

        """
        warnings.warn("This assumes the incoming file has the following structure ...\nname, iso3c, series_description, series_code, years(t), ... ,years(T)")
        # Rename Vars #
        cols = list(df.columns)
        cols[0] = 'name'                                                    #WARNING: Currently assume this order from the file for the first 4 columns
        cols[1] = 'iso3c'   
        cols[2] = 'series_description'
        cols[3] = 'series_code' 
        df.columns = pd.Index(cols)
        # Set Meta Data #
        tmp = df[['name', 'iso3c']].copy(deep=True).drop_duplicates()
        for idx, s in tmp.iterrows():
            self.country_names[s['iso3c']] = s['name']
        self.country_codes = sorted(self.country_names.keys())
        tmp = df[['series_code', 'series_description']].copy(deep=True).drop_duplicates()
        for idx, s in tmp.iterrows():
            self.series_descriptions[s['series_code']] = s['series_description']
        self.series_codes = sorted(self.series_descriptions.keys())
        del tmp
        # Remove Saved Meta Data from Data Table #
        del df['name']
        del df['series_description']
        # Set Index #
        df.set_index(keys=['iso3c', 'series_code'], inplace=True)
        df.columns.names = ['year']
        return df                                                       #Q: Should this set self.data?

    def to_stata(self, fl="", table_type="wide", target_dir="./"):
        """
        Make Stata DTA File of the Data
        """
        stata_data = self.data.copy(deep=True)
        if table_type == "long":   
            if fl=="":
                fl = "wdi_data_long.dta"            
            stata_data = stata_data.stack().reset_index()                   #Series
            stata_data.rename_axis({0:'value'}, inplace=True, axis=1)
            stata_data.to_stata(os.path.expanduser(target_dir) + fl, write_index=False)
        elif table_type == "wide":
            if fl=="":
                fl = "wdi_data_wide.dta"
            stata_data.columns = ['Y'+col for col in stata_data.columns]    #Stata Friendly Column Names
            stata_data = stata_data.reset_index()
            stata_data.to_stata(os.path.expanduser(target_dir) + fl, write_index=False)
        else:
            raise ValueError("table_type must be `long` or `wide`")
        del stata_data
        return fl

    def to_hdf(self, fl="", target_dir ="./"):
        """
        Make HDF File with Long and Wide WDI Data
        """
        if fl == "":
            fl = "wdi_data.h5"
        store = pd.HDFStore(os.path.expanduser(target_dir) + fl, complevel=9, complib='zlib')
        #-Long Data-#
        hdf_long = self.data.copy(deep=True).stack().reset_index()
        hdf_long.rename_axis({0:'value'}, inplace=True, axis=1)
        store.put('long', hdf_long, format='table')
        del hdf_long
        #Wide Data-#
        hdf_wide = self.data.copy(deep=True)
        hdf_wide.columns = ['Y'+col for col in hdf_wide.columns]    #Stata Friendly Column Names
        hdf_wide = hdf_wide.reset_index()
        store.put('wide', hdf_wide, format='table')
        del hdf_wide
        store.close()
        return fl

    ## -- Getter Methods -- ##

    def get(self, cntry, series_code, year, verbose=False):
        """
        Retrieve a data value for Country, Series_Code and Year

        Parameters
        ----------
        cntry           :   str
                            ISO3C Country Code

        series_code     :   str
                            WDI Series Code

        year            :   int 

        Returns
        -------
        value   :  int, str

        """
        idx = (cntry, series_code)
        return self.data.get_value(idx, year)

    ## -- Filters -- ##

    def year_filter(self, years, verbose=False):
        """ 
        Filter WDI Object for Years

        Parameters
        ----------
        years   :   tuple (start_year, end_year), slice (start_year, end_year, step)

        Returns 
        -------
        data    :   pd.dataframe
                    A copy of the adjusted data attribute

        Notes
        -------
        #. Core Data Structure (Wide) with Columns as Years
        
        Warning
        -------
        This method resets the state of the internal data attribute, in addition to start_year and end_year attributes.
        """
        if type(years) == tuple:
            start_year, end_year = years
            if verbose: print "[Year Filter] Start Year: %s and End Year: %s" % (start_year, end_year)
            yearlist = [str(x) for x in range(start_year, end_year+1, 1)]                                       #Note: +1 for Inclusive Years! Not in Python Convention
        elif type(years) == slice:
            start_year, end_year, step = (years.start, years.stop, years.step)
            if verbose: print "[Year Filter] Start Year: %s and End Year: %s and Step: %s" % (start_year, end_year, step)
            yearlist = [str(x) for x in range(start_year, end_year+1, step)]                                    #Note: +1 for Inclusive Years! Not in Python Convention
        else:
            raise ValueError("Years is not a tuple or slice")
        ## -- Reset Data -- ##
        self.data = self.data[yearlist]
        self.start_year = start_year
        self.end_year = end_year
        return self.data

    ## -- Data Retrieval -- ##

    def series(self, series_code, cntry=None, verbose=False):
        """
        Returns a pd.Series or pd.DataFrame of WDI Series that matches a series_code
        
        Parameters
        ----------
        series_code     :   string
                            A WDI Series Code
        cntry           :   string or list(string), optional(default=None)
                            specify a year filter, [default=return all countries]

        Returns
        -------
        data    :   pd.Series (single country) or pd.DataFrame (list of countries)

        """
        if cntry == None:
            if verbose: print "No country specified ... returning data for ALL countries"
            cntry = self.data.index.levels[0]
        elif type(cntry) == unicode:                                #Should I use unicode utf-8 OR Strings?
            if verbose: print "Converting Unicode Country (%s) to ASCII String" % cntry
            cntry.encode('ascii', 'ignore')
        elif type(cntry) != list:
            if verbose: print "Returning Series for Country: %s, and Series: %s" % (cntry, series_code)
            name = cntry + "-" + series_code
            s = self.data.xs(cntry).xs(series_code)
            s.name = name
            return s
        if verbose: print "Returning Series: %s for Countries: %s" % (series_code, cntry)
        idx = list(it.product(cntry, [series_code]))
        return self.data.ix[idx]
    
    def series_long(self, series_code, verbose=False):
        """
        Returns a DataFrame of WDI Series that matches series_codes
    
        Parameters
        ----------
        series_code     :   string or list(string)
                            WDI Series code      

        Returns
        -------
        data    :   pd.DataFrame

        ..  Future Work
            -----------
            [1] Write Tests

        """
        if type(series_code) == str:
            series_code = [series_code]
        for idx, code in enumerate(series_code):
            data = self.series(code, cntry=None, verbose=verbose).reset_index()
            del data['series_code']
            data = data.set_index(['iso3c']).stack()
            data = pd.DataFrame(data, columns=[CodeToName[code]])
            if idx == 0:
                df = data
            else:
                df = df.merge(data, left_index=True, right_index=True)
        #-Ensure Years are Integers-#
        df = df.reset_index()
        df['year'] = df['year'].apply(lambda x: int(x))
        df = df.set_index(['iso3c', 'year'])
        return df

    def year_data(self, year, verbose=False):
        """ 
        Return Year Specific WDI Data 
        
        Parameters
        ----------
        year    :   int, str, list(int), or list(str)


        Returns:
        --------
        data    :   pd.DataFrame

        """
        if type(year) == str or int:
            return wdi.data[str(year)]
        elif type(year) == list:
            return wdi.data[[str(x) for x in year]]
        else:
            raise ValueError("year must be str; int or a list of str; int")

    def cntry_series(self, series_code, cntry, verbose=False):
        """ A simple wrapper for single country series """
        return self.series(series_code, cntry=cntry, verbose=verbose)

    def cntry_data(self, cntry, series_codes=None, verbose=False):
        """
        Find Country WDI Data

        Parameters
        ----------
        cntry           :   str 
                            ISO3C Country Code
        series_codes    :   list(str), optional(default=None)
                            Specify a Specific WDI Series Code

        Returns
        -------
        data    :   pd.Series or pd.DataFrame 
        """
        if series_codes == None:
            return self.data.ix[cntry]                      #All Series Codes Available in Dataset
        else:
            return self.data.ix[cntry].ix[series_codes]     #Only Specified Series

    def lookup_series(self, regexp, verbose=False):
        """ 
        Lockup Possible Series Codes using a regular expression

        Parameters
        ----------
        regexp  :   str 
                    Specify python regular expression for search

        Returns
        -------
        results :   list(tuple(str, str))
                    List of possible matches (Code, Description)

        Raises
        ------
        ValueError
            If nothing matches the regular expression

        """
        exp = re.compile(regexp)
        results = []
        for sc in self.series_descriptions.keys():
            if re.search(exp, self.series_descriptions[sc]):
                results.append((sc, self.series_descriptions[sc]))
        ## -- Error Handling -- ##
        if len(results) == 0:
            raise ValueError("Nothing matched the regex: %s") % regexp
        return results

    ### --- Visualisation and Plotting --- ###

    def ts_plot(series_code, cntry, start_year=None, end_year=None, verbose=False):
        """
        [IN-WORK] Plottng Time Series Data.

        Warning
        -------
        This is currently on hold as Pandas DataFrames supports pretty easy plotting
        """
        ## -- Option Parsing -- ##
        if start_year == None:
            pass
        if end_year == None:
            pass
        ## -- Plotting -- ##
        raise NotImplementedError


### --- Test Scripts --- ###

#-This should be moved to Tests SubPackage-#

if __name__ == '__main__':
    print "WDI Library Test Script"
    print
    W = WDI('WDI_Data.csv',source_ds='70146f20cf40f818e6733d552c6cabb5', hash_file_sep=r' ', verbose=True)
    # - Test Info Method - #
    W.info()
    
    # - Test Series Function - #
    print
    print "Testing series() method"
    s = W.series(r'NY.GDP.MKTP.CD', verbose=True)
    print s[0:10]
    s = W.series(r'NY.GDP.MKTP.CD', cntry=['AFG', 'ZWE'], verbose=True)
    print s
    s = W.series(r'NY.GDP.MKTP.CD', cntry='AUS', verbose=True)
    print s

    # - Test get Function - #
    print
    print "Testing get() method"
    v = W.get('AUS', r'NY.GDP.MKTP.CD', '2000')
    print "Value: %s" % v

    # - Test lookup_series function - #
    print
    print "Testing lookup_series() method"
    r = W.lookup_series(r'GDP per capita growth')
    pp = pprint.PrettyPrinter(indent=2)
    pp.pprint(r)

    # - Year Filter - #
    print
    print "Testing year_filter() method"
    print "Current WDI Object Info:"
    W.info()
    W.year_filter(years=(2001,2002), verbose=True)
    print "Year Filter Info"
    W.info()
