"""
PENN World Tables Dataset

Dataset
-------
2c2e8d593f39ee74aeb2c7c17047ea3f

"""

import os
import pandas as pd
from .meta import series_description

class PENN(object):
    """
    PENN World Tables (Dataset) Object

    Parameters
    ----------
    source_dir  :   string
                    Specify the path to the dataset file. 

    """

    #-Source Directory-#
    source_dir = "" 

    data = None                   
    country_codes = None
    country_names = dict() 
    series_codes = None         
    series_descriptions = series_description
    ## -- Year Attributes -- ##
    start_year = None
    end_year = None

    versions = {
    	8.1 	: 	'pwt81.dta' 			#pwt81.xlsx is also available
    }

    ## -- Setup and Initialise -- ##

    def __init__(self, source_dir, version=8.1, verbose=True):
        self.source_dir = source_dir
        self.version = version
        fl = self.source_dir + self.versions[self.version]
        if verbose: print "Loading data for PENN world tables from: %s" % (fl)
        self.from_df(pd.read_stata(fl))
        
    ## -- Object Information -- ##

    def info(self):
        """ Provide Summary Information of the WDI Object """
        print "\nPENN Object Summary Information\n"
        print "\tpenn.data (Rows: %s, Columns: %s)" % self.data.shape
        print "\tpenn.data.index.names: %s" % self.data.index.names
        print "\tpenn.data.columns: %s" % self.data.columns
        print "\tpenn.start_year: %s" % self.start_year
        print "\tpenn.end_year: %s" % self.end_year
        print

    ## -- IO -- ##

    def from_df(self, df, verbose=False):
        """ 
        Setup PENN Object from ``pd.DataFrame``
        
        Parameters
        ----------
        df      :       pd.DataFrame

        Notes
        -----
          1. This assumes the incoming dataframe is organised the same way as the stata file

        """
        #Rename variables for internal consistency
        self.data = df.rename_axis({'countrycode' : 'iso3c', 'country' : 'countryname'}, axis=1) 	
        self.start_year = self.data.year.min()
        self.end_year = self.data.year.max()
        # Country Meta Data #
        tmp = self.data[['countryname', 'iso3c']].copy(deep=True).drop_duplicates()
        for idx, s in tmp.iterrows():
            self.country_names[s['iso3c']] = s['countryname']
        self.country_codes = sorted(self.country_names.keys())
        del tmp
        # Data Meta Data #
        self.series_codes = self.data.columns.drop(["iso3c", "countryname", "currency_unit", "year"])
        # Drop Redundant Information #
        del self.data["countryname"]
        # Set Index #
        self.data.set_index(keys=['iso3c', 'year'], inplace=True)

    def to_hdf(self, fl="", target_dir ="./"):
        """
        Make HDF File
        """
        if fl == "":
            fl = "penn_data.h5"
        fl = os.path.expanduser(target_dir) + fl
        store = pd.HDFStore(fl, complevel=9, complib='zlib')
        store.put("penn", self.data, format="table") 			#Could Save in Various Formats#
        store.close()
        return fl

    def to_stata(self, fl="", target_dir="./"):
        """
        Make STATA File
        """
        if fl == "":
            fl = "penn_data.dta"
        fl = os.path.expanduser(target_dir)+fl
        self.data.to_stata(fl)
        return fl

    ## -- Getter Methods -- ##

    def get(self, cntry, series_code, year, verbose=False):
        """
        Retrieve a data value for Country, Series_Code and Year
        """
        idx = (cntry, int(year))
        return self.data.get_value(idx, series_code)

    ## -- Filters -- ##

    def year_filter(self, years, verbose=False):
        """ 
        Filter PENN Object for Years
        """
        if type(years) == tuple:
            start_year, end_year = years
            if verbose: print "[Year Filter] Start Year: %s and End Year: %s" % (start_year, end_year)
            yearlist = [str(x) for x in range(start_year, end_year+1, 1)]                                       #Note: +1 for Inclusive Years! Not in Python Convention
        elif type(years) == slice:
            start_year, end_year, step = (years.start, years.stop, years.step)
            if verbose: print "[Year Filter] Start Year: %s and End Year: %s and Step: %s" % (start_year, end_year, step)
            yearlist = [str(x) for x in range(start_year, end_year+1, step)]                                    #Note: +1 for Inclusive Years! Not in Python Convention
        elif type(years) == list:
       		start_year = years[0]
       		end_year = years[-1]
        	yearlist = years
        else:
            raise ValueError("Years is not a tuple, slice or list")
        ## -- Reset Data -- ##
        self.data = self.data.reset_index()
        self.data = self.data.loc[self.data["year"].isin(set(yearlist))]
        self.data = self.data.set_index(["iso3c", "year"])
        self.start_year = start_year
        self.end_year = end_year
        return self.data

    # ## -- Data Retrieval -- ##

    # def series(self, series_code, cntry=None, verbose=False):
    #     """
    #     Returns a pd.Series or pd.DataFrame of WDI Series that matches a series_code
        
    #     Parameters
    #     ----------
    #     series_code     :   string
    #                         A WDI Series Code
    #     cntry           :   string or list(string), optional(default=None)
    #                         specify a year filter, [default=return all countries]

    #     Returns
    #     -------
    #     data    :   pd.Series (single country) or pd.DataFrame (list of countries)

    #     """
    #     if cntry == None:
    #         if verbose: print "No country specified ... returning data for ALL countries"
    #         cntry = self.data.index.levels[0]
    #     elif type(cntry) == unicode:                                #Should I use unicode utf-8 OR Strings?
    #         if verbose: print "Converting Unicode Country (%s) to ASCII String" % cntry
    #         cntry.encode('ascii', 'ignore')
    #     elif type(cntry) != list:
    #         if verbose: print "Returning Series for Country: %s, and Series: %s" % (cntry, series_code)
    #         name = cntry + "-" + series_code
    #         s = self.data.xs(cntry).xs(series_code)
    #         s.name = name
    #         return s
    #     if verbose: print "Returning Series: %s for Countries: %s" % (series_code, cntry)
    #     idx = list(it.product(cntry, [series_code]))
    #     return self.data.ix[idx]
    
    # def series_long(self, series_code, verbose=False):
    #     """
    #     Returns a DataFrame of WDI Series that matches series_codes
    
    #     Parameters
    #     ----------
    #     series_code     :   string or list(string)
    #                         WDI Series code      

    #     Returns
    #     -------
    #     data    :   pd.DataFrame

    #     ..  Future Work
    #         -----------
    #         [1] Write Tests

    #     """
    #     if type(series_code) == str:
    #         series_code = [series_code]
    #     for idx, code in enumerate(series_code):
    #         data = self.series(code, cntry=None, verbose=verbose).reset_index()
    #         del data['series_code']
    #         data = data.set_index(['iso3c']).stack()
    #         data = pd.DataFrame(data, columns=[CodeToName[code]])
    #         if idx == 0:
    #             df = data
    #         else:
    #             df = df.merge(data, left_index=True, right_index=True)
    #     #-Ensure Years are Integers-#
    #     df = df.reset_index()
    #     df['year'] = df['year'].apply(lambda x: int(x))
    #     df = df.set_index(['iso3c', 'year'])
    #     return df

    # def year_data(self, year, verbose=False):
    #     """ 
    #     Return Year Specific WDI Data 
        
    #     Parameters
    #     ----------
    #     year    :   int, str, list(int), or list(str)


    #     Returns:
    #     --------
    #     data    :   pd.DataFrame

    #     """
    #     if type(year) == str or int:
    #         return wdi.data[str(year)]
    #     elif type(year) == list:
    #         return wdi.data[[str(x) for x in year]]
    #     else:
    #         raise ValueError("year must be str; int or a list of str; int")

    # def cntry_series(self, series_code, cntry, verbose=False):
    #     """ A simple wrapper for single country series """
    #     return self.series(series_code, cntry=cntry, verbose=verbose)

    # def cntry_data(self, cntry, series_codes=None, verbose=False):
    #     """
    #     Find Country WDI Data

    #     Parameters
    #     ----------
    #     cntry           :   str 
    #                         ISO3C Country Code
    #     series_codes    :   list(str), optional(default=None)
    #                         Specify a Specific WDI Series Code

    #     Returns
    #     -------
    #     data    :   pd.Series or pd.DataFrame 
    #     """
    #     if series_codes == None:
    #         return self.data.ix[cntry]                      #All Series Codes Available in Dataset
    #     else:
    #         return self.data.ix[cntry].ix[series_codes]     #Only Specified Series

    # def lookup_series(self, regexp, verbose=False):
    #     """ 
    #     Lockup Possible Series Codes using a regular expression

    #     Parameters
    #     ----------
    #     regexp  :   str 
    #                 Specify python regular expression for search

    #     Returns
    #     -------
    #     results :   list(tuple(str, str))
    #                 List of possible matches (Code, Description)

    #     Raises
    #     ------
    #     ValueError
    #         If nothing matches the regular expression

    #     """
    #     exp = re.compile(regexp)
    #     results = []
    #     for sc in self.series_descriptions.keys():
    #         if re.search(exp, self.series_descriptions[sc]):
    #             results.append((sc, self.series_descriptions[sc]))
    #     ## -- Error Handling -- ##
    #     if len(results) == 0:
    #         raise ValueError("Nothing matched the regex: %s") % regexp
    #     return results

    # ### --- Visualisation and Plotting --- ###

    # def ts_plot(series_code, cntry, start_year=None, end_year=None, verbose=False):
    #     """
    #     [IN-WORK] Plottng Time Series Data.

    #     Warning
    #     -------
    #     This is currently on hold as Pandas DataFrames supports pretty easy plotting
    #     """
    #     ## -- Option Parsing -- ##
    #     if start_year == None:
    #         pass
    #     if end_year == None:
    #         pass
    #     ## -- Plotting -- ##
    #     raise NotImplementedError
