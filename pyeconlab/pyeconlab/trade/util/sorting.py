"""
Sorting Utilities
"""


def sorted_dataframes(dataframes, row_sortby=None, row_ascending=True, column_sortby=None, colunn_ascending=True, verbose=False, remove_nan=False, strict_index=False, how='left'):
    ''' Return Dictionary of sorted Dataframe's
        Dependency: sorted_dataframe_year()
        Updated: 
                    03/07/2013  -> Updated ascending option to allow row or column to be ascending/descending individually
    '''
    sorted_dataframes = dict()
    if type(dataframes) == dict:
        for year in dataframes:
            sorted_dataframes[year] = sorted_dataframe_year(dataframes[year], row_sortby, row_ascending, column_sortby, column_ascending, verbose, remove_nan, ascending, strict_index, how)
    else:
        print "Error: Need to pass a Dict(DataFrames) to this Function"
        return None
    return sorted_dataframes


def sorted_dataframe_year(dataframe_year, row_sortby=None, row_ascending=True, column_sortby=None, column_ascending=True, verbose=False, remove_nan=False, strict_index=False, how='left'):
    ''' Return Sorted DataFrame_Year
        Status: Current
        Sort DataFrame_Year by row or column by passing a series to sort on (Default: Ascending Sort)
        Note: Default behaviour is 'LEFT' Index Joining. Mcp matrix will be preserved ... if want matched between the two series then need to change default flag 'how' to 'inner' (intersection)
        Future Work: 1. This could be made more robust by using REGEXR to see if index1 and index2 contain the same basic info "country" vs "countrycode" - Currently only issue's advice due to limited REGEXR exploration
                     2. This Function could be re-written with new knowledge of index object creation (but not time urgent)
        Notes:
                21/06/2013   -> Updated to account for MultiIndex DataFrames. If Incoming as MultiIndex, it Returns as MultiIndex. But this function only sorts non-hierarchical indices
        Updates:
                03/07/2013   -> Updated ascending option to allow row or column to be ascending/descending individually
    '''       
    #Check Incoming dataframe Index Type
    row_index_multilevel = False
    col_index_multilevel = False
    if type(dataframe_year.index) == pd.MultiIndex:
        row_index_multilevel = True
    if type(dataframe_year.columns) == pd.MultiIndex:
        col_index_multilevel = True
    if str(how) == 'inner':
        print "WARNING: Returned DataFrame will Return the INTERSECTION of LEFT AND RIGHT Index; therefore items could be dropped from the original dataframe if data is not available"
    if type(row_sortby) == pd.Series:                                      # row_sortby
        if verbose: print 'Series Index Name: %s -> DataFrame Index Name: %s' % (row_sortby.index.name, dataframe_year.index.name)
        if row_sortby.index.name.lower() != dataframe_year.index.name.lower():
            print 'WARNING: Row Index Names are not the same!, You might be JOINING inappropriate data [Index1: %s; Index2: %s]' % (row_sortby.index.name, dataframe_year.index.name)
            # Advise USER that Index are similar based on simple REGEXR
            if len(row_sortby.index.name) <= len(dataframe_year.index.name):
                match = re.search(row_sortby.index.name.lower(), dataframe_year.index.name.lower())
            else:
                match = re.search(dataframe_year.index.name.lower(), row_sortby.index.name.lower())
            if match: print 'INDEX1 and INDEX2 are SIMILARLY NAMED .... Probably OK!'
            if strict_index: return None
        if verbose: print 'Row Sortby: %s had dimension: %s vs. DataFrame: %s' % (row_sortby.name, row_sortby.shape[0], dataframe_year.shape[0])
        joined = dataframe_year.join(row_sortby, how=how)
        sorted_joined = joined.sort(row_sortby.name, ascending=row_ascending)
        if remove_nan: sorted_joined = sorted_joined.dropna()
        del sorted_joined[row_sortby.name]
        if verbose: print 'ROW: Incoming DataFrame Length: %s --> Outgoing DataFrame Length: %s' % (len(dataframe_year), len(sorted_joined))
        if type(column_sortby) == pd.Series:                               #both row and column sortby
            if verbose: print 'Series Index Name: %s -> DataFrame Index Name: %s' % (column_sortby.index.name, dataframe_year.index.name)
            if column_sortby.index.name.lower() != dataframe_year.columns.name.lower():
                print 'WARNING: Column Index Names are not the same!, You might be JOINING inappropriate data [Index1: %s; Index2: %s]' % (column_sortby.index.name, dataframe_year.columns.name)
                if len(column_sortby.index.name) <= len(dataframe_year.columns.name):                                    #Changed 'if len(row_sortby.index.name)' to if len(column_sortby.index.name)
                    match = re.search(column_sortby.index.name.lower(), dataframe_year.columns.name.lower())
                else:
                    match = re.search(dataframe_year.index.name.lower(), column_sortby.index.name.lower())             #Changed column_sortby.columns.name.lower() to column_sortby.index.name.lower()
                if match: print 'Column INDEX1 and INDEX2 are SIMILARLY NAMED .... Probably OK!'
                if strict_index: return None
            if verbose: print 'Column Sortby: %s had dimension: %s vs. DataFrame: %s' % (column_sortby.name, column_sortby.shape[0], dataframe_year.shape[1])
            joined = sorted_joined.T.join(column_sortby, how=how)
            sorted_joined = joined.sort(column_sortby.name, ascending=column_ascending)
            if remove_nan: sorted_joined  = sorted_joined.dropna()
            del sorted_joined[column_sortby.name]
            if verbose: print 'COLUMN: Incoming DataFrame Width: %s --> Outgoing DataFrame Width: %s' % (len(dataframe_year.T), len(sorted_joined))   #Len Works here because of the Transpose
            sorted_joined = sorted_joined.T   #Move Rows back to Columns to match incoming orientation
            #Reinstate row or col MultiIndex if incoming as MultiIndex
            if row_index_multilevel:
                sorted_joined.index = pd.MultiIndex.from_tuples(sorted_joined.index)
            if col_index_multilevel:
                sorted_joined.columns = pd.MultiIndex.from_tuples(sorted_joined.columns)
            return sorted_joined
        #Reinstate row or col MultiIndex if incoming as MultiIndex
        if row_index_multilevel:
            sorted_joined.index = pd.MultiIndex.from_tuples(sorted_joined.index)
        if col_index_multilevel:
            sorted_joined.columns = pd.MultiIndex.from_tuples(sorted_joined.columns)
        return sorted_joined
    elif type(column_sortby) == pd.Series:                                 # column sortby
        if verbose: print 'Series Index Name: %s -> DataFrame Index Name: %s' % (column_sortby.index.name, dataframe_year.index.name)
        if column_sortby.index.name.lower() != dataframe_year.columns.name.lower():
            print 'WARNING: Column Index Names are not the same!, You might be JOINING inappropriate data [Index1: %s; Index2: %s]' % (column_sortby.index.name, dataframe_year.columns.name)
            if len(column_sortby.index.name) <= len(dataframe_year.columns.name):                                        #Changed 'if len(row_sortby.index.name)' to if len(column_sortby.index.name)
                match = re.search(column_sortby.index.name.lower(), dataframe_year.columns.name.lower())
            else:
                match = re.search(dataframe_year.index.name.lower(), column_sortby.index.name.lower())                #Changed column_sortby.columns.name.lower() to column_sortby.index.name.lower()
            if match: print 'Column INDEX1 and INDEX2 are SIMILARLY NAMED .... Probably OK!'
            if strict_index: return None
        if verbose: print 'Column Sortby: %s had dimension: %s vs. DataFrame: %s' % (column_sortby.name, column_sortby.shape[0], dataframe_year.shape[1])
        joined = dataframe_year.T.join(column_sortby, how=how)
        sorted_joined = joined.sort(column_sortby.name, ascending=column_ascending)
        if remove_nan: sorted_joined  = sorted_joined.dropna()
        del sorted_joined[column_sortby.name]
        if verbose: print 'COLUMN: Incoming DataFrame Width: %s --> Outgoing DataFrame Width: %s' % (len(dataframe_year.T), len(sorted_joined)) #Len Works here because of the Transpose
        sorted_joined = sorted_joined.T
        #Reinstate row or col MultiIndex if incoming as MultiIndex
        if row_index_multilevel:
            sorted_joined.index = pd.MultiIndex.from_tuples(sorted_joined.index)
        if col_index_multilevel:
            sorted_joined.columns = pd.MultiIndex.from_tuples(sorted_joined.columns)
        return sorted_joined
    else:
        print "Error: Need to pass a row_sortby series and/or a column_sortby series"
        return None  


def sort_multiindex_by_sortorder(dataframe_year, sort_level_by, axis=1, how='left'):
    ''' Function: sort_multiindex_by_sortorder - This function sorts a MultiIndex DataFrame (i.e. Two Concordances) and sorts 1 or more levels by a conversion dictionary
        Status: IN-TESTING/DEVELOPMENT (Rows)
        Input: Requires a DataFrame and a sort_level_by group of concordable Dictionaries. Dict(Level : Dict( 'ProductCode' : SortOrder))
        Notes: Sort Ordering can be made ascending descending by changing the SortOrder information in your Dict('ProductCode')
        Options:    sort_level_by   -> dict('level': 'sort_by') where sort_by is a Dict(Key: SortOrderingValue)
                    axis            -> Specifices which axis (axis=0: Rows; axis=1: Columns)
                    how             -> 'merge type'
        Notes: Row is currently not implimented!
    '''
    if type(sort_level_by) != dict:
        print "ERROR: sort_level_by must be a dictionary('level': sort_by), and sort_by is a dictionary('index_match' : sort_ordering)"
        return None
    if axis == 0:
        print "NOT YET IMPLIMENTED!"
        row_index = dataframe_year.index
        return None
        #TO BE IMPLIMENTED#
    elif axis == 1:
        df = dataframe_year.copy()
        col_index = df.columns
        num_levels = len(col_index.levels)
        sort_index = []   #List of Tuples
        #Produce a Temporary Sortable Index
        for item in col_index:
            nw_item = []
            for level in range(0, num_levels, 1):
                if level in sort_level_by.keys():               #Then there is a replacement sort_by
                    sort_by = sort_level_by[level]
                    try:                                        #Catch Any that don't match the based sort_by dictionary
                        nw_item.append(sort_by[item[level]])
                    except: 
                        nw_item.append(np.nan)
                else:
                    nw_item.append(item[level])
            sort_index.append(tuple(nw_item))
        new_cols = pd.MultiIndex.from_tuples(sort_index)
        df.columns = new_cols
        sorted_df = df.sort_index(axis=1)
        #Reverse the sort_by correlation for each level
        r_sort_level_by = dict()
        for level in sort_level_by.keys():
            sort_by = sort_level_by[level]
            r_sort_level_by[level] = dict([[v,k] for k,v in sort_by.items()])
        #Replace Temporary Sortable Information with their original Labels
        col_index = sorted_df.columns
        num_levels = len(col_index.levels)
        sort_index = []   #List of Tuples
        #Rebuild Index 
        for item in col_index:
            nw_item = []
            for level in range(0, num_levels, 1):
                if level in r_sort_level_by.keys():               #Then there is a replacement sort_by
                    sort_by = r_sort_level_by[level]
                    try:                                        #Catch Any that don't match the based sort_by dictionary
                        nw_item.append(sort_by[item[level]])
                    except: 
                        nw_item.append(np.nan)
                else:
                    nw_item.append(item[level])
            sort_index.append(tuple(nw_item))
        new_cols = pd.MultiIndex.from_tuples(sort_index)
        sorted_df.columns = new_cols
        return sorted_df
    else:
        print "ERROR: Need to choose axis 0 (row) or 1 (column)"
    return None