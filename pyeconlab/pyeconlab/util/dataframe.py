"""
DataFrame Utilities
===================

This provides various utilties useful in manipulating pandas DataFrame objects

Organisation
------------
1. Index Functions
2. Merge Functions 
3. Row Finding Functions
4. Sampling Functions
5. Attribute Functions
6. Assert Functions
7. Intertemporal/Dynamic Functions
---
#. Staging Area

Future Work
-----------
1. Split this file into different function types: dataframe_index.py, dataframe_merge.py etc.
2. Staging Area should be phased out in favour of branches?

"""

import copy
import re
import pandas as pd
import numpy as np
from itertools import chain, repeat

from pandas.util.testing import assert_series_equal

# ------------------- #
# - Index Functions - #
# ------------------- #

def recode_index(df, recode, axis='columns', inplace=True, verbose=True):
    """
    Recode A DataFrame Index from a Recode Dictionary and provide a summary of results

    Parameters
    ----------
    df      :   pd.DataFrame (with Index)
                The indexed Pandas Data Frame
    recode  :   dict('From' : 'To')
                A dictionary of what to recode from and to (i.e. ISO3C to CountryNames)
    axis    :   string, optional(default='columns')
                Must specify 'rows' or 'columns'
    inplace :   True/False [Default: True]

    Returns
    -------
    pd.DataFrame() with a new index

    ..  Future Work
        -----------
        1. Convert Messages to use Pretty Print For Tabular Output Etc.
        2. Construct a merge function that behaves similarly to Stata "merge"
        3. Implement for Rows
    """

    # - Parse Inplace Option - #
    if inplace == False:
        df = copy.deepcopy(df)
    recode_set = set(recode.keys())
    # - Rows - #
    if axis == 'rows':
        # - Working HERE - #
        raise NotImplementedError
        # - Working HERE - #
    # - Columns
    elif axis == 'columns':
        # - Prepare Summary - #
        old_idx = set(df.columns)
        summ_msg =  "[Recode Column Summary]" + '\n'                \
                    "# of Column Items: %s" % len(old_idx) + '\n'   \
                    "# of Recode Items: %s" % len(recode_set) + '\n'
        # - Changes - #
        common = old_idx.intersection(recode_set)
        summ_msg += "Updates: \n" + \
                    "------- \n"
        for item in common:
            summ_msg += "  %s -> %s" % (item, recode[item]) + '\n'
        summ_msg += "------- \n"
        summ_msg += "Number of Changes: %s" % len(common) + '\n'
        # - In Recode but not in Column - #
        remaining_recode = recode_set.difference(old_idx)
        if len(remaining_recode) != 0:
            summ_msg += "Unused Recode Items: %s" % remaining_recode + '\n'
        # - In Column but not in Recode - #
        remaining_column = old_idx.difference(recode_set)
        if len(remaining_column) != 0:
            summ_msg += "Unchanged Column Items: %s" % remaining_column + '\n'
        if verbose: print summ_msg
        
        # - Perform Rename Operation - #
        df.rename(columns=recode, inplace=True)         

    else:
        raise ValueError("Axis must be 'rows' or 'columns'")
    return df

# ----------------- #
# - Merge Functions - #
# ----------------- #


def merge_columns(ldf, rdf, on, collapse_columns=('value_x', 'value_y', 'value'), dominant='right', output='final', verbose=True):
    """
    Merge a LEFT and RIGHT DataFrame on a set of columns and then merge columns ``_x`` and ``_y`` specified in columns via a dominant rule. 

    Parameters
    ----------
    ldf                 :   pd.DataFrame
                            Left DataFrame Object
    rdf                 :   pd.Dataframe
                            Right DataFrame Object
    on                  :   list(str)
                            A list of items to merge on (that are common to both dataframes)    
    collapse_columns    :   tuple(str), optional(default=('value_x', 'value_y', 'value'))
                            After Performing Outer Merge Collapse Columns (LEFT, RIGHT, FINAL).
                            **Note**: This is disaggregated to allow great flexibility on L,R columns that don't share the same pre-word.
    dominant            :   str, optional(default='right')
                            Specify a dominant column: 'right' or 'left'. Anything else will return a list of the conflicts
    output              :   str, optional(default='final')
                            Specify what type of output to recieve 'final' or 'stages'. **Note**: Stages is used for debugging etc.

    Returns
    -------
    dataframe           :   pd.DataFrame

    Notes
    -----
    This function is equiped with a basic report which is switched on by default through the ``verbose`` flag

    ..  Future Work
        -----------
        [1] Could Change this to allow lists(collapse_columns)
        [2] Write Tests
        [3] This could be rewritten to make use of pandas.combine() to combine dataframes. Would be better tested

    """
    #-Parse collapse_columns-#
    left_col, right_col, final_col = collapse_columns
    
    #-Number of Observations in Both-#
    num_ldf = len(ldf)
    num_rdf = len(rdf)
    
    #-Inner Merge-#
    inner = ldf.merge(rdf, how='inner', on=on)
    num_matched = len(inner)                            #Number of Inner Matches
    # Compute Relative to Inner Merge - #
    num_new_obs_from_right = num_rdf - num_matched
    num_old_obs_from_left = num_ldf - num_matched
    
    #-Compute Outer Merge-#
    outer = ldf.merge(rdf, how='outer', on=on)
    #Construct Final Merged Columns
    #-New Observations from Right-#
    outer[final_col] = np.where(outer[left_col].isnull(), outer[right_col], outer[left_col])        #Bring in NEW Observations, Else Return Old Observations
    #-Manage Conflicts-#
    right = ldf.merge(rdf, how='right', on=on)[[left_col, right_col]].dropna()
    if dominant.lower() == 'right':
        num_discarded_from_right = 0
        num_overwrite_from_right = len(right[right[left_col] != right[right_col]])
        num_equal_left_right = len(right[right[left_col] == right[right_col]])
            ## --> REMOVE <-- ##
            # - Fill Missing RIGHT_COL Values with Final Values - #
            #outer['tmp_right_col'] = np.where(outer[right_col].isnull(), outer[final_col], outer[right_col])
            # - Update Unequal Values in Final Column with Right Values - #
            #outer[final_col] = np.where(outer[final_col] != outer['tmp_right_col'], outer['tmp_right_col'], outer[final_col])
            ## -------------- ##
        outer[final_col] = np.where(outer[right_col].isnull(), outer[final_col], outer[right_col])
    elif dominant.lower() == 'left':
        # - This is the default state in initial construction of outer [final_col] Merging New Observations - #
        num_discarded_from_right = len(right[right[left_col] != right[right_col]])
        num_overwrite_from_right = 0
        num_equal_left_right = len(right[right[left_col] == right[right_col]])
    else:
        # Show Conflict and Quit - #
        print "[ERROR] Cannot Resolve Conflicts!"
        right = ldf.merge(rdf, how='right', on=on).dropna(subset=[left_col,right_col])
        conflicts = right[right[left_col] != right[right_col]][on + [left_col, right_col]]
        print conflicts[0:10]
        return conflicts

    #-Write Report-#
    report =    u"---------------------------------\n"                                  + \
                u"MERGE Report [Rule: %s, LEFT: %s, RIGHT: %s]\n" % (dominant, left_col, right_col) + \
                u"------------\n"                                                       + \
                u"# of Left Observations: \t%s\n" % (num_ldf)                           + \
                u"# of Right Observations: \t%s\n" % (num_rdf)                          + \
                u"  `ON` Matched Observations: \t%s\n" % (num_matched)                  + \
                u"\n"                                                                   + \
                u"LEFT [%s] STATS:\n" % left_col                                        + \
                u"----------\n"                                                         + \
                u"# of Unmatched (Old) Observations (from LEFT): \t\t%s\n" % (num_old_obs_from_left) + \
                u"# of Right values DISCARDED in preference of Left: \t%s\n" % (num_discarded_from_right) + \
                u"\n"                                                                   + \
                u"RIGHT [%s] STATS:\n" % right_col                                      + \
                u"----------\n"                                                         + \
                u"# of Unmatched (New) Observations (from RIGHT): \t%s\n" % (num_new_obs_from_right) + \
                u"# of Left values OVERWRITTEN in preference of Right: \t%s\n" % (num_overwrite_from_right) + \
                u"\n"                                                                   + \
                u"# of Left values EQUAL to Right values [No Change]: \t%s\n" % (num_equal_left_right) + \
                u"\n"                                                                   + \
                u"Total Number of FINAL Observations: \t%s\n" % (len(outer))            +\
                u"---------------------------------\n"                                          
    
    #-Output Type-#
    if output == 'final':
        # Note: If other data is present, they will retain _x and _y variables
        del outer[left_col]
        del outer[right_col]
            ## -> REMOVE <- ##
            # if dominant == 'right':
            #   del outer['tmp_right_col']
            ## ------------ ##
    elif output == 'stages':
        pass
    else:
        raise ValueError("Output type must be `final` or `stages`") 
    #-Parse Verbosity-#
    if verbose: 
        print report
    return outer


# ------------------------- #
# - Row Finding Functions - #
# ------------------------- #

def find_row(df, row):
    """
    Find and Return a Row in a DataFrame

    Parameters
    ----------
    df  :   pd.DataFrame
            Dataframe containing the data
    row :   pd.Series(?)
            Row to look for in the dataframe

    ..  Future Work
        1. This needs testing
        2. This needs an example to confirm ``row`` types 

    """
    return df.loc[((df == row) | (df.isnull() & row.isnull())).all(1)]


# ------------------------ #
# - Comparison Functions - #
# ------------------------ #

def compare_idx_items(left, right, left_column, right_column, how='outer', tol=100, return_merged=False, dropna=False):
    """ 
    Compare Two Indexed Series or Columns

    Parameters
    ----------
    left            :   pd.Series or pd.DataFrame 
                        Left Series
    right           :   pd.Series or pd.DataFrame
                        Right Series
    left_column     :   str 
                        Specify Left Column Name
    right_column    :   str 
                        Specify Right Column Name
    how             :   str, optional(default='outer')
                        Specify what type of merge: 'left', 'right', 'inner', 'outer'
    tol             :   int, optional(default=100)
                        Specify a tolerance for considering equality in values
    return_merged   :   bool, optional(default=False)
                        Return the merge dataframe or not, useful in debugging. 
    dropna          :   bool, optional(default=False)
                        Set whether dropna should be performed. 
    Notes
    -----
    This function will automatically display a simple report

    """
    assert left.index.names == right.index.names, "Objects must share the same Index Names"
    merged = left.merge(right, how=how, left_index=True, right_index=True, sort=True)
    if left_column == right_column:
        left_column += "_x"
        right_column += "_y"
    merged = merged.rename(columns={left_column : 'left', right_column : 'right'})
    if dropna:
        merged = merged.dropna()
    merged['diff'] = abs(merged['left'] - merged['right'])
    merged['eq'] =  merged['diff'] <= tol 
    merged['lgr'] = merged['left'] > merged['right']
    merged['rgl'] = merged['left'] < merged['right']
    #-Stats-#
    eq = merged['eq'].describe()
    lgr = merged['lgr'].describe()
    rgl = merged['rgl'].describe()
    report = "Comparison between Left: (%s) and Right: (%s)\n"              % (left_column, right_column)                               + \
             "Number of Equal values: %s (%s percent) [Tolerance: %s]\n"    % (int(eq['mean']*eq['count']), int(eq['mean']*100), tol)   + \
             "Number of Left > Right: %s (%s percent) [No Tolerance]\n"     % (int(lgr['mean']*lgr['count']), int(lgr['mean']*100))     + \
             "Number of Left < Right: %s (%s percent) [No Tolerance]\n"     % (int(rgl['mean']*rgl['count']), int(rgl['mean']*100))
    print report
    if return_merged:
        return merged

def compare_dataframe_rows(base, comparison):
    """
    Compare Rows in base DataFrame to see if they are contained in the comparison dataframe and return differences

    Parameters
    ----------
    base        :   pd.DataFrame 
                    Supply a Pandas DataFrame
    comparison  :   pd.DataFrame 
                    Supply a Pandas DataFrame 

    """
    matched = base.loc[((base == comparison) | (base.isnull() & comparison.isnull())).all(1)]
    notmatched = base.loc[((base == comparison) | (base.isnull() & comparison.isnull())).all(1) == False]
    return (matched, notmatched)


# --------------------- #
# - Duplicates Report - #
# --------------------- #

def mark_duplicates(df, on=None, level=None):
    """
    Add a Duplicate Marker to a DataFrame
    
    Warnings
    --------
    Status: IN-WORK

    Parameters
    ----------
    df      :   pd.DataFrame 
                DataFrame to perform task on
    on      :   list(str), optional(default=None)
                specify columns to check for duplicates. Default performs duplicates over entire dataframe
    level   :   specify levels to check for duplicates

    """
    if type(on) == list:
        #-Duplicates by Columns-#
        counts = df.groupby(on).size()
        counts = pd.DataFrame(counts, columns = ['duplicates'])
        df = df.set_index(keys=on).join(counts, how='outer').reset_index()
        return df
    elif type(level) == list:
        #-Duplicates by Levels-#
        raise NotImplementedError
    else:
        #-Duplicates over default-#
        raise NotImplementedError


# ---------------------- #
# - Sampling Functions - #
# ---------------------- #

def random_sample(df, sample_size = 1000):
    """
    Returns a Random Sample of Rows in a Dataframe

    Parameters
    ----------
    df  :   pd.DataFrame 
            DataFrame to extract random rows from.
    sample_size     :   int, optional(default=1000)
                        Determines the size of the Random Sample

    """
    rows = np.random.choice(df.index.values, sample_size)
    return df.ix[rows]



# ----------------------- #
# - Attribute Functions - #
# ----------------------- #

def update_operations(self, add_op_string):
    """ 
    Update an Operations attribute on a class attribute .operations 

    Parameters
    ----------
    add_op_string   :   str
                        A string to append to the current operations string. 

    Notes
    -----
    1. If no ``operations`` attribute is found then it constructs the attribute.
    2. Should this reset complete_dataset (# self.complete_dataset = False           #In General this is true)
    """
    try:
        if type(self.operations) == str or type(self.operations) == unicode:
            self.operations += add_op_string
    except:
        self.operations = add_op_string


def check_operations(self, opstring, verbose=False):
    """ 
    Check if Operation has been conducted on class attribute .operations
    
    Parameters
    ----------
    opstring   :   str
                        A string to append to the current operations string. 

    Returns
    -------
    value       :   bool
                    True = Opstring found, False = Opstring not found

    Raises
    -------
    ValueError
        If no ``operations`` attribute is found

    ..  Future Work   
        1. Standardise opstring variable names

    """
    try:
        if re.search(opstring, self.operations):
            if verbose: print "[INFO] Operation %s has already been conducted on dataset" % opstring 
            return True
        else:
            return False
    except:
        raise ValueError("The incoming class does not have an operations attribute")


# -------------------- #
# - Assert Functions - #
# -------------------- #

#-Fairly Simple Programs, docstrings considered good enough-#

def assert_unique_row_in_df(df, row):
    """
    Assert a Unique Row in DataFrame
    """ 
    assert len(find_row(df, row)) == 1, "Row (%s) Not Found in DataFrame OR has multiple matches (try utils.find_row())" % row

def assert_row_in_df(df, row):
    """
    Assert Row is found in DataFrame 
    """
    assert len(find_row(df, row)) >= 1, "Row (%s) Not Found in Dataset" % row

def assert_unique_rows_in_df(df, rows):
    """
    Assert All Unique Rows in a Dataframe of Rows in the DataFrame
    """
    for idx, row in rows.iterrows():
        assert_unique_row_in_df(df, row)

def assert_rows_in_df(df, rows):
    """
    Assert All Rows are found in the DataFrame 
    """
    for idx, row in rows.iterrows():
        assert_row_in_df(df, row)

def assert_merged_series_items_equal(s1, s2):
    """
    Assert the Inner Join of Two Series Are Equal
    Note: This joins based on index
    """
    s1 = s1.copy()                                      #Don't Alter incoming Series
    s1.name = 's1'
    s2 = s2.copy()
    s2.name = 's2'
    merged = pd.concat([s1, s2], axis=1, join='inner')  #Get Inner Mapping
    assert_series_equal(merged['s1'], merged['s2'])
    
def check_merged_series_items_equal(s1, s2):
    """
    Assert the Inner Join of Two Series Are Equal
    Note: This joins based on index
    """
    try:
        assert_merged_series_items_equal(s1,s2)
        return True
    except:
        return False    

# - Examples of Different Ways to Impliment - #

# This eventually should be removed from the library as it doesn't serve any purpose here. 
# Currently kept as different ways to achieve the same things as a memory tool, and as a repostory
# of examples

def check_rows_from_random_sample_byduplicated(df, rs):
    """
    Iterate over a Random Sample to Make Sure the row is contained in the DataFrame
    Approach: Using Duplicated()

    Notes:
    ------
    [1] timeit: 9.39 s per loop [Same Data as Other check_rows*] 
    """
    #-Check Duplicates Initial Condition-#
    if len(df[df.duplicated()]) != 0:
        raise ValueError("[ERROR] Dataset Already Contains Duplicate Rows!")
    for idx, row in rs.iterrows():
        #-Check if Row is in Data-#
        tmp = df.append(row)
        assert len(tmp[tmp.duplicated()]) == 1, "A duplicate row wasn't found for %s" % (row)

def check_rows_from_random_sample_byiterating(df, rs):
    """
    Iterate over a Random Sample to Make Sure the row is contained in the DataFrame
    Iterating and using equal()

    STATUS: [NOT-WORKING] 
            Equality in the presence of NaN is not established. Using .equal() should account for this

    Notes:
    ------
    [1] timeit: N/A [Same Data as Other check_rows*] 
    """
    match = False
    for rsidx, rsrow in rs.iterrows():
        for idx, s in df.iterrows():
            if s.equals(rsrow):
                match = True
                break
        assert match == True, "Iterating didn't find an equal row %s in the dataframe" % (rsrow)

def check_rows_from_random_sample_byfiltering(df, rs):
    """
    Iterate over a Random Sample to Make Sure the row is contained in the DataFrame
    Filtering Approach

    Notes:
    ------
    [1] timeit: 1.72 s per loop [Same Data as Other check_rows*] 
    """
    for rsidx, rsrow in rs.iterrows():
        tmp = df
        for idx, val in rsrow.iteritems():
            try:
                if np.isnan(val):
                    continue
            except:
                pass
            tmp = tmp[tmp[idx] == val]
        assert len(tmp) == 1, "Filtering didn't produce a unique line in the dataframe: %s" % (len(tmp))

def check_rows_from_random_sample_bybroadcasting(df, rs):
    """
    Brodcast over a DataFrame Looking for rows in DataFrame

    Notes:
    ------
    [1] timeit: 4.14 s per loop [Same Data as Other check_rows*] 
    """
    for rsidx, rsrow in rs.iterrows():
        assert len(df[((df == rsrow) | (df.isnull() & rsrow.isnull())).all(1)]) == 1

def check_rows_from_random_sample_bybroadcasting_columniteration(df, rs):
    """
    Broadcast and Iterate Over Columns (Smaller Dimension) due to long data relative to width

    Notes:
    ------
    [1] timeit: 1.63 s per loop [Same Data as Other check_rows*]
    [2] This is the fastest implementation and is used in assert_functions 
    """
    def finder(df, row):
        for col in df:
            df = df.loc[(df[col] == row[col]) | (df[col].isnull() & pd.isnull(row[col]))]           #This will return if the LAST ELEMENT is EQUAL ONLY! ISSUE
        return df
    
    for rsidx, rsrow in rs.iterrows():
        assert len(finder(df, rsrow)) == 1




# ----------------------------------- #
# - Intertemporal/Dynamic Functions - #
# ----------------------------------- #

def compute_number_of_spells(wide_df, inplace=False):
    """
    Compute Number of Spells in a Wide DataFrame for Each Row
    This is based on identifying unique codes in a series. 

    Parameters
    -----------
    wide_df 	: 	pd.DataFrame(Time Columns)
    				DataFrame containing Columns indexed by Time (i.e. Years)
    Notes
    -----
    1. 	The Columns must contain time data ::
    	DataFrame = index, year_t .. year_T
    2. 	continuous spells can be computed using compute_number_of_continuous_spells() method.

    """
    #-Helper Functions-#
    def num_spells(x):
        """ Compute the spells in each row """
        t = list(x.dropna().unique())
        r = []
        for el in x:
            if not np.isnan(el):                
                r.append(t.index(el)+1)
            else:
                r.append(np.nan)            #Handle np.nan case
        return r
    #-Options-#
    if not inplace:
        wide_df = wide_df.copy(deep=True)
    #-Core-#
    wide_df = wide_df.apply(num_spells, axis=1)
    return wide_df

def compute_number_of_continuous_spells(wide_df, inplace=False):
    """
    Compute Number of Continuous Spells in a Wide DataFrame for Each Row

    Parameters
    -----------
    wide_df 	: 	pd.DataFrame(Time Columns)
    				DataFrame containing Columns indexed by Time (i.e. Years)
	inplace 	: 	bool, optional(default=False)
					Performs the Operation In Place on the Incoming DataFrame

    Notes
    -----
    1. 	The Columns must contain time data ::
    	index, year_t .. year_T
    2. 	[Improvement] This function needs to account for non-adjacent np.nan occurances to compute the number of continuous spells. 
    	Perhaps parsing each row and adjusting np.nan() occurances to be [-1, -1, val, -2, ... -n etc.] for [np.nan, np.nan, 4, np.nan,]
    	This could be done using compute_spell_lengths?

    .. 	Future Work
    	-----------
    	[1] Add Tests
    """
    #-Helper Function-#
    def num_spells(s):
        """ Compute the spells in each row """
        uniq_codes = list(s.dropna().unique())
        spell = 1
        for idx, item in s.iteritems():
            next_idx = idx+1
            s[idx] = spell
            if np.isnan(item):
                #spell += 1         #increment spell?
                s[idx] = np.nan
                continue
            if next_idx > s.index[-1]:
                break
            if item != s[next_idx]:
                spell += 1          #increment spell
        return s
    #-Options-#
    if not inplace:
        wide_df = wide_df.copy(deep=True)
    #-Core-#
    wide_df = wide_df.apply(num_spells, axis=1)
    return wide_df

def compute_spell_lengths(wide_df, incremental=False, inplace=False):
    """
    Compute Spell Lengths for Wide Time Based DataFrames

	Parameters
    -----------
    wide_df 	: 	pd.DataFrame(Time Columns)
    				DataFrame containing Columns indexed by Time (i.e. Years)
    incremental : 	bool, optional(default=False)
    				If True it returns an incremental count [1, 2, 3 ... ] of the spell lengths.
    				If False it returns the final length of each spell
	inplace 	: 	bool, optional(default=False)
					Performs the Operation In Place on the Incoming DataFrame
    Notes
    -----
    [1] Useful for computing dynamic or intertemporal data in computing length of spells across years in a wide dataframe
    
    .. 	Future Work
    	-----------
    	[1] Add Tests

    """
    #-Helper Functions-#
    def spell_len(s):
        spell = 1
        for idx, item in s.iteritems():
            next_idx = idx+1
            if np.isnan(item):
                spell = 1
                s[idx] = np.nan
                continue
            if next_idx > s.index[-1]:
                s[idx] = spell
                break
            s[idx] = spell
            if item == s[next_idx]:
                spell += 1
            else:
                spell = 1
        return s

    def rewrite_group(s, group, group_items):
        for gidx in group:
            s[gidx] = max(group_items)

    def final_len(s):
        group = []
        group_items = []
        for idx, item in s.iteritems():
            next_idx = idx+1
            group.append(idx)
            group_items.append(item)
            if next_idx > s.index[-1]:  
                rewrite_group(s, group, group_items)
                break
            if s[next_idx] > item:
                continue
            else:
                #-Rewrite Max Value for Group-#
                rewrite_group(s, group, group_items)
                group = []
                group_items = []
        return s
    #-Options-#
    if not inplace:
        wide_df = wide_df.copy(deep=True)
    #-Core-#
    wide_df = wide_df.apply(spell_len, axis=1)
    if incremental:
        return wide_df
    wide_df = wide_df.apply(final_len, axis=1)
    return wide_df



# ----------- #
# - IN WORK - #
# ----------- #

#-None-#
