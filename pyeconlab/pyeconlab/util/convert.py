"""
Conversion Utilities
====================

Contains a number of conversion utilities for converting objects.
For example, ``from_dict_to_csv`` will convert a sorted dictionary to a csv file

Notes
------
#. Should These be in files.py?
"""

import csv
import pandas as pd

# ------------- #
# - CSV Files - #
# ------------- #

def from_dict_to_csv(dictionary, fl, header=['key', 'value'], target_dir='csv/'):
    """
    Write a sorted Python Dictionary to CSV

    Parameters
    ----------
    dictionary  :   dict
                    Some dictionary of data
    fl          :   str 
                    Specify a file name
    header      :   list(str), optional(default=['key', 'value'])
                    Provide Headers for csv file
    target_dir  :   str, optional(default="csv/")
                    Looks for a local ``csv/`` directory

    """
    fl = open(target_dir + fl, 'wb')
    writer = csv.writer(fl)
    writer.writerow(header)
    tmp = []
    for key, value in dictionary.items():
        tmp.append((key, value))
    tmp = sorted(tmp)
    for key, value in tmp:
        writer.writerow([key, value])
    fl.close()

# ------------ #
# - Py Files - #
# ------------ #

def from_series_to_pyfile(series, target_dir='data/', fl=None, docstring=None):
    """
    Construct a ``py`` formated file from a Pandas Series Object 
    This is useful when wanting to generate a python list for inclusion in the package

    Parameters
    ---------
    series      :   pd.Series (that has an index)
                    The Pandas Series to Convert
    target_dir  :   str, optional(default="data/")
                    Specify a Target Directory
    fl          :   str, optional(default=None)
                    specify a filename, otherwise one will be generated
    docstring   :   str, optional(default=None)
                    specify a docstring at the top of the file   

    Warning
    -------
    Ensure `series` is named (using the ``.name`` attribute) what you would like the variable to be named in the file

    Example
    -------
    s = pd.Series([1,2,3,4])
    will write a file with:  s.name = [ 1,
                                        2,
                                        ...
                                      ]
    Notes
    -----
    #. Should ``series`` in the function be changed to ``idxseries``?
    """
    if type(series) != pd.Series:
        raise TypeError("series: must be a pd.Series")
    doc_string = u'\"\"\"\n%s\nManual Check: <date>\n\"\"\"\n\n' % docstring    # DocString
    items = u'%s = [' % series.name.replace(' ', '_')                           # Replace spaces with _
    once = True
    for idx, item in enumerate(series.values):
        # - Newline and Tabbed Spacing for Vertical List of Items - #
        tabs = 4
        if once == True:
            items += "\n"
            once = False
        items += '\t'*tabs + '\'' + '%s'%item + '\''  + ',' + '\n'
    doc = doc_string + items + ']\n'
    if type(fl) in [str, unicode]:
        # Write to Disk #
        f = open(target_dir+fl, 'w')
        f.write(doc)
        f.close()            
    else:
        return doc



def from_idxseries_to_pydict(series, target_dir='data/', fl=None, docstring=None, verbose=False):
    """
    Construct a ``py`` file containing a Dictionary from an Indexed Pandas Series Object
    This is useful when wanting to generate a python list for inclusion in the package

    Parameters
    ---------
    series      :   pd.Series (that has an index)
                    The Pandas Series to Convert
    target_dir  :   str, optional(default="data/")
                    Specify a Target Directory
    fl          :   str, optional(default=None)
                    specify a filename, otherwise one will be generated
    docstring   :   str, optional(default=None)
                    specify a docstring at the top of the file 

    Warning
    -------
    Ensure `series` is named (using the ``.name`` attribute) what you would like the variable to be named in the file

    Example
    -------

    s.name = {  index : value,
                ... etc.
             }
    """
    if type(series) != pd.Series:
        raise TypeError("series: must be a pd.Series with an Index")
    docstring = u'\"\"\"\n%s\nManual Check: <date>\n\"\"\"\n\n' % docstring     # DocString
    items = u'%s = {' % series.name.replace(' ', '_')                           # Replace spaces with _
    once = True
    for idx, val in series.iteritems():
        # - Newline and Tabbed Spacing for Vertical List of Items - #
        tabs = 4
        if once == True:
            items += "\n"
            once = False
        items += '\t'*tabs + '\'' + '%s'%idx + '\''  + ' : ' + '\'' + '%s'%str(val).replace("'", "\\'") + '\'' + ',' + '\n'             #Repalce Internal ' with \'
    doc = docstring + items + '}\n'
    if type(fl) in [str, unicode]:
        # Write to Disk #
        if verbose: print "[INFO] Writing file: %s" % (target_dir+fl)
        f = open(target_dir+fl, 'w')
        f.write(doc)
        f.close()
    else:
        return doc  
