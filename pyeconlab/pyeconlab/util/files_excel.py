"""
Utilities for Working with Excel Files
"""

import pandas as pd

#-Compare Contents of Two Excel Files-#

def assert_excel_equal(fl1, fl2, has_index_names=True):
    """
    Compare the Contents of Two Files (fl1, fl2)

    Parameters
    ----------
    fl1     			:   str
                			Specify Filename 1
    fl2     			:   str
                			Specify Filename 2
    has_index_names     :   bool, optional(default=True)
                            Excel Option 

    Notes
    -----
    1. This is useful when comparing two files that don't always have static contents (like xls documents)
    """
    a = pd.read_excel(fl1, has_index_names=has_index_names)
    b = pd.read_excel(fl2, has_index_names=has_index_names)
    assert a.equals(b), "File: %s != %s" (fl1, fl2)