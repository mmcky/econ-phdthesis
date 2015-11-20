"""
NBER WTF Dataset Base Metadata Class
"""

import numpy as np

class NBERWTF(object):
    """
    Parent NBERWTF Class for Trade, Export and Import Objects that contains meta data

    Notes
    -----
    1. Source Dataset Attributes ::

        Years:          1962 to 2000
        Classification: SITC R2 L4
        Notes:          Pre-1974 care is required for constructing intertemporally consistent data

    """

    # - Attributes - #
    source_name             = u'Feenstra (NBER) World Trade Dataset'
    source_years            = xrange(1962, 2000+1, 1)
    source_classification   = 'SITC'
    source_revision         = 2
    source_level            = 4
    source_web              = u"http://cid.econ.ucdavis.edu/nberus.html"
    source_units_value      = 1000
    source_units_value_str  = u'US$1000\'s'
    source_last_checked     = np.datetime64('2014-07-04')