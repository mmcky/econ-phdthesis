"""
Atlas of Complexity Dataset Base Metadata Class
===============================================

Provides META data for the Atlas of Complexity Project Dataset

"""

import numpy as np

class AtlasOfComplexity(object):
    """
    Base Data for Atlas of Complexity Dataset
    """
    #-Source Information-#
    source_classifications=["SITCR2", "HS92"]
    source_years = {"HS92" : xrange(1995,2012+1,1), "SITCR2" : xrange(1962,2012+1,1)}
    source_level = {"HS92" : 4, "SITCR2" : 4}
    source_dtypes = ["trade", "export", "import"]
    source_trade_datafl = {"HS92" : "year_origin_destination_hs.tsv", "SITCR2" : "year_origin_destination_sitc.tsv"}
    source_exportimport_datafl = {"HS92" : "year_origin_hs.tsv", "SITCR2" : "year_origin_sitc.tsv"}
    source_country_datafl = "country.tsv"
    source_product_datafl = {"HS92" : "hs4.tsv", "SITCR2" : "sitc4.tsv"}
    source_hashfl = "hash.md5"

    source_data_http = u"https://atlas.media.mit.edu/about/data/download/"

    #-File Interfaces-#
    #-Trade-#
    source_tradefl_interfaces = {
            "SITCR2"  : ["year", "origin", "destination", "sitc4", "export_val", "import_val"], 
            "HS92"    : ["year", "origin", "destination", "hs4", "export_val", "import_val"], 
    }
    #-Export/Import-#
    source_exportimportfl_interfaces = {    
            "SITCR2"  : ["year", "origin", "sitc4", "export_val", "import_val", "export_rca", "import_rca"],
            "HS92"    : ["year", "origin", "hs4", "export_val", "import_val", "export_rca", "import_rca"], 
    }

    units_value_str = "$'s"
