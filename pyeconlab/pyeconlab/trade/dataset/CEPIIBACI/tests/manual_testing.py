"""
Manual Testing Code Snippets
"""

# Test 1: Compare _SC_ method with construct_sitc_dataset()
import os
from pyeconlab import BACIConstructor
SOURCE_DIR = os.path.expanduser("~/work-data/datasets/e988b6544563675492b59f397a8cb6bb/")
baci = BACIConstructor(source_dir=SOURCE_DIR, source_classification="HS96", standard_names=True)
#-Construct SC-#
sc = baci.construct_dataset_SC_CP_SITCR2L3_Y1998to2012(data_type="export")
del baci
#-Construct SITC-#
baci = BACIConstructor(source_dir=SOURCE_DIR, source_classification="HS96", standard_names=True)
ex = baci.construct_sitc_dataset(data_type="export", dataset="A", product_level=3, report=False, dataset_object=True)
print sc
print ex 

from pyeconlab.util import compare_dataframe_rows
s,d = compare_dataframe_rows(ex.data, sc.data.reindex_like(ex.data))
d

sc.data.ix[1998].ix["ALB"].ix["211"]
ex.data.ix[1998].ix["ALB"].ix["211"]

#Outcome Test 1: There are some differences between the two methods becuase they use different
#country concordances and construct_sitc_dataset() uses the latest with additional countries such as South Sudan etc. 