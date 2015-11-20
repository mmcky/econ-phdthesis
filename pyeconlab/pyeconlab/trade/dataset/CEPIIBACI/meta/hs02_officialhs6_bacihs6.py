"""
Check Official HS6 Codes Vs BACI Codes

File Type: Recipe

Classification: HS02

Note: this is a recipe for comparing codes found in the official 'un' 6 digit HS codes with those in BACI
The data is found in ../dataset.py as an attribute for the dataset object

Future Work
-----------
[1] This is probably not the best place for this. Move to more appropriate place later

"""

#-Dataset-#
from pyeconlab.trade.dataset import BACIConstructor
source_dir = r"C:\Users\Matt-Work\work\testing\baci_test"
a = BACIConstructor(source_dir=source_dir, src_class='HS02', ftype='hdf')

#-Official Codes-#
from pyeconlab.trade.classification import HS2002
op = HS2002()
op = set(op.L6.Code.values)

dp = set(a.dataset.hs6.values)

len(op) 						#5226
len(dp) 						#5219
len(dp.intersection(op)) 		#5218
dp - op 						#{'271000'}
op - dp  						#{'271011', '271019', '271091', '271099', '710820', '711890', '999999', '9999AA'}