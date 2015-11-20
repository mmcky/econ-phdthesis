"""
pyeconlab.trade.dataset.NBERWTF.meta Fixes File
================================================

This is a fixes file and contains the special fixes required due to errors in the original dataset

"""


fix_exporter_to_iso3n =     {
                                'Asia NES'  : 896,
                                'Italy'     : 381,
                                'Norway'    : 579,
                                'Switz.Liecht' : 757,
                                'Samoa'     : 882,
                                'Taiwan'    : 158,
                                'USA'       : 842,
                            }

fix_ecode_to_iso3n =        {                                       
                                '450000'    : 896,
                                '533800'    : 381,
                                '555780'    : 579,
                                '557560'    : 757,
                                '728882'    : 882,
                                '458960'    : 158,
                                '218400'    : 842,
                            }
# Checked With: 
# df = a.raw_data
# df.loc[df.exporter==<item from fix_exporter_to_iso3n>].ecode.unique()


fix_exporter_to_iso3c =     {
                                'Asia NES'  : '.',
                                'Italy'     : 'ITL',
                                'Norway'    : 'NOR',
                                'Switz.Liecht' : 'CHE',
                                'Samoa'     : 'WSM',
                                'Taiwan'    : 'TWN',        #Note this is also 480 (Other Asia, NES)
                                'USA'       : 'USA',
                            }

# Note: These are technically duplicates, but does it help to keep the logic separable?

fix_importer_to_iso3n =     { 
                                'Asia NES'  : 896,
                                'Italy'     : 381,
                                'Norway'    : 579,
                                'Samoa'     : 882,
                                'Switz.Liecht' : 757,
                                'Taiwan'    : 158,
                                'USA'       : 842,
                            }

fix_icode_to_iso3n =        { 
                                '450000'    : 896,
                                '533800'    : 381,
                                '555780'    : 579,
                                '728882'    : 882,
                                '557560'    : 757,
                                '458960'    : 158,
                                '218400'    : 842,
                            }
# Checked With: 
# df = a.raw_data
# df.loc[df.importer==<item from fix_importer_to_iso3n>].icode.unique()

fix_importer_to_iso3c =     {
                                'Asia NES'  : '.',
                                'Italy'     : 'ITL',
                                'Norway'    : 'NOR',
                                'Samoa'     : 'WSM',
                                'Switz.Liecht' : 'CHE',
                                'Taiwan'    : 'TWN',
                                'USA'       : 'USA',

                            }

#-Documented in FAQ-#
#-Use ZWE and MWI from 1965 Onwards-#
fix_nber_africa_trade_flows = {
    "drop" :    {
                'ZWE' : [1962,1963,1964],
                'MWI' : [1962,1963,1964],
                },
}