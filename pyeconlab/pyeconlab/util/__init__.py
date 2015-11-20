"""
Subpackage: Utilities
"""

from .convert   	import  from_series_to_pyfile, from_idxseries_to_pydict, from_dict_to_csv   
from .files     	import  home_folder, check_directory, package_folder, verify_md5hash, expand_homepath
from .files_excel 	import 	assert_excel_equal
from .dataframe 	import  recode_index, random_sample, merge_columns, update_operations, check_operations,                                            \
                        	find_row, assert_unique_row_in_df, assert_row_in_df, assert_unique_rows_in_df, assert_rows_in_df,                           \
                        	compute_number_of_spells, compute_spell_lengths, assert_merged_series_items_equal, check_merged_series_items_equal,         \
                        	mark_duplicates, compare_idx_items, compare_dataframe_rows
from .concordance 	import 	countryname_concordance, concord_data
from .hdf 			import 	convert_hdf_to_stata