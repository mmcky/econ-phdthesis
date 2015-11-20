"""
NBERFeenstraWTF Meta SubPackage

Notes
-----
[1] Should these be promoted to NBERFeenstraWTF Level?
[2] Should this be csv data - with an object wrapper?

"""

# ---------------- #
# - List Objects - #
# ---------------- #

from .countrynames 	import countries
from .exporters 	import exporters
from .importers 	import importers

# ----------------------- #
# - Concordance Objects - #
# ----------------------- #

from .countryname_to_iso3c 	import 	countryname_to_iso3c
from .countryname_to_iso3n 	import 	countryname_to_iso3n
from .iso3c_to_countryname 	import 	iso3c_to_countryname

# ------------------ #
# - Recode Objects - #
# ------------------ #


# --- > DEPRECATED <--- #
# from .recodes import intertemporal
# iso3c_recodes_for_1962_2000 = intertemporal.iso3c_for_1962_2000
# incomplete_iso3c_for_1962_2000 = intertemporal.incomplete_iso3c_for_1962_2000
# --- > END DEPRECATED <--- #

# ------------------------- #
# - Intertemporal Recodes - #
# ------------------------- #

from .intertemporal import IntertemporalCountries, IntertemporalProducts
iso3c_recodes_for_1962_2000 = IntertemporalCountries.iso3c_for_1962_2000
incomplete_iso3c_for_1962_2000 = IntertemporalCountries.incomplete_iso3c_for_1962_2000

# -------------------- #
# - Deletion Objects - #
# -------------------- #
from .deletions import sitc4_deletions, sitc3_deletions

# --------------- #
# - Fix Objects - #
# --------------- #

from .fixes import fix_exporter_to_iso3n, fix_ecode_to_iso3n, fix_exporter_to_iso3c, fix_importer_to_iso3n, fix_icode_to_iso3n, fix_importer_to_iso3c