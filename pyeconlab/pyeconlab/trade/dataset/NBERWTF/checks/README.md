NBERFeenstraWTF Check Files
===========================

Check Files are items like concordances that require manually checking to ensure accuracy.
In addition, they also document any `special_case` changes that may be required.

Files
-----

[1] check_exporters.xlsx

	Description: Manual Check File of Exporters that Document any changes required for concordances as 'special cases' 	{1}

[2] check_importers.xlsx 

	Description: Manual Check File of Importers that Document any changes required for concordances as 'special cases' 	{1}

Construction Recipe
-------------------
{1} 	a = NBERFeenstraWTFConstructor(source_dir=SOURCE_DATA_DIR)
		a.set_dataset(a.raw_data)
		a.add_iso3c()
		a.add_isocountrynames()
		importers = a.dataset[['importer', 'iiso3c', 'iiso3n', 'icountryname']].drop_duplicates()
		exporters = a.dataset[['exporter', 'eiso3c', 'eiso3n', 'ecountryname']].drop_duplicates()
