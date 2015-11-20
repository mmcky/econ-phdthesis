"""
Dataset Info
============

Author: Matthew McKay (mamckay@gmail.com)

Central Source of Dataset Information and Directory Structure etc.

"""

import sys
import os

if sys.platform.startswith('win'):
	DATA_DIR = r"D:/work-data/datasets/"
elif sys.platform.startswith('darwin') or sys.platform.startswith('linux'):             
    abs_path = os.path.expanduser("~")
    DATA_DIR = abs_path + "/work-data/datasets/"

#-Source Information-#

SOURCE_DIR = 	{
	"nber" 		: 	DATA_DIR + "36a376e5a01385782112519bddfac85e" + "/",
	"baci96"	: 	DATA_DIR + "e988b6544563675492b59f397a8cb6bb" + "/",
	"wdi" 		: 	DATA_DIR + "70146f20cf40f818e6733d552c6cabb5" + "/",
	"atlas" 	: 	DATA_DIR + "2d48c79173719bd41eb5e192fb4470b6" + "/",
	"penn" 		: 	DATA_DIR + "2c2e8d593f39ee74aeb2c7c17047ea3f" + "/",
	"waziarg"	: 	DATA_DIR + "e93e2009b02d39655f1beb5bcaaf04a8" + "/",
} 

#-Check Environment Settings-#
for source in SOURCE_DIR.keys():
	if not os.path.isdir(SOURCE_DIR[source]):
		raise ValueError("Directory: %s is not found!" % SOURCE_DIR[source])

#-Target Information-#

TARGET_RAW_DIR = {
	"nber" 		: "./output/raw/",
	"baci96" 	: "./output/raw/",
	"wdi" 		: "./output/raw/",
}

TARGET_DATASET_DIR = {
	"nber" 			: "./output/dataset/nber/",
	"baci96" 		: "./output/dataset/baci96/",
	"nberbaci96" 	: "./output/dataset/nberbaci96/",
	"regression"	: "./output/dataset/regression/",
	"atlas" 		: "./output/dataset/atlas/",
	"wdi" 			: "./output/dataset/wdi/",
	"penn"			: "./output/dataset/penn/"
}

RESULTS_DIR = {
	#-General Analysis-#
	"nber" 			: 	"./output/results/nber/",
	"baci96" 		: 	"./output/results/baci96/",
	"nberbaci96" 	: 	"./output/results/nberbaci96/",
	"atlas" 		: 	"./output/results/atlas/",
}

#-Dataset Attributes-#
YEARS = {
	"nber" 		: (1962,2000),
	"baci96" 	: (1998,2012)
}

#-Thesis Chapter Level Results-#
CHAPTER_RESULTS = {
	1 	: "./output/chapter1/",
	2 	: "./output/chapter2/",
	3 	: "./output/chapter3/",
	4 	: "./output/chapter4/",
	5 	: "./output/chapter5/",
	6 	: "./output/chapter6/",
	"A" : "./output/appendixA/",
	"B" : "./output/appendixB/",
	"C" : "./output/appendixC/",
	"D" : "./output/appendixD/",
	"G" : "./output/appendixG/",
}