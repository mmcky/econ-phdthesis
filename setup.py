"""
PhD Thesis Setup
================

This script will setup the thesis project and any required folders etc.

"""

import os

FOLDERS = [ 
            #-Output Directories-#
            "output",
            "output/raw",           #Files Should be clearly marked as raw_sources.h5
            "output/dataset/",
            "output/dataset/nber",
            "output/dataset/nber/Y7400/",
            "output/dataset/nber/Y8400/",
            "output/dataset/baci96",
            "output/dataset/baci96/harmonised",
            "output/dataset/baci96/harmonised/Y7400/",
            "output/dataset/baci96/harmonised/Y8400/",
            "output/dataset/nberbaci96",
            "output/dataset/nberbaci96/Y7400/",
            "output/dataset/nberbaci96/Y8400",
            "output/dataset/regression/",
            "output/dataset/atlas/",
            "output/dataset/wdi/",
            "output/dataset/penn/",
            
            #-NBER Results-#
            "output/results/nber",
            "output/results/nber/intertemporal-productcodes/",
            "output/results/nber/intertemporal-productcodes/Y7400/",
            "output/results/nber/intertemporal-productcodes/Y8400/",
            "output/results/nber/intertemporal-productcodes-sitcl4/",
            "output/results/nber/intertemporal-productcodes-sitcl4/raw/",
            "output/results/nber/intertemporal-productcodes-sitcl4/plots/",
            "output/results/nber/intertemporal-productcodes-sitcl3/",
            "output/results/nber/intertemporal-productcodes-sitcl3/raw/",
            "output/results/nber/intertemporal-productcodes-sitcl2/",
            "output/results/nber/intertemporal-productcodes-sitcl2/raw/",
            "output/results/nber/intertemporal-exporters/",
            "output/results/nber/intertemporal-exporters/raw/",
            "output/results/nber/intertemporal-countrycodes/",
            "output/results/nber/intertemporal-countrycodes/raw/",
            #-NBER Tables-#
            "output/results/nber/tables/",
            "output/results/nber/tables/Y7400/",
            "output/results/nber/tables/Y8400/",
            #-NBER Plots-#
            "output/results/nber/plots/",
            "output/results/nber/plots/percent_unofficial_codes/",
            "output/results/nber/plots/percent_world_values/",
            "output/results/nber/plots/percent_world_values/Y7400/",
            "output/results/nber/plots/percent_world_values/Y8400/",

            #-BACI96 Results-#
            "output/results/baci96",
            "output/results/baci96/intertemporal-countrycodes/",
            "output/results/baci96/intertemporal-productcodes/",
            #-BACI96 Tables-#
            "output/results/baci96/tables/",
            #-BACI96 Plots-#
            "output/results/baci96/plots/",
            "output/results/baci96/plots/percent_world_values/",

            #-Combined Dataset Results-#
            "output/results/nberbaci96",
            "output/results/nberbaci96/intertemporal-countrycodes/",
            "output/results/nberbaci96/intertemporal-countrycodes/Y7400/",
            "output/results/nberbaci96/intertemporal-countrycodes/Y8400/",
            "output/results/nberbaci96/intertemporal-productcodes/",
            "output/results/nberbaci96/intertemporal-productcodes/Y7400/",
            "output/results/nberbaci96/intertemporal-productcodes/Y8400/",
            #-NBERBACI Tables-#
            "output/results/nberbaci96/tables/",
            "output/results/nberbaci96/tables/Y7400/",
            "output/results/nberbaci96/tables/Y8400/",
            #-NBERBACI Plots-#
            "output/results/nberbaci96/plots/",
            "output/results/nberbaci96/plots/percent_world_values/",

            #-Atlas of Complexity-#
            "output/results/atlas/",
            "output/results/atlas/intertemporal-productcodes/",
            "output/results/atlas/intertemporal-countrycodes/",
            "output/results/atlas/tables/",
            "output/results/atlas/plots/",
            "output/results/atlas/plots/intertemporal-productcodes-num/",

            #-Chapter and Appendix Working Areas-#
            "output/chapter1/",
            "output/chapter2/",
            "output/chapter3/",
            "output/chapter3/sensativity-analysis/",
            "output/chapter3/plots/",
            "output/chapter4/",
            "output/chapter5/",
            "output/chapter6/",
            "output/appendixA/",
            "output/appendixB/",
            "output/appendixC/",
            "output/appendixD/",
            "output/appendixG/",
            
            #-Log Directory-#
            "log/",

          ]

#-Setup Folders-#

for folder in FOLDERS:
    if not os.path.exists(folder):
        print "[Setup] Creating directory: %s" % folder
        os.makedirs(folder)