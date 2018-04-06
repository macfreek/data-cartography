# Data Cartography 2018 (draft)

This repository contains sources and scripts to generate a map of data-intensive research in Europe.

The main output are three map layers:
* The underlying network, using the GÉANT research and education network as source;
* Scientific data centres, listing public data centers with a computer in the 'top500' list;
* Scientific instruments, listing single-site ESFRI projects (European Strategy Forum on Research Infrastructures).

The sources are available locally. Where possible, the sources of the data are accounted for in the file `data_sources.txt`.

The script `generate_map.py` uses the available sources to generate these 3 map layers in geojson format.
The output files are stored in the results directory, and are meant to be used with uMap software.

The current result is visible at http://umap.openstreetmap.fr/en/map/data-cartography-2018-draft_209783.

This work is carried