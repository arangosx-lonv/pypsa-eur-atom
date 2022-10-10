# SPDX-FileCopyrightText: : 2017-2022 The PyPSA-Eur Authors
#
# SPDX-License-Identifier: MIT

# coding: utf-8

# Hacky "import" of set_PROJdir from _helpers.py, which is in another folder. Will find a more elegant solution later.
import os
import sys
if not os.environ.get("PROJ_LIB", False):
    os.environ["PROJ_LIB"] = sys.prefix + "\\Library\\share\\proj"
    print("No default PROJ data directory found, setting to " + os.environ["PROJ_LIB"])

import logging
import yaml
import pypsa
import pandas as pd
import geopandas as gpd

logger = logging.getLogger(__name__)

# Snakemake parameters replicated here
with open('../../../config.yaml') as f:
    config = yaml.safe_load(f)

class filepaths:
    class input:
        network = '../networks/elec_s/elec_s.nc'
        regions_onshore = '../intermediate_files/regions_onshore_elec_s.geojson'
        clustering_shapefile = '_______________.shp' # ENTER SHAPEFILE NAME HERE

    output = '../intermediate_files/tso_busmap.csv'


if __name__ == "__main__":

    # Read in the TSO shapefile
    sf = gpd.read_file(filepaths.input.clustering_shapefile)

    # If the shapefile is ONLY the polygon containing the TSO of interest, define the TSO variable as in the line below
    sf['TSO'] = 'TSO_1'

    # If the shapefile contains multiple TSOs or regions, define the TSO variable based on a variable in the shapefile
    # The specifics of this process will vary from context to context, but the general steps remain the same:
    # 1. Identify the variable with which you can capture the TSO region
    # 2. Define a dictionary to map that variable to a new variable called "TSO" (ideally with relatively short names)
    # 3. Map the values to produce a shapefile where all polygons are identified by their TSO

    # Example: the shapefile had a variable called "LongName" with 14 unique values. We wanted to capture two of them,
    # while combining the remaining 12 under "NG" for National Grid. Define the TSO_mapping dictionary to capture the
    # specific values we want to keep, then map "LongName" and fill in the blanks with "NG".
    TSO_mapping = {"SSE": "SSE",
                   "SPEN (SP Distribution)": "SP"}
    sf['TSO'] = sf.LongName.map(TSO_mapping).fillna("NG")


    # NO USER INPUT REQUIRED BELOW THIS LINE #
    sf_dissolve = sf[['TSO', 'geometry']].to_crs(gpd.read_file(filepaths.input.regions_onshore).crs)

    n = pypsa.Network(filepaths.input.network)

    buses_sf = gpd.GeoDataFrame(n.buses, geometry=gpd.points_from_xy(n.buses.x, n.buses.y)).set_crs(
        gpd.read_file(filepaths.input.regions_onshore).crs)

    buses_tsos = gpd.sjoin(buses_sf, sf_dissolve, how='left')

    # The .fillna setting will fill all other nodes that aren't captured in the shapefile. Don't use "N/A"
    busmap = pd.Series(buses_tsos['index_right'].fillna("TSO_unk")).rename('tso')

    busmap.to_csv(filepaths.output)
