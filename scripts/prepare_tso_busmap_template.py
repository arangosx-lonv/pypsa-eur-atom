# SPDX-FileCopyrightText: : 2017-2022 The PyPSA-Eur Authors
#
# SPDX-License-Identifier: MIT

# coding: utf-8
"""
Creates networks clustered to ``{cluster}`` number of zones with aggregated buses, generators and transmission corridors.

Relevant Settings
-----------------

.. code:: yaml

    clustering:
      cluster_network:
      aggregation_strategies:

    focus_weights:

    solving:
        solver:
            name:

    lines:
        length_factor:

.. seealso::
    Documentation of the configuration file ``config.yaml`` at
    :ref:`toplevel_cf`, :ref:`renewable_cf`, :ref:`solving_cf`, :ref:`lines_cf`

Inputs
------

- ``resources/regions_onshore_elec_s{simpl}.geojson``: confer :ref:`simplify`
- ``resources/regions_offshore_elec_s{simpl}.geojson``: confer :ref:`simplify`
- ``resources/busmap_elec_s{simpl}.csv``: confer :ref:`simplify`
- ``networks/elec_s{simpl}.nc``: confer :ref:`simplify`
- ``data/custom_busmap_elec_s{simpl}_{clusters}.csv``: optional input

Outputs
-------

- ``resources/regions_onshore_elec_s{simpl}_{clusters}.geojson``:

    .. image:: ../img/regions_onshore_elec_s_X.png
        :scale: 33 %

- ``resources/regions_offshore_elec_s{simpl}_{clusters}.geojson``:

    .. image:: ../img/regions_offshore_elec_s_X.png
        :scale: 33 %

- ``resources/busmap_elec_s{simpl}_{clusters}.csv``: Mapping of buses from ``networks/elec_s{simpl}.nc`` to ``networks/elec_s{simpl}_{clusters}.nc``;
- ``resources/linemap_elec_s{simpl}_{clusters}.csv``: Mapping of lines from ``networks/elec_s{simpl}.nc`` to ``networks/elec_s{simpl}_{clusters}.nc``;
- ``networks/elec_s{simpl}_{clusters}.nc``:

    .. image:: ../img/elec_s_X.png
        :scale: 40  %

Description
-----------

.. note::

    **Why is clustering used both in** ``simplify_network`` **and** ``cluster_network`` **?**

        Consider for example a network ``networks/elec_s100_50.nc`` in which
        ``simplify_network`` clusters the network to 100 buses and in a second
        step ``cluster_network``` reduces it down to 50 buses.

        In preliminary tests, it turns out, that the principal effect of
        changing spatial resolution is actually only partially due to the
        transmission network. It is more important to differentiate between
        wind generators with higher capacity factors from those with lower
        capacity factors, i.e. to have a higher spatial resolution in the
        renewable generation than in the number of buses.

        The two-step clustering allows to study this effect by looking at
        networks like ``networks/elec_s100_50m.nc``. Note the additional
        ``m`` in the ``{cluster}`` wildcard. So in the example network
        there are still up to 100 different wind generators.

        In combination these two features allow you to study the spatial
        resolution of the transmission network separately from the
        spatial resolution of renewable generators.

    **Is it possible to run the model without the** ``simplify_network`` **rule?**

        No, the network clustering methods in the PyPSA module
        `pypsa.networkclustering <https://github.com/PyPSA/PyPSA/blob/master/pypsa/networkclustering.py>`_
        do not work reliably with multiple voltage levels and transformers.

.. tip::
    The rule :mod:`cluster_all_networks` runs
    for all ``scenario`` s in the configuration file
    the rule :mod:`cluster_network`.

Exemplary unsolved network clustered to 512 nodes:

.. image:: ../img/elec_s_512.png
    :scale: 40  %
    :align: center

Exemplary unsolved network clustered to 256 nodes:

.. image:: ../img/elec_s_256.png
    :scale: 40  %
    :align: center

Exemplary unsolved network clustered to 128 nodes:

.. image:: ../img/elec_s_128.png
    :scale: 40  %
    :align: center

Exemplary unsolved network clustered to 37 nodes:

.. image:: ../img/elec_s_37.png
    :scale: 40  %
    :align: center

"""
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
    n = pypsa.Network(filepaths.input.network)

    sf = gpd.read_file(filepaths.input.clustering_shapefile)

    # Optional: define a custom mapping to clean/shorten TSO names based on another field (here called "LongName").
    TSO_mapping = {"SSE": "SSE",
                   "SPEN (SP Distribution)": "SP"}
    sf['TSO'] = sf.LongName.map(TSO_mapping).fillna("NG")

    sf_dissolve = sf[['TSO', 'geometry']].dissolve(by='TSO').to_crs(gpd.read_file(filepaths.input.regions_onshore).crs)

    buses_sf = gpd.GeoDataFrame(n.buses, geometry=gpd.points_from_xy(n.buses.x, n.buses.y)).set_crs(
        gpd.read_file(filepaths.input.regions_onshore).crs)

    buses_tsos = gpd.sjoin(buses_sf, sf_dissolve, how='left')

    # The .fillna setting will fill all other nodes that aren't captured in the shapefile. Don't use "N/A"
    busmap = pd.Series(buses_tsos['index_right'].fillna("TSO_unk")).rename('tso')

    busmap.to_csv(filepaths.output)
