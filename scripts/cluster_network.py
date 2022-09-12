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

    country_weights:

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
from _helpers import set_PROJdir
set_PROJdir()

import logging
from _helpers import configure_logging, update_p_nom_max, get_aggregation_strategies

import yaml

import pypsa

import pandas as pd
import numpy as np
import geopandas as gpd
import pyomo.environ as po
import matplotlib.pyplot as plt
import seaborn as sns

from functools import reduce

from pypsa.networkclustering import (busmap_by_kmeans, busmap_by_hac,
                                     busmap_by_greedy_modularity, get_clustering_from_busmap)

import warnings
warnings.filterwarnings(action='ignore', category=UserWarning)

from add_electricity import load_costs

idx = pd.IndexSlice

logger = logging.getLogger(__name__)

# Snakemake parameters replicated here
with open('../config.yaml') as f:
    config = yaml.safe_load(f)

class filepaths:
    class input:
        network = '../networks/elec_s.nc'
        regions_onshore = "../resources/regions_onshore_elec_s.geojson"
        regions_offshore = "../resources/regions_offshore_elec_s.geojson"
        busmap = '../resources/busmap_elec_s.csv'
        tso_busmap = ('../resources/tso_busmap.csv'
                      if config["enable"].get("tso_busmap", False) else [])
        custom_busmap = lambda w: ('../data/custom_busmap_elec_s_' + w + '.csv'
                                   if config["enable"].get("custom_busmap", False) else [])
        tech_costs = "../resources/costs.csv"

    class output:
        network = lambda w: '../networks/elec_s_' + w + '.nc'
        regions_onshore = lambda w: '../resources/regions_onshore_elec_s_' + w + '.geojson'
        regions_offshore = lambda w: '../resources/regions_offshore_elec_s_' + w + '.geojson'
        busmap = lambda w: '../resources/busmap_elec_s_' + w + '.csv'
        linemap = lambda w: '../resources/linemap_elec_s_' + w + '.csv'


def normed(x): return (x/x.sum()).fillna(0.)


def weighting_for_country(n, x):
    conv_carriers = {'OCGT','CCGT','PHS','hydro'}
    gen = (n
           .generators.loc[n.generators.carrier.isin(conv_carriers)]
           .groupby('bus').p_nom.sum()
           .reindex(n.buses.index, fill_value=0.) +
           n
           .storage_units.loc[n.storage_units.carrier.isin(conv_carriers)]
           .groupby('bus').p_nom.sum()
           .reindex(n.buses.index, fill_value=0.))
    load = n.loads_t.p_set.mean().groupby(n.loads.bus).sum()

    b_i = x.index
    g = normed(gen.reindex(b_i, fill_value=0))
    l = normed(load.reindex(b_i, fill_value=0))

    w = g + l
    return (w * (100. / w.max())).clip(lower=1.).astype(int)


def get_feature_for_hac(n, buses_i=None, feature=None):

    if buses_i is None:
        buses_i = n.buses.index

    if feature is None:
        feature = "solar+onwind-time"

    carriers = feature.split('-')[0].split('+')
    if "offwind" in carriers:
        carriers.remove("offwind")
        carriers = np.append(carriers, n.generators.carrier.filter(like='offwind').unique())

    if feature.split('-')[1] == 'cap':
        feature_data = pd.DataFrame(index=buses_i, columns=carriers)
        for carrier in carriers:
            gen_i = n.generators.query("carrier == @carrier").index
            attach = n.generators_t.p_max_pu[gen_i].mean().rename(index = n.generators.loc[gen_i].bus)
            feature_data[carrier] = attach

    if feature.split('-')[1] == 'time':
        feature_data = pd.DataFrame(columns=buses_i)
        for carrier in carriers:
            gen_i = n.generators.query("carrier == @carrier").index
            attach = n.generators_t.p_max_pu[gen_i].rename(columns = n.generators.loc[gen_i].bus)
            feature_data = pd.concat([feature_data, attach], axis=0)[buses_i]

        feature_data = feature_data.T
        # timestamp raises error in sklearn >= v1.2:
        feature_data.columns = feature_data.columns.astype(str)

    feature_data = feature_data.fillna(0)

    return feature_data


def distribute_clusters(n, n_clusters, country_weights=None, tso_weights = None, solver_name="cbc"):

    assert not (country_weights is None and tso_weights is not None), "TSO weights provided but no country weights provided. TSO weights can only be used in combination with country weights. Check config file."

    if (country_weights is not None) and (tso_weights is None):
        N = n.buses.groupby(['country', 'sub_network']).size()

        assert n_clusters >= len(N) and n_clusters <= N.sum(), \
            f"Number of clusters must be {len(N)} <= n_clusters <= {N.sum()} for this selection of countries."

        L = (n.loads_t.p_set.mean()
             .groupby(n.loads.bus).sum()
             .groupby([n.buses.country, n.buses.sub_network]).sum()
             .pipe(normed))

        total_country = sum(list(country_weights.values()))

        assert total_country <= 1.0, "The sum of focus weights must be less than or equal to 1."

        for country, weight in country_weights.items():
            L[country] = weight * L[country].transform(lambda x: normed(x))

        remainder = [c not in country_weights.keys() for c in L.index.get_level_values('country')]
        L[remainder] = L.loc[remainder].pipe(normed) * (1 - total_country)

        logger.warning('Using custom country weights for determining number of clusters.'
                       'Nodes distributed across sub-networks within countries according to relative load.')

    elif (country_weights is not None) and (tso_weights is not None):
        tso_busmap = pd.read_csv(filepaths.input.tso_busmap).set_index('Bus').squeeze()
        tso_busmap.index = tso_busmap.index.astype(str)

        n.buses = n.buses.merge(tso_busmap, left_index=True, right_index=True)

        N = n.buses.groupby(['country', 'tso']).size()

        assert n_clusters >= len(N) and n_clusters <= N.sum(), \
            f"Number of clusters must be {len(N)} <= n_clusters <= {N.sum()} for this selection of countries and TSOs."

        L = (n.loads_t.p_set.mean()
             .groupby(n.loads.bus).sum()
             .groupby([n.buses.country, n.buses.tso]).sum()
             .pipe(normed))

        total_country = sum(list(country_weights.values()))
        assert total_country <= 1.0, "The sum of country weights must be less than or equal to 1."

        for country in tso_weights:
            total_tso = sum(list(tso_weights[country].values()))
            assert total_tso <= 1.0, ("The sum of TSO weights for " + country + " must be less than or equal to 1.")

        for country, c_weight in country_weights.items():
            if tso_weights.get(country, False):
                total_tso = sum(list(tso_weights[country].values()))

                for tso, t_weight in tso_weights[country].items():
                    L[country, tso] = t_weight * c_weight

                remainder = [t not in tso_weights[country].keys() for t in L[country].index.get_level_values('tso')]
                L[country][remainder] = L[country].loc[remainder].pipe(normed) * (1 - total_tso)
            else:
                L[country] = c_weight * L[country].transform(lambda x: normed(x))

        remainder = [c not in country_weights.keys() for c in L.index.get_level_values('country')]
        L[remainder] = L.loc[remainder].pipe(normed) * (1 - total_country)

        logger.warning('Using custom country weights and TSO weights for determining distribution of clusters.')

    else:
        N = n.buses.groupby(['country', 'sub_network']).size()

        assert n_clusters >= len(N) and n_clusters <= N.sum(), \
            f"Number of clusters must be {len(N)} <= n_clusters <= {N.sum()} for this selection of countries."

        L = (n.loads_t.p_set.mean()
             .groupby(n.loads.bus).sum()
             .groupby([n.buses.country, n.buses.sub_network]).sum()
             .pipe(normed))
    print(L)
    assert np.isclose(L.sum(), 1.0, rtol=1e-3), f"Country weights L must sum up to 1.0 when distributing clusters. Is {L.sum()}."

    m = po.ConcreteModel()
    def n_bounds(model, *n_id):
        return (1, N[n_id])
    m.n = po.Var(list(L.index), bounds=n_bounds, domain=po.Integers)
    m.tot = po.Constraint(expr=(po.summation(m.n) == n_clusters))
    m.objective = po.Objective(expr=sum((m.n[i] - L.loc[i]*n_clusters)**2 for i in L.index),
                               sense=po.minimize)

    opt = po.SolverFactory(solver_name)
    if not opt.has_capability('quadratic_objective'):
        logger.warning(f'The configured solver `{solver_name}` does not support quadratic objectives. Falling back to `ipopt`.')
        opt = po.SolverFactory('ipopt')

    results = opt.solve(m)
    assert results['Solver'][0]['Status'] == 'ok', f"Solver returned non-optimally: {results}"

    return pd.Series(m.n.get_values(), index=L.index).round().astype(int)


def busmap_for_n_clusters(n, n_clusters, solver_name, country_weights=None, tso_weights = None,
                          algorithm="kmeans", feature=None, **algorithm_kwds):
    if algorithm == "kmeans":
        algorithm_kwds.setdefault('n_init', 1000)
        algorithm_kwds.setdefault('max_iter', 30000)
        algorithm_kwds.setdefault('tol', 1e-6)
        algorithm_kwds.setdefault('random_state', 0)

    def fix_country_assignment_for_hac(n):
        from scipy.sparse import csgraph

        # overwrite country of nodes that are disconnected from their country-topology
        for country in n.buses.country.unique():
            m = n[n.buses.country == country].copy()

            _, labels = csgraph.connected_components(m.adjacency_matrix(), directed=False)

            component = pd.Series(labels, index=m.buses.index)
            component_sizes = component.value_counts()

            if len(component_sizes)>1:
                disconnected_bus = component[component==component_sizes.index[-1]].index[0]

                neighbor_bus = (
                    n.lines.query("bus0 == @disconnected_bus or bus1 == @disconnected_bus")
                    .iloc[0][['bus0', 'bus1']]
                )
                new_country = list(set(n.buses.loc[neighbor_bus].country)-set([country]))[0]

                logger.info(
                    f"overwriting country `{country}` of bus `{disconnected_bus}` "
                    f"to new country `{new_country}`, because it is disconnected "
                    "from its inital inter-country transmission grid."
                )
                n.buses.at[disconnected_bus, "country"] = new_country
        return n

    if algorithm == "hac":
        feature = get_feature_for_hac(n, buses_i=n.buses.index, feature=feature)
        n = fix_country_assignment_for_hac(n)

    if (algorithm != "hac") and (feature is not None):
        logger.warning(f"Keyword argument feature is only valid for algorithm `hac`. "
                       f"Given feature `{feature}` will be ignored.")

    n.determine_network_topology()

    cluster_distribution = distribute_clusters(n, n_clusters, country_weights = country_weights,
                                               tso_weights = tso_weights, solver_name=solver_name)

    def busmap_for_country(x):
        prefix = x.name[0] + x.name[1] + ' '
        logger.debug(f"Determining busmap for country {prefix[:-1]}")
        if len(x) == 1:
            return pd.Series(prefix + '0', index=x.index)
        weight = weighting_for_country(n, x)

        if algorithm == "kmeans":
            return prefix + busmap_by_kmeans(n, weight, cluster_distribution[x.name], buses_i=x.index, **algorithm_kwds)
        elif algorithm == "hac":
            return prefix + busmap_by_hac(n, cluster_distribution[x.name], buses_i=x.index, feature=feature.loc[x.index])
        elif algorithm == "modularity":
            return prefix + busmap_by_greedy_modularity(n, cluster_distribution[x.name], buses_i=x.index)
        else:
            raise ValueError(f"`algorithm` must be one of 'kmeans', 'hac', or 'modularity'. Is {algorithm}.")

    if tso_weights is not None:
        result = (n.buses.groupby(['country', 'tso'], group_keys=False)
            .apply(busmap_for_country).squeeze().rename('busmap'))
    else:
        result = (n.buses.groupby(['country', 'sub_network'], group_keys=False)
            .apply(busmap_for_country).squeeze().rename('busmap'))
    return result


def clustering_for_n_clusters(n, n_clusters, custom_busmap=False, aggregate_carriers=None,
                              line_length_factor=1.25, aggregation_strategies=dict(), solver_name="cbc",
                              algorithm="hac", feature=None, extended_link_costs=0,
                              country_weights=None, tso_weights = None):

    bus_strategies, generator_strategies = get_aggregation_strategies(aggregation_strategies)

    if not isinstance(custom_busmap, pd.Series):
        busmap = busmap_for_n_clusters(n, n_clusters, solver_name, country_weights, tso_weights, algorithm, feature)
    else:
        busmap = custom_busmap

    clustering = get_clustering_from_busmap(
        n, busmap,
        bus_strategies=bus_strategies,
        aggregate_generators_weighted=True,
        aggregate_generators_carriers=aggregate_carriers,
        aggregate_one_ports=["Load", "StorageUnit"],
        line_length_factor=line_length_factor,
        generator_strategies=generator_strategies,
        scale_link_capital_costs=False)

    if not n.links.empty:
        nc = clustering.network
        nc.links['underwater_fraction'] = (n.links.eval('underwater_fraction * length')
                                        .div(nc.links.length).dropna())
        nc.links['capital_cost'] = (nc.links['capital_cost']
                                    .add((nc.links.length - n.links.length)
                                        .clip(lower=0).mul(extended_link_costs),
                                        fill_value=0))

    return clustering


def cluster_regions(busmaps, input=None, output=None, output_suffix=str()):

    busmap = reduce(lambda x, y: x.map(y), busmaps[1:], busmaps[0])

    for which in ('regions_onshore', 'regions_offshore'):
        regions = gpd.read_file(getattr(input, which))
        regions = regions.reindex(columns=["name", "geometry"]).set_index('name')
        regions_c = regions.dissolve(busmap)
        regions_c.index.name = 'name'
        regions_c = regions_c.reset_index()
        regions_c.to_file(getattr(output, which)(output_suffix))


def plot_busmap_for_n_clusters(n, n_clusters, fn=None):
    busmap = busmap_for_n_clusters(n, n_clusters)
    cs = busmap.unique()
    cr = sns.color_palette("hls", len(cs))
    n.plot(bus_colors=busmap.map(dict(zip(cs, cr))))
    if fn is not None:
        plt.savefig(fn, bbox_inches='tight')
    del cs, cr


if __name__ == "__main__":
    n = pypsa.Network(filepaths.input.network)

    country_weights = config.get('country_weights', None)
    tso_weights = config.get('tso_weights', None)

    renewable_carriers = pd.Index([tech
                                   for tech in n.generators.carrier.unique()
                                   if tech in config['renewable']])

    for nclust in config['scenario']['clusters']:
        if str(nclust).endswith('m'):
            n_clusters = int(nclust[:-1])
            aggregate_carriers = config["electricity"].get("conventional_carriers")
        elif nclust == 'all':
            n_clusters = len(n.buses)
            aggregate_carriers = None  # All
        else:
            n_clusters = int(nclust)
            aggregate_carriers = None  # All

        if n_clusters == len(n.buses):
            # Fast-path if no clustering is necessary
            busmap = n.buses.index.to_series()
            linemap = n.lines.index.to_series()
            clustering = pypsa.networkclustering.Clustering(n, busmap, linemap, linemap, pd.Series(dtype='O'))
        else:
            line_length_factor = config['lines']['length_factor']
            Nyears = n.snapshot_weightings.objective.sum()/8760

            hvac_overhead_cost = (load_costs(filepaths.input.tech_costs, config['costs'], config['electricity'], Nyears)
                                  .at['HVAC overhead', 'capital_cost'])

            def consense(x):
                v = x.iat[0]
                assert ((x == v).all() or x.isnull().all()), (
                    "The `potential` configuration option must agree for all renewable carriers, for now!"
                )
                return v
            aggregation_strategies = config["clustering"].get("aggregation_strategies", {})
            # translate str entries of aggregation_strategies to pd.Series functions:
            aggregation_strategies = {
                p: {k: getattr(pd.Series, v) for k,v in aggregation_strategies[p].items()}
                for p in aggregation_strategies.keys()
            }

            custom_busmap = config["enable"].get("custom_busmap", False)
            if custom_busmap:
                custom_busmap = pd.read_csv(filepaths.input.custom_busmap(n_clusters), index_col=0, squeeze=True)
                custom_busmap.index = custom_busmap.index.astype(str)
                logger.info(f"Imported custom busmap from {filepaths.input.custom_busmap(n_clusters)}")

            cluster_config = config.get('clustering', {}).get('cluster_network', {})
            clustering = clustering_for_n_clusters(n, n_clusters, custom_busmap, aggregate_carriers,
                                                   line_length_factor, aggregation_strategies,
                                                   config['solving']['solver']['name'],
                                                   cluster_config.get("algorithm", "hac"),
                                                   cluster_config.get("feature", "solar+onwind-time"),
                                                   hvac_overhead_cost, country_weights, tso_weights)

        update_p_nom_max(clustering.network)

        clustering.network.meta = config
        clustering.network.export_to_netcdf(filepaths.output.network(str(nclust)))

        # also available: linemap_positive, linemap_negative
        getattr(clustering, 'busmap').to_csv(filepaths.output.busmap(str(nclust)))
        getattr(clustering, 'linemap').to_csv(filepaths.output.linemap(str(nclust)))

        cluster_regions((clustering.busmap,), filepaths.input, filepaths.output, str(nclust))
