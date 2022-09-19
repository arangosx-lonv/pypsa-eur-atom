# SPDX-FileCopyrightText: : 2017-2022 The PyPSA-Eur Authors
#
# SPDX-License-Identifier: MIT

# coding: utf-8
"""
Lifts electrical transmission network to a single 380 kV voltage layer,
removes dead-ends of the network,
and reduces multi-hop HVDC connections to a single link.

Relevant Settings
-----------------

.. code:: yaml

    clustering:
      simplify_network:
      cluster_network:
      aggregation_strategies:

    costs:
        year:
        version:
        fill_values:
        marginal_cost:
        capital_cost:

    electricity:
        max_hours:

    lines:
        length_factor:

    links:
        p_max_pu:

    solving:
        solver:
            name:

.. seealso::
    Documentation of the configuration file ``config.yaml`` at
    :ref:`costs_cf`, :ref:`electricity_cf`, :ref:`renewable_cf`,
    :ref:`lines_cf`, :ref:`links_cf`, :ref:`solving_cf`

Inputs
------

- ``resources/costs.csv``: The database of cost assumptions for all included technologies for specific years from various sources; e.g. discount rate, lifetime, investment (CAPEX), fixed operation and maintenance (FOM), variable operation and maintenance (VOM), fuel costs, efficiency, carbon-dioxide intensity.
- ``resources/regions_onshore.geojson``: confer :ref:`busregions`
- ``resources/regions_offshore.geojson``: confer :ref:`busregions`
- ``networks/elec.nc``: confer :ref:`electricity`

Outputs
-------

- ``resources/regions_onshore_elec_s.geojson``:

    .. image:: ../img/regions_onshore_elec_s.png
            :scale: 33 %

- ``resources/regions_offshore_elec_s.geojson``:

    .. image:: ../img/regions_offshore_elec_s  .png
            :scale: 33 %

- ``resources/busmap_elec_s.csv``: Mapping of buses from ``networks/elec.nc`` to ``networks/elec_s.nc``;
- ``networks/elec_s.nc``:

    .. image:: ../img/elec_s.png
        :scale: 33 %

Description
-----------

The rule :mod:`simplify_network` does three things:

1. Create an equivalent transmission network in which all voltage levels are mapped to the 380 kV level by the function ``simplify_network(...)``.

2. DC only sub-networks that are connected at only two buses to the AC network are reduced to a single representative link in the function ``simplify_links(...)``. The components attached to buses in between are moved to the nearest endpoint. The grid connection cost of offshore wind generators are added to the captial costs of the generator.

3. Stub lines and links, i.e. dead-ends of the network, are sequentially removed from the network in the function ``remove_stubs(...)``. Components are moved along.

"""
from _helpers import set_PROJdir, update_p_nom_max, get_aggregation_strategies
set_PROJdir()

import logging
from add_electricity import load_costs
import yaml
import pandas as pd
import geopandas as gpd
import numpy as np
import scipy as sp
from scipy.sparse.csgraph import connected_components, dijkstra
from functools import reduce
import pypsa
from pypsa.io import import_components_from_dataframe, import_series_from_dataframe
from pypsa.networkclustering import busmap_by_stubs, aggregategenerators, aggregateoneport, get_clustering_from_busmap

logger = logging.getLogger(__name__)

# Snakemake parameters replicated here
with open('../config.yaml') as f:
    config = yaml.safe_load(f)

class filepaths:
    class input:
        network = '../models/' + config['project_folder'] + '/networks/elec.nc'
        tech_costs = '../data/costs.csv'
        regions_onshore = '../models/' + config['project_folder'] + '/intermediate_files/regions_onshore.geojson'
        regions_offshore = '../models/' + config['project_folder'] + '/intermediate_files/regions_offshore.geojson'

    class output:
        network = '../models/' + config['project_folder'] + '/networks/elec_s.nc'
        regions_onshore = '../models/' + config['project_folder'] + '/intermediate_files/regions_onshore_elec_s.geojson'
        regions_offshore = '../models/' + config['project_folder'] + '/intermediate_files/regions_offshore_elec_s.geojson'
        busmap = '../models/' + config['project_folder'] + '/intermediate_files/busmap_elec_s.csv'
        connection_costs = '../models/' + config['project_folder'] + '/intermediate_files/connection_costs_s.csv'


def simplify_network_to_380(n):
    ## All goes to v_nom == 380
    logger.info("Mapping all network lines onto a single 380kV layer")

    n.buses['v_nom'] = 380.

    linetype_380, = n.lines.loc[n.lines.v_nom == 380., 'type'].unique()
    lines_v_nom_b = n.lines.v_nom != 380.
    n.lines.loc[lines_v_nom_b, 'num_parallel'] *= (n.lines.loc[lines_v_nom_b, 'v_nom'] / 380.)**2
    n.lines.loc[lines_v_nom_b, 'v_nom'] = 380.
    n.lines.loc[lines_v_nom_b, 'type'] = linetype_380
    n.lines.loc[lines_v_nom_b, 's_nom'] = (
        np.sqrt(3) * n.lines['type'].map(n.line_types.i_nom) *
        n.lines.bus0.map(n.buses.v_nom) * n.lines.num_parallel
    )

    # Replace transformers by lines
    trafo_map = pd.Series(n.transformers.bus1.values, index=n.transformers.bus0.values)
    trafo_map = trafo_map[~trafo_map.index.duplicated(keep='first')]
    several_trafo_b = trafo_map.isin(trafo_map.index)
    trafo_map.loc[several_trafo_b] = trafo_map.loc[several_trafo_b].map(trafo_map)
    missing_buses_i = n.buses.index.difference(trafo_map.index)
    missing = pd.Series(missing_buses_i, missing_buses_i)
    trafo_map = pd.concat([trafo_map, missing])

    for c in n.one_port_components|n.branch_components:
        df = n.df(c)
        for col in df.columns:
            if col.startswith('bus'):
                df[col] = df[col].map(trafo_map)

    n.mremove("Transformer", n.transformers.index)
    n.mremove("Bus", n.buses.index.difference(trafo_map))

    return n, trafo_map


def _prepare_connection_costs_per_link(n, costs):
    if n.links.empty: return {}

    connection_costs_per_link = {}

    for tech in config['renewable']:
        if tech.startswith('offwind'):
            connection_costs_per_link[tech] = (
                n.links.length * config['lines']['length_factor'] *
                (n.links.underwater_fraction * costs.at[tech + '-connection-submarine', 'capital_cost'] +
                 (1. - n.links.underwater_fraction) * costs.at[tech + '-connection-underground', 'capital_cost'])
            )

    return connection_costs_per_link


def _compute_connection_costs_to_bus(n, busmap, costs, connection_costs_per_link=None, buses=None):
    if connection_costs_per_link is None:
        connection_costs_per_link = _prepare_connection_costs_per_link(n, costs)

    if buses is None:
        buses = busmap.index[busmap.index != busmap.values]

    connection_costs_to_bus = pd.DataFrame(index=buses)

    for tech in connection_costs_per_link:
        adj = n.adjacency_matrix(weights=pd.concat(dict(Link=connection_costs_per_link[tech].reindex(n.links.index),
                                                        Line=pd.Series(0., n.lines.index))))

        costs_between_buses = dijkstra(adj, directed=False, indices=n.buses.index.get_indexer(buses))
        connection_costs_to_bus[tech] = costs_between_buses[np.arange(len(buses)),
                                                            n.buses.index.get_indexer(busmap.loc[buses])]

    return connection_costs_to_bus


def _adjust_capital_costs_using_connection_costs(n, connection_costs_to_bus):
    connection_costs = {}
    for tech in connection_costs_to_bus:
        tech_b = n.generators.carrier == tech
        costs = n.generators.loc[tech_b, "bus"].map(connection_costs_to_bus[tech]).loc[lambda s: s>0]
        if not costs.empty:
            n.generators.loc[costs.index, "capital_cost"] += costs
            logger.info("Displacing {} generator(s) and adding connection costs to capital_costs: {} "
                        .format(tech, ", ".join("{:.0f} Eur/MW/a for `{}`".format(d, b) for b, d in costs.iteritems())))
            connection_costs[tech] = costs
    pd.DataFrame(connection_costs).to_csv(filepaths.output.connection_costs)
            

def _aggregate_and_move_components(n, busmap, connection_costs_to_bus, aggregation_strategies=dict(),
                                   aggregate_one_ports={"Load", "StorageUnit"}):

    def replace_components(n, c, df, pnl):
        n.mremove(c, n.df(c).index)

        import_components_from_dataframe(n, df, c)
        for attr, df in pnl.items():
            if not df.empty:
                import_series_from_dataframe(n, df, c, attr)

    _adjust_capital_costs_using_connection_costs(n, connection_costs_to_bus)

    _, generator_strategies = get_aggregation_strategies(aggregation_strategies)

    generators, generators_pnl = aggregategenerators(n, busmap, custom_strategies = generator_strategies)

    replace_components(n, "Generator", generators, generators_pnl)

    for one_port in aggregate_one_ports:
        df, pnl = aggregateoneport(n, busmap, component=one_port)
        replace_components(n, one_port, df, pnl)

    buses_to_del = n.buses.index.difference(busmap)
    n.mremove("Bus", buses_to_del)
    for c in n.branch_components:
        df = n.df(c)
        n.mremove(c, df.index[df.bus0.isin(buses_to_del) | df.bus1.isin(buses_to_del)])


def simplify_links(n, costs, aggregation_strategies=dict()):
    ## Complex multi-node links are folded into end-points
    logger.info("Simplifying connected link components")

    if n.links.empty:
        return n, n.buses.index.to_series()

    # Determine connected link components, ignore all links but DC
    adjacency_matrix = n.adjacency_matrix(branch_components=['Link'],
                                          weights=dict(Link=(n.links.carrier == 'DC').astype(float)))

    _, labels = connected_components(adjacency_matrix, directed=False)
    labels = pd.Series(labels, n.buses.index)

    G = n.graph()

    def split_links(nodes):
        nodes = frozenset(nodes)

        seen = set()
        supernodes = {m for m in nodes
                      if len(G.adj[m]) > 2 or (set(G.adj[m]) - nodes)}

        for u in supernodes:
            for m, ls in G.adj[u].items():
                if m not in nodes or m in seen: continue

                buses = [u, m]
                links = [list(ls)] #[name for name in ls]]

                while m not in (supernodes | seen):
                    seen.add(m)
                    for m2, ls in G.adj[m].items():
                        if m2 in seen or m2 == u: continue
                        buses.append(m2)
                        links.append(list(ls)) # [name for name in ls])
                        break
                    else:
                        # stub
                        break
                    m = m2
                if m != u:
                    yield pd.Index((u, m)), buses, links
            seen.add(u)

    busmap = n.buses.index.to_series()

    connection_costs_per_link = _prepare_connection_costs_per_link(n, costs)
    connection_costs_to_bus = pd.DataFrame(0., index=n.buses.index, columns=list(connection_costs_per_link))

    for lbl in labels.value_counts().loc[lambda s: s > 2].index:

        for b, buses, links in split_links(labels.index[labels == lbl]):
            if len(buses) <= 2: continue

            logger.debug('nodes = {}'.format(labels.index[labels == lbl]))
            logger.debug('b = {}\nbuses = {}\nlinks = {}'.format(b, buses, links))

            m = sp.spatial.distance_matrix(n.buses.loc[b, ['x', 'y']],
                                           n.buses.loc[buses[1:-1], ['x', 'y']])
            busmap.loc[buses] = b[np.r_[0, m.argmin(axis=0), 1]]
            connection_costs_to_bus.loc[buses] += _compute_connection_costs_to_bus(n, busmap, costs, connection_costs_per_link, buses)

            all_links = [i for _, i in sum(links, [])]

            p_max_pu = config['links'].get('p_max_pu', 1.)
            lengths = n.links.loc[all_links, 'length']
            name = lengths.idxmax() + '+{}'.format(len(links) - 1)
            params = dict(
                carrier='DC',
                bus0=b[0], bus1=b[1],
                length=sum(n.links.loc[[i for _, i in l], 'length'].mean() for l in links),
                p_nom=min(n.links.loc[[i for _, i in l], 'p_nom'].sum() for l in links),
                underwater_fraction=sum(lengths/lengths.sum() * n.links.loc[all_links, 'underwater_fraction']),
                p_max_pu=p_max_pu,
                p_min_pu=-p_max_pu,
                underground=False,
                under_construction=False
            )

            logger.info("Joining the links {} connecting the buses {} to simple link {}".format(", ".join(all_links), ", ".join(buses), name))

            n.mremove("Link", all_links)

            static_attrs = n.components["Link"]["attrs"].loc[lambda df: df.static]
            for attr, default in static_attrs.default.iteritems(): params.setdefault(attr, default)
            n.links.loc[name] = pd.Series(params)

            # n.add("Link", **params)

    logger.debug("Collecting all components using the busmap")

    _aggregate_and_move_components(n, busmap, connection_costs_to_bus, aggregation_strategies)
    return n, busmap

def remove_stubs(n, costs, aggregation_strategies=dict()):
    logger.info("Removing stubs")

    busmap = busmap_by_stubs(n) #  ['country'])

    connection_costs_to_bus = _compute_connection_costs_to_bus(n, busmap, costs)

    _aggregate_and_move_components(n, busmap, connection_costs_to_bus, aggregation_strategies)

    return n, busmap

def simplified_regions(busmaps, input=None, output=None):

    busmap = reduce(lambda x, y: x.map(y), busmaps[1:], busmaps[0])

    for which in ('regions_onshore', 'regions_offshore'):
        regions = gpd.read_file(getattr(input, which))
        regions = regions.reindex(columns=["name", "geometry"]).set_index('name')
        regions_c = regions.dissolve(busmap)
        regions_c.index.name = 'name'
        regions_c = regions_c.reset_index()
        regions_c.to_file(getattr(output, which))


if __name__ == "__main__":

    n = pypsa.Network(filepaths.input.network)

    aggregation_strategies = config["clustering"].get("aggregation_strategies", {})
    # translate str entries of aggregation_strategies to pd.Series functions:
    aggregation_strategies = {
        p: {k: getattr(pd.Series, v) for k,v in aggregation_strategies[p].items()}
        for p in aggregation_strategies.keys()
    }

    n, trafo_map = simplify_network_to_380(n)

    Nyears = n.snapshot_weightings.objective.sum() / 8760

    technology_costs = load_costs(filepaths.input.tech_costs, config['costs'], config['electricity'], Nyears)

    n, simplify_links_map = simplify_links(n, technology_costs, aggregation_strategies)

    n, stub_map = remove_stubs(n, technology_costs, aggregation_strategies)

    busmaps = [trafo_map, simplify_links_map, stub_map]

    # some entries in n.buses are not updated in previous functions, therefore can be wrong. as they are not needed
    # and are lost when clustering (for example with the simpl wildcard), we remove them for consistency:
    buses_c = {'symbol', 'tags', 'under_construction', 'substation_lv', 'substation_off'}.intersection(n.buses.columns)
    n.buses = n.buses.drop(buses_c, axis=1)

    update_p_nom_max(n)

    n.meta = config
    n.export_to_netcdf(filepaths.output.network)

    busmap_s = reduce(lambda x, y: x.map(y), busmaps[1:], busmaps[0])
    busmap_s.to_csv(filepaths.output.busmap)

    simplified_regions(busmaps, filepaths.input, filepaths.output)
