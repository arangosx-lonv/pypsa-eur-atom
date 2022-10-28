# SPDX-FileCopyrightText: : 2017-2022 The PyPSA-Eur Authors
# SPDX-License-Identifier: MIT
# coding: utf-8
"""
Description
-----------
Creates GIS shape files of the countries, exclusive economic zones, and `NUTS3 regions <https://en.wikipedia.org/wiki/Nomenclature_of_Territorial_Units_for_Statistics>`_.

Inputs
------
- ``data/borders_national/ne_10m_admin_0_countries.shp``: World country shapes
- ``data/eez/eez_v11.shp``: World `exclusive economic zones <https://en.wikipedia.org/wiki/Exclusive_economic_zone>`_
- ``data/borders_nuts3/NUTS_3_2021_with_UK.shp``: Europe NUTS3 regions (including UK Local Authority Districts)
- ``data/downscaling/nama_10r_3popgdp_2021.tsv.gz``: Average annual population by NUTS3 region
- ``data/downscaling/nama_10r_3gdp_2021.tsv.gz``: Gross domestic product (GDP) by NUTS 3 regions
- ``data/downscaling/uk_regional_population_2021H1.csv``: UK population by Local Authority District
- ``data/downscaling/uk_regional_gdp_2020.csv``: UK GDP by Local Authority District

Outputs
-------
- ``intermediate_files/country_shapes.geojson``: Country shapes out of country selection
- ``intermediate_files/offshore_shapes.geojson``: EEZ shapes out of country selection
- ``intermediate_files/europe_shape.geojson``: Outline of country selection including national borders and EEZ shapes
- ``intermediate_files/nuts3_shapes.geojson``: NUTS3 shapes out of country selection including population and GDP data

"""
from _helpers import set_PROJdir
set_PROJdir()

import logging
import yaml
import numpy as np
from operator import attrgetter
from functools import reduce
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union
from shapely.validation import make_valid
import pycountry as pyc

logger = logging.getLogger(__name__)

# Snakemake parameters replicated here
with open('../config.yaml') as f:
    config = yaml.safe_load(f)

class filepaths:
    class input:
        naturalearth = '../data/borders_national/ne_10m_admin_0_countries.shp'
        nuts3 = '../data/borders_nuts3/NUTS_3_2021_with_UK.shp'
        nuts3pop = '../data/downscaling/nama_10r_3popgdp_2021.tsv.gz'
        nuts3gdp = '../data/downscaling/nama_10r_3gdp_2021.tsv.gz'
        ukpop = '../data/downscaling/uk_regional_population_2021H1.csv'
        ukgdp = '../data/downscaling/uk_regional_gdp_2020.csv'
        eez = '../data/eez/eez_v11.shp'

    class output:
        country_shapes = '../models/' + config['project_folder'] + '/intermediate_files/country_shapes.geojson'
        nuts3_shapes = '../models/' + config['project_folder'] + '/intermediate_files/nuts3_shapes.geojson'
        offshore_shapes = '../models/' + config['project_folder'] + '/intermediate_files/offshore_shapes.geojson'
        europe_shape = '../models/' + config['project_folder'] + '/intermediate_files/europe_shape.geojson'

def _get_country(target, **keys):
    assert len(keys) == 1
    try:
        return getattr(pyc.countries.get(**keys), target)
    except (KeyError, AttributeError):
        return np.nan


def countries(naturalearth, country_list):
    if 'RS' in country_list: country_list.append('KV')

    df = gpd.read_file(naturalearth)

    # Names are a hassle in naturalearth, try several fields
    fieldnames = (df[x].where(lambda s: s!='-99') for x in ('ISO_A2', 'WB_A2', 'ADM0_A3'))
    df['name'] = reduce(lambda x,y: x.fillna(y), fieldnames, next(fieldnames)).str[0:2]

    df = df.loc[df.name.isin(country_list) & ((df['scalerank'] == 0) | (df['scalerank'] == 5))]
    s = df.set_index('name')['geometry'].map(lambda q: q.simplify(tolerance = 0.01))
    # s = df.set_index('name')['geometry']
    if 'RS' in country_list: s['RS'] = s['RS'].union(s.pop('KV'))

    return s


def eez(country_shapes, eez, country_list):
    df = gpd.read_file(eez)
    df = df.query('POL_TYPE == "200NM"') # Remove disputed areas as well as joint regimes
    df = df.loc[df['ISO_TER1'].isin([_get_country('alpha_3', alpha_2=c) for c in country_list])]
    df['name'] = df['ISO_TER1'].map(lambda c: _get_country('alpha_2', alpha_3=c))
    s = df.set_index('name').geometry.map(lambda s: s.simplify(tolerance = 0.01))
    # s = df.set_index('name').geometry
    s = gpd.GeoSeries({k:v for k,v in s.iteritems() if v.distance(country_shapes[k]) < 1e-3})
    s = s.to_frame("geometry")
    s.geometry = s.apply(lambda row: make_valid(row.geometry) if not row.geometry.is_valid else row.geometry, axis=1)
    s.index.name = "name"
    return s


def country_cover(country_shapes, eez_shapes=None):
    shapes = country_shapes
    if eez_shapes is not None:
        shapes = pd.concat([shapes, eez_shapes])

    europe_shape = unary_union(shapes)
    if isinstance(europe_shape, MultiPolygon):
        europe_shape = max(europe_shape, key=attrgetter('area'))
    return Polygon(shell=europe_shape.exterior)


def nuts3(country_shapes, nuts3, nuts3pop, nuts3gdp, ukpop, ukgdp):
    df = gpd.read_file(nuts3)

    # Implementing this reduces detail slightly, but reduces nuts file size by >95%
    df.geometry = df.geometry.map(lambda s: s.simplify(tolerance = 0.01))
    # Simplification can cause some geometries to become invalid - re-fix here to be sure
    df.geometry = df.apply(lambda row: make_valid(row.geometry) if not row.geometry.is_valid else row.geometry, axis=1)

    df = df.rename(columns={'NUTS3': 'id'})[['id', 'geometry']].set_index('id')

    pop = pd.read_table(nuts3pop, na_values=[':'], delimiter=' ?\t', engine='python')
    pop = (pop
           .set_index(pd.MultiIndex.from_tuples(pop.pop('freq,unit,geo\\TIME_PERIOD').str.split(','))).loc['A', 'THS']
           .applymap(lambda x: pd.to_numeric(x, errors='coerce'))
           .fillna(method='ffill', axis=1))['2020']

    gdp = pd.read_table(nuts3gdp, na_values=[':'], delimiter=' ?\t', engine='python')
    gdp = (gdp
           .set_index(pd.MultiIndex.from_tuples(gdp.pop('freq,unit,geo\\TIME_PERIOD').str.split(','))).loc[
               'A', 'MIO_EUR']
           .applymap(lambda x: pd.to_numeric(x, errors='coerce'))
           .fillna(method='ffill', axis=1))['2020']

    uk_pop = pd.read_csv(ukpop).set_index('Code')
    uk_pop = uk_pop['All ages'].squeeze()

    uk_gdp = pd.read_csv(ukgdp).set_index('LA code')
    uk_gdp = uk_gdp['2020'].squeeze()

    pop = pd.concat([pop, uk_pop])
    gdp = pd.concat([gdp, uk_gdp])

    df = df.join(pd.DataFrame(dict(pop=pop, gdp=gdp)))

    # UK local authority district ID value identify the country, then lead with a zero (E/W/N) or one (Scotland)
    df['country'] = df.index.to_series().str[:2].replace(dict(E0='GB', N0='GB', S1='GB', W0='GB', EL='GR'))

    excludenuts = pd.Index(('FRY10', 'FRY20', 'FRY30', 'FRY40', 'FRY50',  # French overseas territories
                            'PT200', 'PT300',  # Azores and Madeira
                            'ES703', 'ES704', 'ES705', 'ES706', 'ES707', 'ES708', 'ES709',  # Canary Islands
                            'ELZZZ',  # Mount Athos autonomous monastic community
                            'NO0B1', 'NO0B2'))  # Svalbard and Jan Mayen
    # Serbia (RS) actually has some regional information in Eurostat, but it's incomplete so not using it here.
    excludecountry = pd.Index(('MT', 'TR', 'LI', 'IS', 'CY', 'KV', 'AD', 'SM', 'VA', 'MC', 'RS'))

    df = df.loc[df.index.difference(excludenuts)]
    df = df.loc[~df.country.isin(excludecountry)]

    df = df.loc[df.index.difference(excludenuts)]
    df = df.loc[~df.country.isin(excludecountry)]

    # Add country-level values for Bosnia and Herzegovina, Serbia
    manual = gpd.GeoDataFrame(
        [['BA000', 'BA', 3263.], # https://data.worldbank.org/indicator/SP.POP.TOTL?locations=BA
         ['ME000', 'ME', 620.], # https://data.worldbank.org/indicator/SP.POP.TOTL?locations=ME
         ['RS000', 'RS', 6844.]], # https://data.worldbank.org/indicator/SP.POP.TOTL?locations=RS
        columns=['NUTS_ID', 'country', 'pop']
    ).set_index('NUTS_ID')
    manual['geometry'] = manual['country'].map(country_shapes)
    manual = manual.dropna()

    df = pd.concat([df, manual], sort=False)

    return df


if __name__ == "__main__":
    country_shapes = countries(filepaths.input.naturalearth, config['countries'])
    country_shapes.reset_index().to_file(filepaths.output.country_shapes)

    offshore_shapes = eez(country_shapes, filepaths.input.eez, config['countries'])
    offshore_shapes.reset_index().to_file(filepaths.output.offshore_shapes)

    europe_shape = gpd.GeoDataFrame(geometry=[country_cover(country_shapes, offshore_shapes.geometry)])
    europe_shape.reset_index().to_file(filepaths.output.europe_shape)

    nuts3_shapes = nuts3(country_shapes, filepaths.input.nuts3, filepaths.input.nuts3pop,
                         filepaths.input.nuts3gdp, filepaths.input.ukpop, filepaths.input.ukgdp)
    nuts3_shapes.reset_index().to_file(filepaths.output.nuts3_shapes)
