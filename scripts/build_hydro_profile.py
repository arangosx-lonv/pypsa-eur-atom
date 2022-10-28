# SPDX-FileCopyrightText: : 2017-2022 The PyPSA-Eur Authors
# SPDX-License-Identifier: MIT
# coding: utf-8
"""
Description
-----------
Build hydroelectric inflow time-series for each country.

Inputs
------
- ``data/eia_hydro_annual_generation.csv``: Hydroelectricity net generation per country and year
- ``intermediate_files/country_shapes.geojson``: Country shapes out of country selection
- ``"data/cutouts/" + config['renewable']['hydro']['cutout'].nc``: Weather dataset with runoff information

Outputs
-------
- ``intermediate_files/profile_hydro.nc``: Country-level time series of inflow to the state of charge (in MW), e.g., due to river inflow in hydro reservoir

"""
from _helpers import set_PROJdir
set_PROJdir()

import logging
import yaml
import atlite
import geopandas as gpd
import pandas as pd
import country_converter as coco
cc = coco.CountryConverter()

# Snakemake parameters replicated here
with open('../config.yaml') as f:
    config = yaml.safe_load(f)

class filepaths:
    class input:
        country_shapes = '../models/' + config['project_folder'] + '/intermediate_files/country_shapes.geojson'
        eia_hydro_generation = '../data/eia_hydro_annual_generation.csv'
        cutout = f"../data/cutouts/{config['renewable']['hydro']['cutout']}.nc" if 'hydro' in config['renewable'] else "config['renewable']['hydro']['cutout'] not configured"

    output = '../models/' + config['project_folder'] + '/intermediate_files/profile_hydro.nc'

def get_eia_annual_hydro_generation(fn, countries):

    # in billion kWh/a = TWh/a
    df = pd.read_csv(fn, skiprows=2, index_col=1, na_values=[u' ','--']).iloc[1:, 1:]
    df.index = df.index.str.strip()

    former_countries = {
        "Former Czechoslovakia": dict(
            countries=["Czech Republic", "Slovakia"],
            start=1980, end=1992),
        "Former Serbia and Montenegro": dict(
            countries=["Serbia", "Montenegro"],
            start=1992, end=2005),
        "Former Yugoslavia": dict(
            countries=["Slovenia", "Croatia", "Bosnia and Herzegovina", "Serbia", "Montenegro", "North Macedonia"],
            start=1980, end=1991),
    }

    for k, v in former_countries.items():
        period = [str(i) for i in range(v["start"], v["end"]+1)]
        ratio = df.loc[v['countries']].T.dropna().sum()
        ratio /= ratio.sum()
        for country in v['countries']:
            df.loc[country, period] = df.loc[k, period] * ratio[country]

    baltic_states = ["Latvia", "Estonia", "Lithuania"]
    df.loc[baltic_states] = df.loc[baltic_states].T.fillna(df.loc[baltic_states].mean(axis=1)).T

    df.loc["Germany"] = df.filter(like='Germany', axis=0).sum()
    df.loc["Serbia"] += df.loc["Kosovo"].fillna(0.)
    df = df.loc[~df.index.str.contains('Former')]
    df.drop(["Europe", "Germany, West", "Germany, East", "Kosovo"], inplace=True)

    df.index = cc.convert(df.index, to='iso2')
    df.index.name = 'countries'

    df = df.T[countries] * 1e6  # in MWh/a

    return df


logger = logging.getLogger(__name__)

if __name__ == "__main__":
    config_hydro = config['renewable']['hydro']
    cutout = atlite.Cutout(filepaths.input.cutout)

    countries = config['countries']
    country_shapes = (gpd.read_file(filepaths.input.country_shapes)
                      .set_index('name')['geometry'].reindex(countries))
    country_shapes.index.name = 'countries'

    fn = filepaths.input.eia_hydro_generation
    eia_stats = get_eia_annual_hydro_generation(fn, countries)
    
    inflow = cutout.runoff(shapes=country_shapes,
                           smooth=True,
                           lower_threshold_quantile=True,
                           normalize_using_yearly=eia_stats)

    if 'clip_min_inflow' in config_hydro:
        inflow = inflow.where(inflow > config_hydro['clip_min_inflow'], 0)

    inflow.to_netcdf(filepaths.output)
