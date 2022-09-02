# SPDX-FileCopyrightText: : 2017-2022 The PyPSA-Eur Authors
#
# SPDX-License-Identifier: MIT

"""
Rasters the vector data of the `Natura 2000 <https://en.wikipedia.org/wiki/Natura_2000>`_ natural protection areas onto all cutout regions.

Relevant Settings
-----------------

.. code:: yaml

    renewable:
        {technology}:
            cutout:

.. seealso::
    Documentation of the configuration file ``config.yaml`` at
    :ref:`renewable_cf`

Inputs
------

- ``data/bundle/natura/Natura2000_end2015.shp``: `Natura 2000 <https://en.wikipedia.org/wiki/Natura_2000>`_ natural protection areas.

    .. image:: ../img/natura.png
        :scale: 33 %

Outputs
-------

- ``resources/natura.tiff``: Rasterized version of `Natura 2000 <https://en.wikipedia.org/wiki/Natura_2000>`_ natural protection areas to reduce computation times.

    .. image:: ../img/natura.png
        :scale: 33 %

Description
-----------

"""

import logging
from _helpers import configure_logging

import yaml
import atlite
import geopandas as gpd
import rasterio as rio
from rasterio.features import geometry_mask
from rasterio.warp import transform_bounds

logger = logging.getLogger(__name__)

# Snakemake parameters replicated here
with open('../config.yaml') as f:
    config = yaml.safe_load(f)

class filepaths:
    class input:
        natura = "data/bundle/natura/Natura2000_end2015.shp",
        cutouts = {f"../cutouts/{cut}.nc" for cut in config['atlite']['cutouts']}

    output = '../resources/natura.tiff'


def determine_cutout_xXyY(cutout_name):
    cutout = atlite.Cutout(cutout_name)
    assert cutout.crs.to_epsg() == 4326
    x, X, y, Y = cutout.extent
    dx, dy = cutout.dx, cutout.dy
    return [x - dx/2., X + dx/2., y - dy/2., Y + dy/2.]


def get_transform_and_shape(bounds, res):
    left, bottom = [(b // res)* res for b in bounds[:2]]
    right, top = [(b // res + 1) * res for b in bounds[2:]]
    shape = int((top - bottom) // res), int((right - left) / res)
    transform = rio.Affine(res, 0, left, 0, -res, top)
    return transform, shape


if __name__ == "__main__":
    cutouts = filepaths.input.cutouts
    xs, Xs, ys, Ys = zip(*(determine_cutout_xXyY(cutout) for cutout in cutouts))
    bounds = transform_bounds(4326, 3035, min(xs), min(ys), max(Xs), max(Ys))
    transform, out_shape = get_transform_and_shape(bounds, res=100)

    # adjusted boundaries
    shapes = gpd.read_file(filepaths.input.natura).to_crs(3035)
    raster = ~geometry_mask(shapes.geometry, out_shape[::-1], transform)
    raster = raster.astype(rio.uint8)

    with rio.open(filepaths.output, 'w', driver='GTiff', dtype=rio.uint8,
                  count=1, transform=transform, crs=3035, compress='lzw',
                  width=raster.shape[1], height=raster.shape[0]) as dst:
        dst.write(raster, indexes=1)
