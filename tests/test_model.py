# coding=utf-8

from __future__ import absolute_import

import numpy
from datacube.testutils import mk_sample_dataset, mk_sample_product
from datacube.model import GridSpec
from datacube.utils import geometry
from datacube.storage.storage import measurement_paths


def test_gridspec():
    gs = GridSpec(crs=geometry.CRS('EPSG:4326'), tile_size=(1, 1), resolution=(-0.1, 0.1), origin=(10, 10))
    poly = geometry.polygon([(10, 12.2), (10.8, 13), (13, 10.8), (12.2, 10), (10, 12.2)], crs=geometry.CRS('EPSG:4326'))
    cells = {index: geobox for index, geobox in list(gs.tiles_inside_geopolygon(poly))}
    assert set(cells.keys()) == {(0, 1), (0, 2), (1, 0), (1, 1), (1, 2), (2, 0), (2, 1)}
    assert numpy.isclose(cells[(2, 0)].coordinates['longitude'].values, numpy.linspace(12.05, 12.95, num=10)).all()
    assert numpy.isclose(cells[(2, 0)].coordinates['latitude'].values, numpy.linspace(10.95, 10.05, num=10)).all()


def test_gridspec_upperleft():
    """ Test to ensure grid indexes can be counted correctly from bottom left or top left
    """
    tile_bbox = geometry.BoundingBox(left=1934400.0, top=2414800.0, right=2084400.000, bottom=2264800.000)
    bbox = geometry.BoundingBox(left=1934615, top=2379460, right=1937615, bottom=2376460)
    # Upper left - validated against WELD product tile calculator
    # http://globalmonitoring.sdstate.edu/projects/weld/tilecalc.php
    gs = GridSpec(crs=geometry.CRS('EPSG:5070'), tile_size=(-150000, 150000), resolution=(-30, 30),
                  origin=(3314800.0, -2565600.0))
    cells = {index: geobox for index, geobox in list(gs.tiles(bbox))}
    assert set(cells.keys()) == {(30, 6)}
    assert cells[(30, 6)].extent.boundingbox == tile_bbox

    gs = GridSpec(crs=geometry.CRS('EPSG:5070'), tile_size=(150000, 150000), resolution=(-30, 30),
                  origin=(14800.0, -2565600.0))
    cells = {index: geobox for index, geobox in list(gs.tiles(bbox))}
    assert set(cells.keys()) == {(30, 15)}  # WELD grid spec has 21 vertical cells -- 21 - 6 = 15
    assert cells[(30, 15)].extent.boundingbox == tile_bbox


def test_dataset_measurement_paths():
    format = 'GeoTiff'

    ds = mk_sample_dataset([dict(name=n,
                                 path=n+'.tiff')
                            for n in 'a b c'.split(' ')],
                           uri='file:///tmp/datataset.yml',
                           format=format)

    assert ds.uri_scheme == 'file'
    assert ds.format == format
    paths = measurement_paths(ds)

    for k, v in paths.items():
        assert v == 'file:///tmp/' + k + '.tiff'


def test_product_dimensions():
    product = mk_sample_product('test_product')
    assert product.grid_spec is None
    assert product.dimensions == ('time', 'y', 'x')

    product = mk_sample_product('test_product', with_grid_spec=True)
    assert product.grid_spec is not None
    assert product.dimensions == ('time', 'y', 'x')
