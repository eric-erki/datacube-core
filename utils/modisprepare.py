# coding=utf-8
"""
Preparation code supporting MODIS Collection 6 MCD43A1-4 from the USGS LPDAAC
 - https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a1_v006
 - https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a2_v006
 - https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a3_v006
 - https://lpdaac.usgs.gov/dataset_discovery/modis/modis_products_table/mcd43a4_v006
for direct (zip) read access by datacube.

example usage:
    modisprepare.py
    MCD43A1.A2017301.h32v10.006.2017312201117.hdf.xml --output MDC43A1.yaml
    --no-checksum --date 10/10/2017
"""
from __future__ import absolute_import, division
import os
import hashlib
import uuid
import logging
from xml.etree import ElementTree
from pathlib import Path
import yaml
import click
from osgeo import gdal, osr
from datetime import datetime
from dateutil import parser
from click_datetime import Datetime


def get_coords(geo_ref_points, spatial_ref):
    """
    Returns transformed coordinates in latitude and longitude from input
    reference points and spatial reference
    """
    spatial_ref = osr.SpatialReference(spatial_ref)
    t = osr.CoordinateTransformation(spatial_ref, spatial_ref.CloneGeogCS())

    def transform(p):
        lon, lat, z = t.TransformPoint(p['x'], p['y'])
        return {'lon': lon, 'lat': lat}
    return {key: transform(p) for key, p in geo_ref_points.items()}


def populate_coord(doc):
    proj = doc['grid_spatial']['projection']
    doc['extent']['coord'] = get_coords(proj['geo_ref_points'], proj['spatial_reference'])


def fill_image_data(doc, granule_path):
    format_ = None
    bands = {}

    gran_file = gdal.Open(str(granule_path))
    quoted = '"' + str(granule_path) + '"'
    for subds in gran_file.GetSubDatasets():
        index = subds[0].find(quoted)
        if not format_:
            ds = gdal.Open(subds[0])
            projection = ds.GetProjection()
            t = ds.GetGeoTransform()
            bounds = t[0], t[3], t[0] + t[1] * ds.RasterXSize, t[3] + t[5] * ds.RasterYSize
            del ds
            format_ = subds[0][:index - 1]
        else:
            assert format_ == subds[0][:index - 1]

        layer = subds[0][index + len(quoted) + 1:]

        band_name = layer.split(':')[-1]

        if band_name.find('_Parameters_') >= 0:
            for band, suffix in enumerate(['iso', 'vol', 'geo'], 1):
                bands[band_name + '_' + suffix] = {
                    'band': band,
                    'path': str(granule_path),
                    'layer': layer
                }

        else:
            bands[band_name] = {
                'path': str(granule_path),
                'layer': layer
            }
    del gran_file

    if not format_:
        raise RuntimeError('empty dataset')

    doc['image'] = {'bands': bands}
    doc['format'] = {'name': format_}
    doc['grid_spatial'] = {
        'projection': {
            'geo_ref_points': {
                'ul': {'x': min(bounds[0], bounds[2]), 'y': max(bounds[1], bounds[3])},
                'ur': {'x': max(bounds[0], bounds[2]), 'y': max(bounds[1], bounds[3])},
                'll': {'x': min(bounds[0], bounds[2]), 'y': min(bounds[1], bounds[3])},
                'lr': {'x': max(bounds[0], bounds[2]), 'y': min(bounds[1], bounds[3])},
            },
            'spatial_reference': projection,
        }
    }


def prepare_dataset(path):
    """
    Returns yaml content based on content found at input file path
    """
    root = ElementTree.parse(str(path)).getroot()
    product_type = root.findall('./GranuleURMetaData/CollectionMetaData/ShortName')[0].text
    station = root.findall('./DataCenterId')[0].text
    ct_time = parser.parse(root.findall('./GranuleURMetaData/InsertTime')[0].text)
    from_dt = parser.parse('%s %s' % (root.findall('./GranuleURMetaData/RangeDateTime/RangeBeginningDate')[0].text,
                                      root.findall('./GranuleURMetaData/RangeDateTime/RangeBeginningTime')[0].text))
    to_dt = parser.parse('%s %s' % (root.findall('./GranuleURMetaData/RangeDateTime/RangeEndingDate')[0].text,
                                    root.findall('./GranuleURMetaData/RangeDateTime/RangeEndingTime')[0].text))
    checksum_sha1 = hashlib.sha1(open(path, 'rb').read()).hexdigest()
    granules = [granule.text for granule in
                root.findall('./GranuleURMetaData/DataFiles/DataFileContainer/DistributedFileName')]

    documents = []
    for granule in granules:
        doc = {
            'id': str(uuid.uuid4()),
            'product_type': product_type,
            'creation_dt': ct_time.isoformat(),
            'checksum_sha1': checksum_sha1,
            'platform': {'code': 'AQUA_TERRA'},
            'instrument': {'name': 'MODIS'},
            'acquisition': {'groundstation': {'code': station}},
            'extent': {
                'from_dt': from_dt.isoformat(),
                'to_dt': to_dt.isoformat(),
                'center_dt': (from_dt + (to_dt - from_dt) // 2).isoformat(),
            },
            'lineage': {'source_datasets': {}},
        }
        documents.append(doc)
        fill_image_data(doc, path.parent.joinpath(granule))
        populate_coord(doc)
    return documents


def make_datasets(datasets):
    for dataset in datasets:
        path = Path(dataset)

        if path.is_dir():
            paths = list(path.glob('*.xml'))
        elif path.suffix != '.xml':
            raise RuntimeError('want xml')
        else:
            paths = [path]

        for path in paths:
            logging.info("Processing %s...", path)
            try:
                yield path.parent, prepare_dataset(path)
            except Exception as e:
                logging.info("Failed: %s", e)


def absolutify_paths(doc, path):
    for band in doc['image']['bands'].values():
        band['path'] = str(path / band['path'])
    return doc


def archive_yaml(yaml_path, output):
    """
    Archives the input file to the output destination
    """
    archive_path = os.path.join(output, "archive")
    if not os.path.exists(archive_path):
        os.makedirs(archive_path)
    os.rename(yaml_path, (os.path.join(archive_path, os.path.basename(yaml_path))))


@click.command(help=__doc__)
@click.option('--output', help="Write datasets into this directory",
              type=click.Path(exists=False, writable=True, dir_okay=True))
@click.argument('datasets',
                type=click.Path(exists=True, readable=True, writable=False),
                nargs=-1)
@click.option('--date', type=Datetime(format='%d/%m/%Y'), default=datetime.now(),
              help="Enter file creation start date for data preparation")
@click.option('--checksum/--no-checksum', help="Checksum the input dataset to confirm match",
              default=False)
def main(output, datasets, checksum, date):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

    for dataset in datasets:
        (mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(dataset)
        create_date = datetime.utcfromtimestamp(ctime)
        print(date, create_date)
        if create_date <= date:
            logging.info("Dataset creation time "+str(create_date)+" is older than start date "+str(date)+"...SKIPPING")
        else:
            path = Path(dataset)
            if path.suffix not in ['.xml']:
                raise RuntimeError('expecting .xml format as input')
            logging.info("Processing %s", path)
            output_path = Path(output)
            yaml_path = output_path.joinpath(path.name + '.yaml')
            logging.info("Output %s", yaml_path)
            if os.path.exists(yaml_path):
                logging.info("Output already exists %s", yaml_path)
                with open(yaml_path) as f:
                    if checksum:
                        logging.info("Running checksum comparison")
                        datamap = yaml.load_all(f)
                        for data in datamap:
                            yaml_sha1 = data['checksum_sha1']
                            checksum_sha1 = hashlib.sha1(open(path, 'rb').read()).hexdigest()
                        if checksum_sha1 == yaml_sha1:
                            # Only checksum the xml - not the HDF format
                            logging.info("Dataset preparation already done...SKIPPING")
                            continue
                        else:
                            logging.info("Dataset has changed...ARCHIVING out of date yaml")
                            archive_yaml(yaml_path, output)
                    else:
                        logging.info("Dataset preparation already done...SKIPPING")
                        continue
            documents = prepare_dataset(path)
            if documents:
                logging.info("Writing %s dataset(s) into %s", len(documents), yaml_path)
                with open(yaml_path, 'w') as stream:
                    yaml.dump_all(documents, stream)
            else:
                logging.info("No datasets discovered. Bye!")


if __name__ == "__main__":
    main()
