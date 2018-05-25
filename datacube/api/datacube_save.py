from __future__ import absolute_import, division, print_function

import logging
import numpy
from uuid import uuid4
from pathlib import Path
import datetime
from pyproj import Proj, transform
from osgeo import ogr, osr
from pandas import to_datetime

from datacube.drivers import storage_writer_by_name
from datacube.model import Dataset
from datacube.model.utils import make_dataset
from datacube.utils.geometry import _get_coordinates
from pprint import pprint

_LOG = logging.getLogger('datacube-save')


class DatacubeSave(object):

    def __init__(self, datacube):
        self.dc = datacube

    def save(self, array, location, driver_name, product_name, metadata_type, chunking,
             global_attributes=None, variable_params=None):
        # pylint: disable=too-many-locals
        """
        Saves a modified xarray.Dataset from a previous dc.load()

        example:
        ds = DatacubeSave(dc)
        ds.save(nbar, 'my_bucket', 's3aio', 'dcsave_mydata', 'eo',
                chunking = {'time': 1, 'x': 3, 'y': 3})
        ds.save(nbar, '/home/ubuntu/data/output', 's3aio_test', 'dcsave_mydata', 'eo',
                chunking = {'time': 1, 'x': 3, 'y': 3})
        ds.save(nbar, '/home/ubuntu/data/output', 'NetCDF CF', 'dcsave_mydata', 'eo',
                chunking = {'time': 1, 'x': 4, 'y': 4})

        :param xarray.Dataset array: dataset to save
        :param str location: Location to save to. A s3 bucket if using the s3aio or s3aio_test driver.
        :param str driver_name: Storage Driver to use.
        :param str product_name: Product name for the saved dataset. Must start with 'dcsave_'
        :param str metadata_type: Name of existing metadata_type or a dict containing the metadata_type definition.
        :param tupple chunking: chunking to use for storage
        :param dict global_attributes: global metadata to include.
        :param dict variable_params: storage specific parameters
        :rtype: None

        # TODO:
        #   - implement metadata_type dict option
        #   - multiproc the for loop
        #   - create tests
        #   - documentation
        """
        def mk_uri(file_path):
            if driver.uri_scheme == "file":
                return file_path.absolute().as_uri()
            return '{}://{}'.format(driver.uri_scheme, file_path)

        if not product_name.startswith('dcsave_'):
            raise ValueError("Parameter product_name must start with 'dcsave_'")

        # get storage driver
        driver = storage_writer_by_name(driver_name)

        # create or reuse existing product
        product_dict, dimension_order = self.create_product(array, driver_name, driver.format, product_name,
                                                            metadata_type=metadata_type)

        global_attributes, variable_params, storage_config = self.create_storage_params(dimension_order, product_dict,
                                                                                        location, chunking,
                                                                                        global_attributes,
                                                                                        variable_params)

        product_type = self.dc.index.products.get_by_name(product_name)
        if product_type:
            _LOG.info('DatasetType %s already exists, resuing.', product_type.name)
        else:
            product_type = self.dc.index.products.add_document(product_dict)
            _LOG.info('Creating new DatasetType %s.', product_type.name)

        for time in array.time:
            # add time dimension back in after isel
            data_slice = array.sel(time=time).expand_dims('time', 0)

            # name files
            date_string = to_datetime(time.values).strftime('%Y%m%d%H%M%S%f')

            file_path = Path(location + '/' + product_name + '_' + date_string + ".nc").absolute()

            # create dataset (use create_dataset for distributed save)
            dataset = make_dataset(product=product_type,
                                   sources=[],
                                   extent=array.geobox.extent,
                                   center_time=time.values,
                                   uri=mk_uri(file_path),
                                   app_info=None,
                                   valid_data=None)

            # writing to storage unit

            storage_metadata = driver.write_dataset_to_storage(data_slice, file_path,
                                                               global_attributes=global_attributes,
                                                               variable_params=variable_params,
                                                               storage_config=storage_config)

            # indexing dataset
            extra_args = {}
            if (storage_metadata is not None) and len(storage_metadata) > 0:
                extra_args['storage_metadata'] = storage_metadata
            self.dc.index.datasets.add(dataset, sources_policy='skip', **extra_args)

        _LOG.info('Save complete')

    def create_product(self, array, driver_name, format_type, product_name, description=None, metadata=None,
                       metadata_type=None):
        product_dict = {
            'name': product_name,
            'managed': True,
            'description': 'user saved Dataset',
            'metadata': {
                'product_type': 'array_data',
                'platform': {
                    'code': 'USER_RESULTS'
                },
                'instrument': {
                    'name': 'USER'
                },
                'format': {
                    'name': format_type
                }
            },
            'metadata_type': 'eo',
            'measurements': [],
            'storage': {
                'crs': str(array.crs),
                'driver': driver_name,
                'tile_size': {},
                'resolution': {}
            }
        }
        if description:
            product_dict['description'] = description
        if metadata:
            product_dict['metadata'] = metadata
        if metadata_type:
            product_dict['metadata_type'] = metadata_type
        else:
            product_dict['metadata_type'] = array.metadata_type

        for name, variable in array.data_vars.items():
            measurement = {}
            measurement['name'] = name
            measurement['dtype'] = str(variable.dtype)
            measurement['units'] = variable.units
            measurement['nodata'] = variable.nodata
            measurement['spectral_definition'] = variable.spectral_definition
            product_dict['measurements'].append(measurement)

        dimension_order = None
        for name, variable in array.data_vars.items():
            product_dict['storage']['tile_size'][variable.dims[0]] = 1 # tile size for time, always 1
            product_dict['storage']['tile_size'][variable.dims[1]] = array.geobox.resolution[0] # tile size for y
            product_dict['storage']['tile_size'][variable.dims[2]] = array.geobox.resolution[1] # tile size for y

            product_dict['storage']['resolution'][variable.dims[1]] = array.geobox.resolution[0] # resolution for y
            product_dict['storage']['resolution'][variable.dims[2]] = array.geobox.resolution[1] # resolution for x
            dimension_order = list(variable.dims)
            break

        return product_dict, dimension_order

    def create_storage_params(self, dimension_order, product_dict, location, chunking, global_attributes,
                              variable_params):
        global_attributes_default = {
            'acknowledgment': 'tbc',
            'cdm_data_type': 'Grid',
            'coverage_content_type': 'tbc',
            'history': 'tbc',
            'institution': 'tbc',
            'instrument': 'none',
            'keywords': 'tbc',
            'keywords_vocabulary': 'tbc',
            'license': 'tbc',
            'platform': 'USER_RESULTS',
            'product_suite': 'tbc',
            'product_version': 1,
            'publisher_email': 'tbc',
            'publisher_name': 'tbc',
            'publisher_url': 'tbc',
            'references': 'tbc',
            'source': 'tbc',
            'summary': 'tbc',
            'title': 'tbc'
        }
        if global_attributes:
            global_attributes_default.update(global_attributes)

        storage_config_default = {
            'bucket': location,
            'chunking': chunking,
            'crs': product_dict['storage']['crs'],
            'dimension_order': dimension_order,
            'driver': product_dict['storage']['driver'],
            'resolution': product_dict['storage']['resolution'],
            'tile_size': product_dict['storage']['tile_size']
        }

        variable_params_default = {}
        for measurement in product_dict['measurements']:
            variable_param = {}
            variable_param['zlib'] = True
            if variable_params:
                variable_param.update(variable_params)
            variable_param['attrs'] = {'long_name': measurement['name']}
            variable_param['chunksizes'] = [chunking[d] for d in dimension_order]
            variable_params_default[measurement['name']] = variable_param

        return global_attributes_default, variable_params_default, storage_config_default
