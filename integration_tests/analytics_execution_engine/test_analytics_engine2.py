'''Test the store handler by flushing it, then writing and reading from it.

CAUTION: The database with the last possible index (typically 15) in the store is used. If it ever
contains any data, that gets wiped out!
'''

from __future__ import absolute_import

import logging
from time import sleep
from copy import deepcopy
from pathlib import Path
from urllib.parse import urlparse
from shutil import rmtree
from pprint import pformat
import pytest
from celery import Celery
import numpy as np

from datacube.engine_common.store_handler import StoreHandler, JobStatuses
from datacube.engine_common.rpc_celery import launch_ae_worker, stop_worker, run_python_function_base
from datacube.analytics.analytics_client import AnalyticsClient
from datacube.analytics.update_engine2 import UpdateEngineV2, UpdateActions
from datacube.api.core import Datacube

# Skip all tests if redis cannot be imported
redis = pytest.importorskip('redis')


@pytest.fixture
def store_handler(redis_config):
    '''Connect to the store and flushes the last DB.

    CAUTION: The database with the last possible index (typically 15) in the store is used. If it
    contains any data, that gets wiped out!

    That DB gets wiped again at the end of the tests.
    '''
    store_handler = StoreHandler(**redis_config)
    yield store_handler
    store_handler._store.flushdb()


@pytest.fixture
def input_data():
    '''Fixed test data. The shape is (1, 231, 420).'''
    return {
        'query': {
            'query_1': {
                'product': 'ls5_nbar_albers',
                'measurements': ['blue', 'red'],
                'x': (149.07, 149.18),
                'y': (-35.32, -35.28)
            },
            'query_2': {
                'product': 'ls5_nbar_albers',
                'measurements': ['blue', 'red'],
                'x': (149.07, 149.18),
                'y': (-35.32, -35.28)
            }
        },
        'storage_params': {
            'chunk': (1, 200, 200)
        }
    }


@pytest.fixture(scope='session')
def ee_celery(ee_config):
    process = launch_ae_worker(ee_config)
    yield
    print('Teardown celery')
    process.terminate()


def run_python_function_base_direct(self, *args, **kargs):
    '''Call worker without celery for proper test coverage accounting.'''
    return run_python_function_base(*args, **kargs)


def _submit(test_name, tmpdir, store_handler, local_config, celery_enabled, base_function, test_callback, **params):
    '''Submits a user function and runs a callback test function.

    This cleans the store before and after.
    '''
    logger = logging.getLogger(__name__)
    logger.debug('{sep} Starting {} {sep}'.format(test_name, sep='='*20))

    client = AnalyticsClient(local_config)

    # Override celery calls with direct calls, useful to get proper
    # test coverage estimates
    if not celery_enabled:
        client._run_python_function_base = run_python_function_base_direct.__get__(client, AnalyticsClient)

    logger.debug(client)
    jro = client.submit_python_function(base_function, walltime='00:01:25', paths=local_config.files_loaded,
                                        env=local_config._env, tmpdir=str(tmpdir), **params)

    # Wait a while for the main job to complete
    for tstep in range(85):
        if jro.status == JobStatuses.COMPLETED:
            break
        sleep(1.0)
    assert jro.status == JobStatuses.COMPLETED, client._store.str_dump()

    # Base job should have completed
    assert jro.job.status == JobStatuses.COMPLETED

    # logger.debug('JRO\n{}'.format(jro))
    # logger.debug('Store dump\n{}'.format(client._store.str_dump()))

    test_callback(jro, logger)

    # Clean up S3 and local filesystem
    client.cleanup(jro)

    logger.debug('{sep} Completed {} {sep}'.format(test_name, sep='='*20))


def check_submit_user_data(tmpdir, store_handler, local_config, celery_enabled, input_data):
    '''Check retrieval of user data created by user function as files.'''
    filename = 'my_result.txt'
    text = 'This is an auxillary output file with some text'

    def base_function(data, dc=None, function_params=None, user_data=None):
        # Example of import in the user function
        from pathlib import Path
        filepath = Path(function_params['output_dir']) / function_params['filename']
        # Example of user-data (auxillary file)
        with filepath.open('w') as f:
            f.write(function_params['text'])
        output = {}
        for query, input_data in data.items():
            output_data = input_data
            output[query] = {
                'chunk': output_data.blue.shape,  # will be incorrect for bottom/right cells
                'data': output_data
            }
        return output

    function_params = {
        'filename': filename,
        'text': text,
        'some_value': 2
    }

    def test_callback(jro, logger):
        '''Check user data retrieval.'''
        user_data = jro.user_data
        for datum in user_data:
            filepath = None
            for key, value in datum.items():
                if key == 'files':
                    for filepath in value.values():
                        with Path(filepath).open() as fh:
                            assert fh.read() == text

    _submit('check_submit_user_data', tmpdir, store_handler, local_config, celery_enabled,
            base_function, test_callback,
            function_params=function_params, data=input_data)


def check_submit_job(tmpdir, store_handler, local_config, celery_enabled, index, input_data, chunk=None):
    '''Test basic aspects of job submission.

    It checks the store data and also retrieves and checks the data through the JRO. The default
    chunk size can be modified to test border cases, e.g. single chunk.
    '''
    if chunk:
        data = deepcopy(input_data)
        data['storage_params']['chunk'] = chunk
    else:
        data = input_data

    def base_function(data, dc=None, function_params=None, user_data=None):
        output = {}
        for query, input_data in data.items():
            output_data = input_data
            output_data.red.attrs = input_data.red.attrs.copy()
            output_data.blue.attrs = input_data.blue.attrs.copy()
            output[query] = {
                'chunk': output_data.blue.shape,  # will be incorrect for bottom/right cells
                'data': output_data
            }
        return output

    def test_callback(jro, logger):
        '''Perform store-related and data-related checks.'''
        # == Store related checks ==
        # Ensure an id is set for the job and one of its results
        assert isinstance(jro.job.id, int)
        result_id = jro.results.query_1_red.id
        assert isinstance(result_id, int)

        # Check data stored correctly: normally, never use the client's store directly
        job_meta = jro.client._store.get_job(jro.job.id)
        assert jro.client._store.get_data(job_meta.data_id) == input_data['query']

        # There should be at least one job dependency
        job_dep = jro.client._store.get_job_dependencies(jro.job.id)
        assert len(job_dep[0]) > 0

        # Check the result's base name
        # assert jro.results.query_1_red.to_dict()['base_name'] == 'job_{}_query_1_red'.format(jro.job.id)

        # == Data related checks ==
        # Check all chunk and shape
        for k, ds in jro.results.datasets.items():
            assert ds.to_dict()['chunk'] == data['storage_params']['chunk']

        # Retrieve result and check shape
        returned_calc = jro.results.query_1_red[:]
        assert(returned_calc.shape == (1, 231, 420))

        # Retrieve data directly and check that results are same as data
        dc = Datacube(index=index)
        data_array = dc.load(product='ls5_nbar_albers', latitude=(-35.32, -35.28), longitude=(149.07, 149.18))
        np.testing.assert_array_equal(returned_calc.values, data_array.red.values)

        # Check coordinates and attributes are the same
        np.testing.assert_array_equal(returned_calc.x, data_array.red.x)
        np.testing.assert_array_equal(returned_calc.y, data_array.red.y)
        np.testing.assert_array_equal(returned_calc.time, data_array.red.time)

    _submit('check_submit_job with chunk={}'.format(chunk),
            tmpdir, store_handler, local_config, celery_enabled, base_function, test_callback,
            data=data)


def check_do_the_math(tmpdir, store_handler, local_config, celery_enabled, index, input_data):
    '''Test basic band maths.'''
    # TODO: This kind of function not yet supported:
    # import xarray as xr
    # def general_calculation(data):
    #     new_quantity = data['red'] + data['blue']
    #     return xr.Dataset({'new_quantity': new_quantity})

    # Simple transform
    def band_transform(data, dc=None, function_params=None, user_data=None):
        output = {}
        for query, input_data in data.items():
            output_data = input_data + 1000
            output_data.red.attrs = input_data.red.attrs.copy()
            output_data.blue.attrs = input_data.blue.attrs.copy()
            output[query] = {
                'chunk': output_data.blue.shape,  # will be incorrect for bottom/right cells
                'data': output_data
            }
        return output

    def test_callback(jro, logger):
        '''Retrieve data directly and check that bands are transformed.'''
        returned_calc = jro.results.query_1_red[:]
        dc = Datacube(index=index)
        data_array = dc.load(product='ls5_nbar_albers', latitude=(-35.32, -35.28), longitude=(149.07, 149.18))
        np.testing.assert_array_equal(returned_calc.values, data_array.red.values + 1000)

        # Check coordinates and attributes are the same
        np.testing.assert_array_equal(returned_calc.x, data_array.red.x)
        np.testing.assert_array_equal(returned_calc.y, data_array.red.y)
        np.testing.assert_array_equal(returned_calc.time, data_array.red.time)

    _submit('check_do_the_math',
            tmpdir, store_handler, local_config, celery_enabled, band_transform, test_callback,
            data=input_data)


def check_submit_invalid_data_and_user_tasks(tmpdir, store_handler, local_config, celery_enabled, input_data):
    '''Test for failure if both data and user_tasks are specified for a job.'''
    def base_function(data, dc=None, function_params=None, user_data=None):
        return data['query_1']
    function_params = {
        'shp': (Path(__file__).parent / 'data' / 'polygons.shp').as_uri(),
        'shx': (Path(__file__).parent / 'data' / 'polygons.shx').as_uri(),
        'dbf': (Path(__file__).parent / 'data' / 'polygons.dbf').as_uri(),
    }
    user_tasks = [{'filename': 'polygons.shp',
                   'feature': n} for n in range(4)]

    def test_callback(jro, logger):
        logger.debug(jro)

    # Cannot specify data and user_task at the same time
    with pytest.raises(ValueError):
        _submit('check_submit_invalid_data_and_user_tasks',
                tmpdir, store_handler, local_config, celery_enabled, base_function, test_callback,
                data=input_data, user_tasks=user_tasks)


def check_submit_job_user_tasks(tmpdir, store_handler, local_config, celery_enabled):
    '''Test submission of function with user_tasks instead of data.'''
    def base_function(data, dc=None, function_params=None, user_task=None):
        from pathlib import Path
        from osgeo.gdal import OpenEx
        filepath = Path(function_params['input_dir']) / user_task['filename']
        dataSource = OpenEx(str(filepath))
        extent = None
        feature_no = user_task['feature']
        if dataSource:
            layer = dataSource.GetLayer()
            feature = layer.GetFeature(feature_no)
            geom = feature.GetGeometryRef()
            extent = geom.GetEnvelope()
        else:
            print('Could not open {}'.format(filepath))
        output_path = Path(function_params['output_dir']) / 'sub_dir' / 'feature{:02d}.txt'.format(feature_no)
        output_path.parent.mkdir()
        with output_path.open('w') as fh:
            fh.write('Test: {}\n'.format(feature_no))
        return extent

    function_params = {
        'shp': (Path(__file__).parent / 'data' / 'polygons.shp').as_uri(),
        'shx': (Path(__file__).parent / 'data' / 'polygons.shx').as_uri(),
        'dbf': (Path(__file__).parent / 'data' / 'polygons.dbf').as_uri(),
    }

    user_tasks = [{'filename': 'polygons.shp',
                   'feature': n} for n in range(4)]

    def test_callback(jro, logger):
        '''Check we obtain the expected file contents and extents.'''
        expected_extents = [
            (146.99176708700008, 147.00366523700006, -35.306823330999975, -35.287790282999936),
            (146.43423890000008, 146.47296589200005, -35.249874674999944, -35.19856893299993),
            (146.14353063500005, 146.22597876600003, -35.344454834999965, -35.23281874899993),
            (146.33783807400005, 146.35879447800005, -35.41324290999995, -35.393512105999946)
        ]
        extents = {}
        user_data = jro.user_data
        logger.debug('JRO User data:\n{}'.format(pformat(user_data)))
        for datum in user_data:
            feature_no = None
            exts = None
            filepath = None
            for key, value in datum.items():
                if key == 'output':
                    exts = value
                elif key == 'files':
                    for name, path in value.items():
                        if name[:15] == 'sub_dir/feature':
                            feature_no = int(name[15:17])
                            filepath = path
                else:
                    raise Exception('Unexpected user data: {}: {}'.format(key, value))
            with Path(filepath).open() as fh:
                assert fh.read() == 'Test: {}\n'.format(feature_no)
            extents[feature_no] = exts
        for feature_no, expected_extent in enumerate(expected_extents):
            assert feature_no in extents
            assert extents[feature_no] == expected_extent

    _submit('check_submit_job_user_tasks',
            tmpdir, store_handler, local_config, celery_enabled, base_function, test_callback,
            function_params=function_params, user_tasks=user_tasks)


def test_submit_invalid_update(local_config):
    '''Test for failure of jro updates when passing insufficient data or wrong type.'''
    updater = UpdateEngineV2(paths=local_config.files_loaded,
                             env=local_config._env)
    # Submit invalid action type
    with pytest.raises(ValueError):
        updater.execute(1, 3)

    # Submit invalid result id
    for action in UpdateActions:
        with pytest.raises(ValueError):
            updater.execute(action, 0)
