'''Test the store handler by flushing it, then writing and reading from it.

CAUTION: The database with the last possible index (typically 15) in the store is used. If it ever
contains any data, that gets wiped out!
'''

from __future__ import absolute_import

import logging
from time import sleep
from pathlib import Path
from urllib.parse import urlparse
from shutil import rmtree
from pprint import pformat
import pytest
from celery import Celery
import numpy as np

from datacube.engine_common.store_handler import StoreHandler, JobStatuses
from datacube.analytics.analytics_worker import launch_ae_worker, stop_worker
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


@pytest.fixture(scope='module')
def user_data():
    users = {}
    for user_no in range(2):
        jobs = []
        for job_no in range(6):
            def function(user_no=user_no, job_no=job_no):
                return 'User {:03d}, job {:03d}'.format(user_no, job_no)
            jobs.append({
                'job_type': FUNCTION_TYPES[job_no % 3],
                'function': function,
                'data': 'Data for {:03d}-{:03d}'.format(user_no, job_no),
                'results': [{
                    'result_type': RESULT_TYPES[result_no % 3],
                    'descriptor': 'Descriptor for {:03d}-{:03d}-{:03d}'.format(user_no, job_no, result_no)
                } for result_no in range(3)]
                })
        users['user{:03d}'.format(user_no)] = jobs
    return users


@pytest.fixture(scope='session')
def ee_celery(ee_config):
    process = launch_ae_worker(ee_config)
    yield
    print('Teardown celery')
    process.terminate()


def _test_submit_invalid_job(store_handler, redis_config, local_config, index, ee_celery):
    '''
    Test for failure of job submission when passing insufficient data or wrong type
    '''
    store_handler._store.flushdb()
    client = AnalyticsClient(local_config)

    # submit bad data that is not a dictionary
    with pytest.raises(TypeError):
        analysis_p = client.submit_python_function(lambda x: x, [1, 2, 3, 4])

    # Submit bad data that is a dict but does not have any measurements
    with pytest.raises(LookupError):
        analysis_p = client.submit_python_function(lambda x: x, {'a': 1, 'b': 2})

    store_handler._store.flushdb()


def check_submit_job_params(store_handler, local_config, index):
    logger = logging.getLogger(__name__)
    logger.debug('Started.')

    store_handler._store.flushdb()

    filename = 'my_result.txt'

    def base_function(data, function_params=None, user_data=None):
        # Example of import in the function
        from pathlib import Path
        filepath = Path(function_params['output_dir']) / function_params['filename']
        # Example of user-data (auxillary file)
        with filepath.open('w') as f:
            f.write('This is an auxillary output file with some text')
        return data['query_1']

    function_params = {
        'filename': filename,
        'some_value': 2
    }
    data = {
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
            'chunk': (1, 120, 420)
        }
    }

    client = AnalyticsClient(local_config)
    client.update_config(local_config)
    # Use a chunk with x=120 so that 2 sub-jobs get created
    # TODO: eventually only the jro should be returned. For now we use the results directly for debug
    jro, results = client.submit_python_function(base_function, function_params=function_params,
                                                 data=data)

    # Wait a while for the main job to complete
    for tstep in range(30):
        if jro.job.status == JobStatuses.COMPLETED:
            break
        sleep(0.5)
    assert jro.job.status == JobStatuses.COMPLETED
    jro.update()

    logger.debug('JRO\n{}'.format(jro))
    logger.debug('Store dump\n{}'.format(client._store.str_dump()))

    use_s3 = local_config.execution_engine_config['use_s3']
    user_data = jro.user_data
    for datum in user_data:
        assert filename in datum
        if use_s3:
            assert 'bucket' in datum[filename]
            assert 'key' in datum[filename]
            assert datum[filename]['key'][-len(filename):] == filename
        else:
            filepath = urlparse(datum[filename]).path
            with Path(filepath).open() as fh:
                assert fh.read() == 'This is an auxillary output file with some text'
        # Onus is on end user to clean output files copied locally
        if 'base_dir' in datum:
            rmtree(datum['base_dir'])

    # Leave time for workers to complete their tasks then flush the store
    sleep(1)
    store_handler._store.flushdb()


def check_submit_job(store_handler, local_config, index):
    '''Test the following:
        - the submission of a job with real data
        - decomposition
        - execute function
        - save data
        - construct JRO
        - check JRO.

    This is a stub.

    This test function needs further work to test the JRO and corresponding store values.
    '''
    logger = logging.getLogger(__name__)
    logger.debug('Started.')

    store_handler._store.flushdb()

    def base_function(data, function_params=None, user_data=None):
        return data['query_1']
    data = {
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
            'chunk': (1, 231, 420)
        }
    }
    client = AnalyticsClient(local_config)
    client.update_config(local_config)
    # TODO: eventually only the jro should be returned. For now we use the results directly for debug
    jro, results = client.submit_python_function(base_function, data=data)

    # Wait a while for the main job to complete
    for tstep in range(30):
        if jro.job.status == JobStatuses.COMPLETED:
            break
        sleep(0.5)
    assert jro.job.status == JobStatuses.COMPLETED
    jro.update()

    logger.debug('JRO\n{}'.format(jro))
    logger.debug('Store dump\n{}'.format(client._store.str_dump()))

    # Ensure an id is set for the job and one of its datasets
    assert isinstance(jro.job.id, int)
    result_id = jro.results.datasets['blue'].to_dict()['id']
    assert isinstance(result_id, int)

    # Check the dataset base name
    assert jro.results.datasets['blue'].to_dict()['base_name'] == 'result_{:07d}'.format(result_id)

    # chunk and shape
    for k, ds in jro.results.datasets.items():
        assert ds.to_dict()['chunk'] == (1, 231, 420)
        # TODO: implement JRO updates through new calls to the client --> engine

    # Base job should be complete unless something went wrong with worker threads.
    # submit_python_function currently waits until jobs complete then sets base job status
    assert jro.job.status == JobStatuses.COMPLETED

    # Retrieve result and check shape
    returned_calc = jro.results.red[:]
    assert(returned_calc.shape == (1, 231, 420))

    # Retrieve data directly and check that results are same as data
    dc = Datacube(index=index)
    data_array = dc.load(product='ls5_nbar_albers', latitude=(-35.32, -35.28), longitude=(149.07, 149.18))
    np.testing.assert_array_equal(returned_calc.values, data_array.red.values)

    # check data stored correctly
    final_job = client._store.get_job(jro.job.id)
    assert client._store.get_data(final_job.data_id) == data['query']

    # there should be at least one job dependency
    job_dep = client._store.get_job_dependencies(jro.job.id)
    assert len(job_dep[0]) > 0

    for datum in jro.user_data:
        # Onus is on end user to clean output files copied locally
        if 'base_dir' in datum:
            rmtree(datum['base_dir'])

    # Leave time for workers to complete their tasks then flush the store
    sleep(1)
    store_handler._store.flushdb()


def check_do_the_math(store_handler, local_config, index):
    """
    Submit a function that does something
    """
    logger = logging.getLogger(__name__)
    logger.debug('Started.')

    store_handler._store.flushdb()

    # TODO: This kind of function not yet supported:
    # import xarray as xr
    # def general_calculation(data):
    #     new_quantity = data['red'] + data['blue']
    #     return xr.Dataset({'new_quantity': new_quantity})

    # Simple transform
    def band_transform(data, function_params=None, user_data=None):
        return data['query_1'] + 1000

    data = {
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
            'chunk': (1, 231, 420)
        }
    }

    client = AnalyticsClient(local_config)
    client.update_config(local_config)
    # TODO: eventually only the jro should be returned. For now we use the results directly for debug
    jro, results = client.submit_python_function(band_transform, data=data)

    # Wait a while for the main job to complete
    for tstep in range(30):
        if jro.job.status == JobStatuses.COMPLETED:
            break
        sleep(0.5)
    assert jro.job.status == JobStatuses.COMPLETED

    print('Before JRO update', jro.results.red['shape'])
    jro.update()
    print('After JRO update', jro.results.red[:].shape)

    returned_calc = jro.results.red[:]

    # Retrieve data directly and check that bands are transformed
    dc = Datacube(index=index)
    data_array = dc.load(product='ls5_nbar_albers', latitude=(-35.32, -35.28), longitude=(149.07, 149.18))
    np.testing.assert_array_equal(returned_calc.values, data_array.red.values + 1000)

    # Leave time for workers to complete their tasks then flush the store
    sleep(1)
    print('Store dump\n{}'.format(client._store.str_dump()))
    store_handler._store.flushdb()


def check_submit_invalid_data_and_user_tasks(local_config):
    '''Test for failure if both data and user_tasks are specified for a job.'''

    def base_function(data, function_params=None, user_data=None):
        return data['query_1']
    data = {
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
            'chunk': (1, 231, 420)
        }
    }
    function_params = {
        'shp': (Path(__file__).parent / 'data' / 'polygons.shp').as_uri(),
        'shx': (Path(__file__).parent / 'data' / 'polygons.shx').as_uri(),
        'dbf': (Path(__file__).parent / 'data' / 'polygons.dbf').as_uri(),
    }
    user_tasks = [{'filename': 'polygons.shp',
                   'feature': n} for n in range(4)]
    client = AnalyticsClient(local_config)
    client.update_config(local_config)
    # Submit invalid action type
    with pytest.raises(ValueError):
        client.submit_python_function(base_function, data=data, user_tasks=user_tasks)


def check_submit_job_user_tasks(store_handler, local_config, index):
    '''Test the following:
        - the submission of a job with user tasks (instead of automatic data decomposition)
        - decomposition using user tasks
        - execute function
        - save data
        - construct JRO
        - check JRO.

    This is a stub.

    This test function needs further work to test the JRO and corresponding store values.
    '''
    logger = logging.getLogger(__name__)
    logger.debug('Started.')

    store_handler._store.flushdb()

    def base_function(data, function_params=None, user_task=None):
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

    expected_extents = [
        (146.99176708700008, 147.00366523700006, -35.306823330999975, -35.287790282999936),
        (146.43423890000008, 146.47296589200005, -35.249874674999944, -35.19856893299993),
        (146.14353063500005, 146.22597876600003, -35.344454834999965, -35.23281874899993),
        (146.33783807400005, 146.35879447800005, -35.41324290999995, -35.393512105999946)
    ]

    client = AnalyticsClient(local_config)
    client.update_config(local_config)
    # TODO: eventually only the jro should be returned. For now we use the results directly for debug
    jro, results = client.submit_python_function(base_function, function_params=function_params,
                                                 user_tasks=user_tasks)

    # Wait a while for the main job to complete
    for tstep in range(30):
        if jro.job.status == JobStatuses.COMPLETED:
            break
        sleep(0.5)
    assert jro.job.status == JobStatuses.COMPLETED
    jro.update()

    logger.debug('JRO\n{}'.format(jro.results))
    logger.debug('Store dump\n{}'.format(client._store.str_dump()))

    # Check we obtain the expected file contents and extents
    extents = {}
    use_s3 = local_config.execution_engine_config['use_s3']
    user_data = jro.user_data
    print('JRO User data:\n{}'.format(pformat(user_data)))
    for datum in user_data:
        feature_no = None
        for key, value in datum.items():
            if key[8:15] == 'feature':
                feature_no = int(key[15:17])
                filepath = urlparse(value).path
                if not use_s3:
                    with Path(filepath).open() as fh:
                        assert fh.read() == 'Test: {}\n'.format(feature_no)
                # Onus is on end user to clean output files copied locally
                if 'base_dir' in datum:
                    rmtree(datum['base_dir'])
                if 'output' in datum:
                    extents[feature_no] = datum['output']
                    break
    for feature_no, expected_extent in enumerate(expected_extents):
        assert feature_no in extents
        assert extents[feature_no] == expected_extent

    # Leave time for workers to complete their tasks then flush the store
    sleep(1)
    store_handler._store.flushdb()


def test_submit_invalid_update(ee_config):
    '''Test for failure of jro updates when passing insufficient data or wrong type.'''
    updater = UpdateEngineV2(ee_config)
    # Submit invalid action type
    with pytest.raises(ValueError):
        updater.execute(1, 3)

    # Submit invalid result id
    for action in UpdateActions:
        with pytest.raises(ValueError):
            updater.execute(action, 0)
