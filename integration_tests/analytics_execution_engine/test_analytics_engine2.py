'''Test the store handler by flushing it, then writing and reading from it.

CAUTION: The database with the last possible index (typically 15) in the store is used. If it ever
contains any data, that gets wiped out!
'''

from __future__ import absolute_import

import logging
from time import sleep
from sys import version_info
import pytest
from celery import Celery
import numpy as np

from datacube.analytics.utils.store_handler import StoreHandler, JobStatuses
from datacube.analytics.analytics_engine2 import launch_ae_worker, stop_worker, initialise_engines
from datacube.analytics.analytics_client import AnalyticsClient
from datacube.api.core import Datacube

# Skip all tests if redis cannot be imported
redis = pytest.importorskip('redis')


@pytest.fixture(scope='module')
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
def ee_celery(local_config, request):
    if version_info < (3, 0):
        pytest.skip('Celery tests require python3 ')
        return
    yield launch_ae_worker(local_config)
    print('Teardown celery')
    stop_worker()


def test_submit_invalid_job(store_handler, redis_config, local_config, driver_manager, ee_celery):
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


def check_submit_job(store_handler, redis_config, local_config, driver_manager):
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

    def base_function(data):
        return data
    data = {
        'product': 'ls5_nbar_albers',
        'measurements': ['blue', 'red'],
        'x': (149.07, 149.18),
        'y': (-35.32, -35.28)
    }
    client = AnalyticsClient(local_config)
    # TODO: eventually only the jro should be returned. For now we use the results directly for debug
    jro, results = client.submit_python_function(base_function, data, storage_params={'chunk': (1, 231, 420)})

    # TODO: remove when results are not returned any more
    for result_p in results:
        result = result_p.get(disable_sync_subtasks=False)
        assert result.red.shape == (1, 231, 420)
        assert result.blue.shape == (1, 231, 420)

    # Wait a while for the main job to complete
    for tstep in range(30):
        if jro.job.status == JobStatuses.COMPLETED:
            break
        sleep(0.1)
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
    dc = Datacube(driver_manager=driver_manager)
    data_array = dc.load(product='ls5_nbar_albers', latitude=(-35.32, -35.28), longitude=(149.07, 149.18))
    np.testing.assert_array_equal(returned_calc.values, data_array.red.values)

    # check data stored correctly
    final_job = client._store.get_job(jro.job.id)
    assert client._store.get_data(final_job.data_id) == data

    # there should be at least one job dependency
    job_dep = client._store.get_job_dependencies(jro.job.id)
    assert len(job_dep[0]) > 0

    # Leave time for workers to complete their tasks then flush the store
    sleep(0.4)
    store_handler._store.flushdb()


def check_do_the_math(store_handler, redis_config, local_config, driver_manager):
    """
    Submit a function that does something
    """
    logger = logging.getLogger(__name__)
    logger.debug('Started.')

    # TODO: This kind of function not yet supported:
    # import xarray as xr
    # def general_calculation(data):
    #     new_quantity = data['red'] + data['blue']
    #     return xr.Dataset({'new_quantity': new_quantity})

    # Simple transform
    def band_transform(data):
        return data + 1000

    data_desc = {
        'product': 'ls5_nbar_albers',
        'measurements': ['blue', 'red'],
        'x': (149.07, 149.18),
        'y': (-35.32, -35.28)
    }
    client = AnalyticsClient(local_config)
    # TODO: eventually only the jro should be returned. For now we use the results directly for debug
    jro, results = client.submit_python_function(band_transform, data_desc, storage_params={'chunk': (1, 231, 420)})

    # TODO: remove when results are not returned any more
    for result_p in results:
        result = result_p.get(disable_sync_subtasks=False)
        assert result.red.shape == (1, 231, 420)
        assert result.blue.shape == (1, 231, 420)

    # Wait a while for the main job to complete
    for tstep in range(30):
        if jro.job.status == JobStatuses.COMPLETED:
            break
        sleep(0.1)
    assert jro.job.status == JobStatuses.COMPLETED

    print('Before JRO update', jro.results.red['shape'])
    jro.update()
    print('After JRO update', jro.results.red[:].shape)

    returned_calc = jro.results.red[:]

    # Retrieve data directly and check that bands are transformed
    dc = Datacube(driver_manager=driver_manager)
    data_array = dc.load(product='ls5_nbar_albers', latitude=(-35.32, -35.28), longitude=(149.07, 149.18))
    np.testing.assert_array_equal(returned_calc.values, band_transform(data_array.red.values))

    # Leave time for workers to complete their tasks then flush the store
    sleep(0.4)
    store_handler._store.flushdb()
