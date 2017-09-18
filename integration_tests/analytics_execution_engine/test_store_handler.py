'''Test the store handler by flushing it, then writing and reading from it.

CAUTION: The database with the last possible index (typically 15) in the store is used. If it ever
contains any data, that gets wiped out!
'''

from __future__ import absolute_import

from os.path import expanduser
from pathlib import Path
from configparser import ConfigParser
import pytest

from datacube.analytics.utils.store_handler import *

# Skip all tests if redis cannot be imported
redis = pytest.importorskip('redis')


DEFAULT_CONFIG_FILES = [expanduser('~/.datacube.conf'),
                        expanduser('~/.datacube_integration.conf'),
                        str(Path(__file__).parent.parent.joinpath('agdcintegration.conf')),
                        str(Path(__file__).parent.joinpath('./.datacube.conf')),
                        str(Path(__file__).parent.joinpath('./.datacube_integration.conf'))]
'''Config files from which to pull redis config. The `redis` section in any such file gets merged if
present, later files overwriting earlier ones if the same fields are set again.'''

DEFAULT_REDIS_CONFIG = {
    'host': '127.0.0.1',
    'port': 6379,
    'db': 0,
    'password': None
}
'''Default redis config. It gets merged with/overwritten by the config files.'''

FUNCTION_TYPES = list(FunctionTypes)
RESULT_TYPES = list(ResultTypes)


def get_all_lists(store_handler):
    output = []
    for status in store_handler.K_LISTS.keys():
        output.append(' - {}: {}'.format(status.name,
                                         store_handler._items_with_status(
                                             store_handler.K_JOBS, status)))
    return '\n'.join(output)


@pytest.fixture(scope='module')
def redis_config():
    '''Retrieve and test the redis configuration.

    Configuration is retrieved from `DEFAULT_CONFIG_FILES` or `DEFAULT_REDIS_CONFIG`, and then ping
    the server to check whether it's alive. If so, the config is returned. Otherwise, None is
    returned and all tests in this file are skipped.
    '''
    # Source config
    redis_config = DEFAULT_REDIS_CONFIG
    config = ConfigParser()
    config.read(DEFAULT_CONFIG_FILES)
    if 'redis' in config:
        redis_config.update(config['redis'])
    # Test server
    try:
        store = redis.StrictRedis(**redis_config)
        if store.ping():
            # Select the DB with last index in the current store
            redis_config['db'] = int(store.config_get('databases')['databases']) - 1
            print('\nUsing redis config: {}'.format(redis_config))
            return redis_config
    except redis.exceptions.ConnectionError as conn_error:
        pass
    # Skill all tests
    pytest.skip('No running redis server found at {}'.format(redis_config))
    return None


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


def test_invalid_function_metadata():
    '''Test that FunctionMetadata objects require FunctionTypes as first param.'''
    with pytest.raises(ValueError):
        function = FunctionMetadata(1, None)
    with pytest.raises(ValueError):
        function = FunctionMetadata('Hello', None)


def test_add_job_invalid(store_handler):
    '''Test the addition of jobs with invalid parameters.'''
    # Invalid job: not function (among others)
    with pytest.raises(ValueError):
        store_handler.add_job(None, None, None, None)


def test_add_job(store_handler, user_data):
    '''Test the addition and retrieval of jobs.'''
    store_handler._store.flushdb()
    job_ids_orig = []
    descriptors = []
    for user_no, jobs in user_data.items():
        for job in jobs:
            results = []
            for result in job['results']:
                results.append(ResultMetadata(result['result_type'], result['descriptor']))
                descriptors.append(result['descriptor'])
            result_ids = store_handler.add_result(results)
            job_ids_orig.append(store_handler.add_job(job['job_type'],
                                                      job['function'],
                                                      job['data'],
                                                      result_ids))

    # List all keys in the store
    num_jobs = len(job_ids_orig)
    expected_keys = sorted(
        [u'functions:{}'.format(i) for i in range(1, num_jobs + 1)] +
        [u'data:{}'.format(i) for i in range(1, num_jobs + 1)] +
        [u'jobs:{}'.format(i) for i in range(1, num_jobs + 1)] +
        [u'results:{}'.format(i) for i in range(1, len(descriptors) + 1)] +
        [u'functions:total', u'data:total', u'jobs:total', u'jobs:queued', u'results:total']
    )
    assert str(store_handler) == 'Store keys: {}'.format(expected_keys)

    # List all queued jobs
    job_ids = store_handler.queued_jobs()
    assert job_ids == list(range(1, num_jobs + 1))

    # Retrieve all job functions and data
    FUNCTION_TYPES = list(FunctionTypes)
    for job_id in job_ids:
        job = store_handler.get_job(job_id)
        function_id = job.function_id
        function = store_handler.get_function(function_id)
        data_id = job.data_id
        data = store_handler.get_data(data_id)

        expected_user = int((job_id - 1) / 6)
        expected_job = (job_id - 1) % 6
        print('Job #{:03d} has function #{:03d} ({:7}): "{}" and data #{:03d}: "{}"'.format(
            job_id, function_id, function.function_type.name,
            function.function(), data_id, data))
        assert function.function() == 'User {:03d}, job {:03d}'.format(expected_user, expected_job)
        assert function.function_type == FUNCTION_TYPES[(job_id - 1) % 3]
        assert data == 'Data for {:03d}-{:03d}'.format(expected_user, expected_job)


def test_invalid_set_job_status(store_handler):
    '''Test set_job_status with invalid values.'''
    # Invalid job id: not in store
    with pytest.raises(ValueError):
        store_handler.set_job_status(10000, JobStatuses.COMPLETED)
    # Invalid status: int instead of Enum
    with pytest.raises(ValueError):
        store_handler.set_job_status(9, 1)
    # Invalid status: string instead of Enum
    with pytest.raises(ValueError):
        store_handler.set_job_status(9, 'Hello')


def test_set_job_status(store_handler, user_data):
    '''Test the modification of job status (moving from list to list) in the store.'''
    store_handler._store.flushdb()
    job_ids_orig = []
    descriptors = []
    for user_no, jobs in user_data.items():
        for job in jobs:
            results = []
            for result in job['results']:
                results.append(ResultMetadata(result['result_type'], result['descriptor']))
                descriptors.append(result['descriptor'])
            result_ids = store_handler.add_result(results)
            job_ids_orig.append(store_handler.add_job(job['job_type'],
                                                      job['function'],
                                                      job['data'],
                                                      result_ids))
    store_handler.set_job_status(3, JobStatuses.RUNNING)
    store_handler.set_job_status(4, JobStatuses.COMPLETED)
    store_handler.set_job_status(5, JobStatuses.CANCELLED)
    store_handler.set_job_status(6, JobStatuses.ERRORED)
    print('\nAll lists after status change:\n{}'.format(get_all_lists(store_handler)))
    assert store_handler.queued_jobs() == list(range(1, 3)) + list(range(7, 13))
    assert store_handler.running_jobs() == [3]
    assert store_handler.completed_jobs() == [4]
    assert store_handler.cancelled_jobs() == [5]
    assert store_handler.errored_jobs() == [6]

    # Check invalid input raises an error
    with pytest.raises(ValueError):
        store_handler.set_job_status(9, 1)


def test_add_result(store_handler, user_data):
    '''Test the addition and retrieval results attached to jobs.'''
    store_handler._store.flushdb()
    job_ids = []
    descriptors = []
    for user_no, jobs in user_data.items():
        for job in jobs:
            results = []
            for result in job['results']:
                results.append(ResultMetadata(result['result_type'], result['descriptor']))
                descriptors.append(result['descriptor'])
            result_ids = store_handler.add_result(results)
            job_ids.append(store_handler.add_job(job['job_type'],
                                                 job['function'],
                                                 job['data'],
                                                 result_ids))

    # Validate retrieval straight from result objects
    checked = 0
    for desc_id, descriptor in enumerate(descriptors):
        result = store_handler.get_result(desc_id+1)
        assert result.descriptor == descriptor
        checked += 1
    assert checked > 0, 'No descriptors to assess, please check test'

    # Validate retrieval through jobs
    checked = 0
    for job_id in job_ids:
        job = store_handler.get_job(job_id)
        data = store_handler.get_data(job.data_id)
        for res, result_id in enumerate(job.result_ids):
            result = store_handler.get_result(result_id)
            assert result.result_type == RESULT_TYPES[res % 3]
            assert result.descriptor == '{}-{:03d}'.format(data.replace('Data', 'Descriptor'), res)
        checked += 1
    assert checked > 0, 'No results to assess, please check test'
    store_handler._store.flushdb()


def test_add_result_invalid(store_handler):
    '''Test exceptions on the addition of invalid result parameters.'''
    with pytest.raises(ValueError):
        result_meta = ResultMetadata(1, 'Invalid result type')
    with pytest.raises(ValueError):
        result_meta = ResultMetadata('Hello', 'Invalid result type')
    with pytest.raises(ValueError):
        store_handler.add_result(None)
    with pytest.raises(ValueError):
        store_handler.add_result('Hello')
