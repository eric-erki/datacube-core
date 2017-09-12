'''Test the store handler by flushing it, then writing and reading from it.

CAUTION: The database with the last possible index (typically 15) in the store is used. If it ever
contains any data, that gets wiped out!
'''

from __future__ import absolute_import

import pytest
from os.path import expanduser
from configparser import ConfigParser

from datacube.analytics.utils.store_handler import StoreHandler, FunctionTypes

# Skip all tests if redis cannot be imported
redis = pytest.importorskip('redis')


DEFAULT_CONFIG_FILES = [expanduser('~/.datacube.conf'),
                        expanduser('~/.datacube_integration.conf')]
'''Config files from which to pull redis config. The `redis` section in any such file gets merged if
present, later files overwriting earlier ones if the same fields are set again.'''

DEFAULT_REDIS_CONFIG = {
    'host': '127.0.0.1',
    'port': 6379,
    'db': 0,
    'password': None
}
'''Default redis config. It gets merged with/overwritten by the config files.'''


@pytest.fixture
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
            return redis_config
    except redis.exceptions.ConnectionError as conn_error:
        pass
    # Skill all tests
    pytest.skip('No running redis server found at {}'.format(redis_config))
    return None


def test_store_handler(redis_config):
    '''Test the store handler by flushing it, then writing and reading from it.

    CAUTION: The database with the last possible index (typically 15) in the store is used. If it
    ever contains any data, that gets wiped out!
    '''
    sh = StoreHandler(**redis_config)
    sh._store.flushdb()

    # Create 2 users with 6 jobs each. The function simply returns a string denoting the user and
    # job number. Same for the data.
    funcTypes = list(FunctionTypes)
    for user in range(2):
        user_id = 'user{:03d}'.format(user)
        for job in range(6):
            def function():
                return 'User {:03d}, job {:03d}'.format(user, job)
            data = 'Data for {:03d}-{:03d}'.format(user, job)
            # Asign function type to same number as job % 3, for testing only!
            sh.add_job(funcTypes[job % 3], function, data)

    # List all keys in the store
    expected_keys = sorted(
        [u'functions:{}'.format(i) for i in range(1, 13)] +
        [u'data:{}'.format(i) for i in range(1, 13)] +
        [u'jobs:{}'.format(i) for i in range(1, 13)] +
        [u'functions:total', u'data:total', u'jobs:total', u'jobs:queued']
    )
    print(str(sh))
    assert str(sh) == 'Store keys: {}'.format(expected_keys)

    # List all queued jobs
    job_ids = sh.queued_jobs()
    print('Job IDs: {}'.format(job_ids))
    assert job_ids == list(range(1, 13))

    # Retrieve all job functions and data
    for job_id in job_ids:
        job = sh.get_job(job_id)
        function_id = job.function_id
        function = sh.get_function(function_id)
        data_id = job.data_id
        data = sh.get_data(data_id)

        expected_user = int((job_id - 1) / 6)
        expected_job = (job_id - 1) % 6
        print('Job #{:03d} has function #{:03d} ({:7}): "{}" and data #{:03d}: "{}"'.format(
              job_id, function_id, function.function_type.name,
              function.function(), data_id, data))
        assert function.function() == 'User {:03d}, job {:03d}'.format(expected_user, expected_job)
        assert function.function_type == funcTypes[(job_id - 1) % 3]
        assert data == 'Data for {:03d}-{:03d}'.format(expected_user, expected_job)
