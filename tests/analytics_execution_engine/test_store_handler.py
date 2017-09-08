from __future__ import absolute_import

import pytest

from datacube.analytics.utils.store_handler import StoreHandler, FunctionTypes


def test_store_handler():
    # Create a new store handler and flush the redis DB. Normally, the _store variable should never
    # be accessed directly. Flushing will have to be managed carefully (if any flushing at all).
    sh = StoreHandler()
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
