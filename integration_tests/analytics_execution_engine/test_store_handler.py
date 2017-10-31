'''Test the store handler by flushing it, then writing and reading from it.

CAUTION: The database with the last possible index (typically 15) in the store is used. If it ever
contains any data, that gets wiped out!
'''

from __future__ import absolute_import

from os.path import expanduser
from pathlib import Path
from configparser import ConfigParser
import pytest

import datacube.analytics.job_result
from datacube.engine_common.store_handler import *


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
                'function_type': FUNCTION_TYPES[job_no % 3],
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
    store_handler._store.flushdb()
    # Invalid job: not function (among others)
    with pytest.raises(ValueError):
        store_handler.add_job(None, None, None, None)


def test_add_job(store_handler, user_data):
    '''Test the addition and retrieval of jobs.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    # List all keys in the store
    assert len(expected_jobs) > 0, 'No jobs to assess, please check test'
    job_count = len(expected_jobs)
    result_count = len(expected_results)
    expected_keys = sorted(
        [u'functions:{}'.format(i+1) for i in range(job_count)] +
        [u'data:{}'.format(i+1) for i in range(job_count)] +
        [u'jobs:{}'.format(i+1) for i in range(job_count)] +
        [u'jobs:stats:{}'.format(i+1) for i in range(job_count)] +
        [u'results:{}'.format(i+1) for i in range(result_count)] +
        [u'results:stats:{}'.format(i+1) for i in range(result_count)] +
        [u'functions:total', u'data:total', u'jobs:total', u'jobs:queued', u'results:total']
    )
    assert str(store_handler) == 'Store keys: {}'.format(expected_keys)

    # List all queued jobs
    job_ids = store_handler.queued_jobs()
    assert sorted(job_ids) == sorted(expected_jobs.keys())

    # Retrieve all job functions and data
    for job_id in job_ids:
        expected_job = expected_jobs[job_id]

        job = store_handler.get_job(job_id)
        function_meta = store_handler.get_function(job.function_id)
        assert function_meta.function_type == expected_job['function_type']
        assert function_meta.function() == expected_job['function']()

        data = store_handler.get_data(job.data_id)
        assert data == expected_job['data']


def test_get_job_status(store_handler, user_data):
    '''Test get job status.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    assert len(expected_jobs) > 0, 'No jobs to assess, please check test'
    for job_id in expected_jobs.keys():
        assert store_handler.get_job_status(job_id) == JobStatuses.QUEUED

    # Check invalid inputs raise an error
    with pytest.raises(ValueError):
        status = store_handler.get_job_status(0)
    with pytest.raises(ValueError):
        status = store_handler.get_job_status('Hello')


def test_set_job_status(store_handler, user_data):
    '''Test the modification of job status (moving from list to list) in the store.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    # We test using 4 random `expected_jobs` ids
    assert len(expected_jobs) > 3, 'Not enough jobs to assess, please check test'
    jids = list(expected_jobs.keys())[:4]
    store_handler.set_job_status(jids[0], JobStatuses.RUNNING)
    assert store_handler.running_jobs() == [jids[0]]

    store_handler.set_job_status(jids[0], JobStatuses.COMPLETED)
    store_handler.set_job_status(jids[1], JobStatuses.COMPLETED)
    assert store_handler.completed_jobs() == [jids[0], jids[1]]

    store_handler.set_job_status(jids[2], JobStatuses.CANCELLED)
    assert store_handler.cancelled_jobs() == [jids[2]]

    store_handler.set_job_status(jids[0], JobStatuses.ERRORED)
    store_handler.set_job_status(jids[3], JobStatuses.ERRORED)
    assert store_handler.errored_jobs() == [jids[0], jids[3]]

    # print('\nAll lists after status change:\n{}'.format(get_all_lists(store_handler)))
    assert store_handler.queued_jobs() == list(set(expected_jobs.keys()) - set(jids))
    assert store_handler.running_jobs() == []
    assert store_handler.completed_jobs() == [jids[1]]
    assert store_handler.cancelled_jobs() == [jids[2]]
    assert store_handler.errored_jobs() == [jids[0], jids[3]]

    # Invalid job id: not in store
    with pytest.raises(ValueError):
        store_handler.set_job_status(0, JobStatuses.COMPLETED)
    # Invalid job id: string
    with pytest.raises(ValueError):
        store_handler.set_job_status('Hello', JobStatuses.COMPLETED)
    # Invalid status: int instead of Enum
    with pytest.raises(ValueError):
        store_handler.set_job_status(9, 1)
    # Invalid status: string instead of Enum
    with pytest.raises(ValueError):
        store_handler.set_job_status(9, 'Hello')


def test_add_result(store_handler, user_data):
    '''Test the addition and retrieval results attached to jobs.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    # Validate retrieval straight from result objects
    assert len(expected_jobs) > 0, 'No jobs to assess, please check test'
    assert len(expected_results) > 0, 'No results to assess, please check test'
    for result_id, result_metadata in expected_results.items():
        result = store_handler.get_result(result_id)
        if isinstance(result, ResultMetadata):
            assert result.descriptor == result_metadata.descriptor
            assert result.result_type == result_metadata.result_type
        else:
            assert isinstance(result, list)
            assert sorted(result) == sorted(result_metadata)

    # Validate retrieval through jobs
    job_ids = store_handler.queued_jobs()
    assert sorted(job_ids) == sorted(expected_jobs.keys())
    for job_id in job_ids:
        expected_job = expected_jobs[job_id]
        expected_result_ids = expected_results[expected_job['result_id']]
        # List of results
        job = store_handler.get_job(job_id)
        result_ids = store_handler.get_result(job.result_id)
        assert isinstance(result_ids, list)
        assert isinstance(expected_result_ids, list)
        assert sorted(result_ids) == sorted(expected_result_ids)

        for result_id in result_ids:
            result_metadata = expected_results[result_id]
            result = store_handler.get_result(result_id)
            assert isinstance(result, ResultMetadata)
            assert isinstance(result_metadata, ResultMetadata)
            assert result.descriptor == result_metadata.descriptor
            assert result.result_type == result_metadata.result_type


def test_add_result_invalid(store_handler):
    '''Test exceptions on the addition of invalid result parameters.'''
    store_handler._store.flushdb()
    with pytest.raises(ValueError):
        result_meta = ResultMetadata(1, 'Invalid result type')
    with pytest.raises(ValueError):
        result_meta = ResultMetadata('Hello', 'Invalid result type')
    with pytest.raises(ValueError):
        store_handler.add_result(None)
    with pytest.raises(ValueError):
        store_handler.add_result('Hello')


def test_get_result_status(store_handler, user_data):
    '''Test get result status.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    assert len(expected_results) > 0, 'No results to assess, please check test'
    for result_id in expected_results.keys():
        assert store_handler.get_result_status(result_id) == JobStatuses.QUEUED

    # Check invalid inputs raise an error
    with pytest.raises(ValueError):
        status = store_handler.get_result_status(0)
    with pytest.raises(ValueError):
        status = store_handler.get_result_status('Hello')


def test_set_result_status(store_handler, user_data):
    '''Test the modification of result status in the store.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    assert len(expected_results) > 0, 'No results to assess, please check test'
    result_id = list(expected_results.keys())[0]  # Randomly pick first result ID
    for status in JobStatuses:
        store_handler.set_result_status(result_id, JobStatuses.RUNNING)
        store_handler.get_result_status(result_id) == status

    # Invalid result id: not in store
    with pytest.raises(ValueError):
        store_handler.set_result_status(0, JobStatuses.COMPLETED)
    # Invalid result id: string
    with pytest.raises(ValueError):
        store_handler.set_result_status('Hello', JobStatuses.COMPLETED)
    # Invalid status: int instead of Enum
    with pytest.raises(ValueError):
        store_handler.set_result_status(9, 1)
    # Invalid status: string instead of Enum
    with pytest.raises(ValueError):
        store_handler.set_result_status(9, 'Hello')


class LogObject(object):
    '''A dummy user-defined job logs object.'''
    def __init__(self, base_str, num_logs):
        self._logs = ['{} #{:03d}'.format(base_str, i) for i in range(num_logs)]

    @property
    def logs(self):
        return self._logs

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.logs == other.logs
        else:
            return False


def test_job_logs(store_handler, user_data):
    '''Test the addition and retrieval of various types of job logs.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    assert len(expected_jobs) > 0, 'No jobs to assess, please check test'
    job_id = list(expected_jobs.keys())[0]
    # Create various log items to be tested
    logs = [
        'Test log 1',
        'Test log 2',
        '''Test longer
        log text.''',
        999.999,
        LogObject('Test log object', 10)
    ]
    # Add the logs
    for log in logs:
        store_handler.add_job_logs(job_id, log)
    # Validate order and values of retrieved logs
    retrieved = store_handler.get_job_logs(job_id)
    assert len(retrieved) == len(logs)
    for log_no, log in enumerate(logs):
        retrieved[log_no] == log


def test_result_logs(store_handler, user_data):
    '''Test the addition and retrieval of various types of job logs.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    assert len(expected_jobs) > 0, 'No jobs to assess, please check test'
    assert len(expected_results) > 0, 'No results to assess, please check test'
    result_id = list(expected_results.keys())[0]
    # Create various log items to be tested
    logs = [
        'Test log 1',
        'Test log 2',
        '''Test longer
        log text.''',
        999.999,
        LogObject('Test log object', 10)
    ]
    # Add the logs
    for log in logs:
        store_handler.add_result_logs(result_id, log)
    # Validate order and values of retrieved logs
    retrieved = store_handler.get_result_logs(result_id)
    assert len(retrieved) == len(logs)
    for log_no, log in enumerate(logs):
        retrieved[log_no] == log


def test_system_logs(store_handler, user_data):
    '''Test the addition and retrieval of various types of system logs.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    assert len(expected_jobs) > 0, 'No jobs to assess, please check test'
    job_id = list(expected_jobs.keys())[0]
    # Create various log items to be tested
    logs = [
        'Test log 1',
        'Test log 2',
        '''Test longer
        log text.''',
        999.999,
        LogObject('Test log object', 10)
    ]
    # Add the logs
    for log in logs:
        store_handler.add_system_logs(job_id, log)
    # Validate order and values of retrieved logs
    retrieved = store_handler.get_system_logs(job_id)
    assert len(retrieved) == len(logs)
    for log_no, log in enumerate(logs):
        retrieved[log_no] == log


def test_dependencies(store_handler, user_data):
    '''Test job dependencies addition and retrieval.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    # Create base job
    base_result_ids = []
    for result_no in range(3):
        base_result_metadata = ResultMetadata(ResultTypes.FILE,
                                              'Descriptor for base job-{:03d}'.format(result_no))
        base_result_id = store_handler.add_result(base_result_metadata)
        base_result_ids.append(base_result_id)
    # List of base result ids
    base_result_id = store_handler.add_result(base_result_ids)
    # Job only stores the list of results

    def base_function():
        return 'Base function'
    base_job_id = store_handler.add_job(FunctionTypes.PICKLED,
                                        base_function,
                                        'Data for base job',
                                        base_result_id)
    # Add dependencies
    store_handler.add_job_dependencies(base_job_id, list(expected_jobs.keys()), list(expected_results.keys()))

    assert store_handler.get_job_dependencies(base_job_id) == (
        list(expected_jobs.keys()),
        list(expected_results.keys())
    )


def test_get_job_progress(store_handler, user_data):
    '''Test the progress reporting of a job.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    # Create base job
    base_result_ids = []
    for result_no in range(3):
        base_result_metadata = ResultMetadata(ResultTypes.FILE,
                                              'Descriptor for base job-{:03d}'.format(result_no))
        base_result_id = store_handler.add_result(base_result_metadata)
        base_result_ids.append(base_result_id)
    # List of base result ids
    base_result_id = store_handler.add_result(base_result_ids)
    # Job only stores the list of results

    def base_function():
        return 'Base function'
    base_job_id = store_handler.add_job(FunctionTypes.PICKLED,
                                        base_function,
                                        'Data for base job',
                                        base_result_id)
    # Add dependencies
    store_handler.add_job_dependencies(base_job_id, list(expected_jobs.keys()))

    # We test using 4 random `expected_jobs` ids
    assert len(expected_jobs) > 3, 'Not enough jobs to assess, please check test'
    jids = list(expected_jobs.keys())[:4]

    # No progress yet
    assert store_handler.get_job_progress(base_job_id) == (0, len(expected_jobs))

    # 2 jobs completed
    store_handler.set_job_status(jids[0], JobStatuses.COMPLETED)
    store_handler.set_job_status(jids[3], JobStatuses.COMPLETED)
    assert store_handler.get_job_progress(base_job_id) == (2, len(expected_jobs))

    # All jobs completed
    for jid in expected_jobs.keys():
        store_handler.set_job_status(jid, JobStatuses.COMPLETED)
    assert store_handler.get_job_progress(base_job_id) == (len(expected_jobs), len(expected_jobs))

    # Invalid job id: not in store
    with pytest.raises(ValueError):
        store_handler.get_job_progress(0)
    # Invalid job id: string
    with pytest.raises(ValueError):
        store_handler.get_job_progress('Hello')


def test_get_result_progress(store_handler, user_data):
    '''Test the progress reporting of a job.'''
    store_handler._store.flushdb()
    expected_jobs = {}
    expected_results = {}
    for user_no, jobs in user_data.items():
        for job in jobs:
            result_ids = []
            for result in job['results']:
                # Individual result
                result_metadata = ResultMetadata(result['result_type'],
                                                 result['descriptor'])
                result_id = store_handler.add_result(result_metadata)
                result_ids.append(result_id)
                expected_results[result_id] = result_metadata
            # List of result ids
            result_id = store_handler.add_result(result_ids)
            expected_results[result_id] = result_ids
            # Job only stores the list of results
            job_id = store_handler.add_job(job['function_type'],
                                           job['function'],
                                           job['data'],
                                           result_id)
            expected_jobs[job_id] = {
                'function_type': job['function_type'],
                'function': job['function'],
                'data': job['data'],
                'result_id': result_id
            }

    # Create base job
    base_result_ids = []
    for result_no in range(3):
        base_result_metadata = ResultMetadata(ResultTypes.FILE,
                                              'Descriptor for base job-{:03d}'.format(result_no))
        base_result_id = store_handler.add_result(base_result_metadata)
        base_result_ids.append(base_result_id)
    # List of base result ids
    base_result_id = store_handler.add_result(base_result_ids)
    # Job only stores the list of results

    def base_function():
        return 'Base function'
    base_job_id = store_handler.add_job(FunctionTypes.PICKLED,
                                        base_function,
                                        'Data for base job',
                                        base_result_id)
    # Add dependencies
    store_handler.add_job_dependencies(base_job_id, list(expected_jobs.keys()))

    # We test using 4 random `expected_jobs` ids
    assert len(expected_jobs) > 3, 'Not enough jobs to assess, please check test'
    jids = list(expected_jobs.keys())[:4]

    # 1. No progress yet
    # Check base result id (a list of 3 result_ids)
    assert store_handler.get_result_progress(base_result_id) == (0, 3)
    # Check each individual result inside base result id
    for rid in base_result_ids:
        assert store_handler.get_result_progress(rid) == (0, 1)
    # Check all results in subjobs (toplevel list + each individual result each time)
    for result_id, rids in expected_results.items():
        if isinstance(rids, ResultMetadata):
            assert store_handler.get_result_progress(result_id) == (0, 1)
        else:
            assert store_handler.get_result_progress(result_id) == (0, len(rids))

    # 2. All individual subresults complete ==> all top level results also complete
    for rid in base_result_ids:
        store_handler.set_result_status(rid, JobStatuses.COMPLETED)
    for result_id, rids in expected_results.items():
        if isinstance(rids, ResultMetadata):
            store_handler.set_result_status(result_id, JobStatuses.COMPLETED)
    # Check base result id (a list of 3 result_ids)
    assert store_handler.get_result_progress(base_result_id) == (3, 3)
    # Check each individual result inside base result id
    for rid in base_result_ids:
        assert store_handler.get_result_progress(rid) == (1, 1)
    # Check all results in subjobs (toplevel list + each individual result each time)
    for result_id, rids in expected_results.items():
        if isinstance(rids, ResultMetadata):
            assert store_handler.get_result_progress(result_id) == (1, 1)
        else:
            assert store_handler.get_result_progress(result_id) == (len(rids), len(rids))

    # Invalid job id: not in store
    with pytest.raises(ValueError):
        store_handler.get_result_progress(0)
    # Invalid job id: string
    with pytest.raises(ValueError):
        store_handler.get_result_progress('Hello')
