'''Test the store workers management.
'''

from __future__ import absolute_import

from time import time
import pytest

import datacube.analytics.job_result
from datacube.engine_common.store_workers import StoreWorkers, WorkerMetadata, WorkerTypes, WorkerStatuses


@pytest.fixture(scope='module')
def store_workers(redis_config):
    '''Connect to the store and flushes the DB.

    The DB gets flushed again at the end of the tests.
    '''
    store_workers = StoreWorkers(**redis_config)
    yield store_workers
    store_workers._store.flushdb()


@pytest.fixture(scope='module')
def workers_metadata():
    workers = {}
    for worker_type in WorkerTypes:
        for worker_no in range(3):
            name = '{}-worker-{:03d}'.format(worker_type.name.lower(), worker_no)
            workers[name] = WorkerMetadata(name, worker_type, time())
    return workers


class LogObject(object):
    '''A dummy user-defined worker logs object.'''
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


def test_add_worker_invalid(store_workers):
    '''Test the addition of workers with invalid metadata.'''
    store_workers._store.flushdb()
    # Invalid job: not function (among others)
    with pytest.raises(ValueError):
        store_workers.add_worker(None)
    with pytest.raises(ValueError):
        store_workers.add_worker(0)
    with pytest.raises(ValueError):
        store_workers.add_worker('Hello')

    # Invalid worker type
    with pytest.raises(ValueError):
        meta = WorkerMetadata('worker1', None, None)
    with pytest.raises(ValueError):
        meta = WorkerMetadata('worker1', 0, None)


def test_add(store_workers, workers_metadata):
    '''Test the addition and status retrieval of workers.'''
    store_workers._store.flushdb()
    worker_ids = [store_workers.add_worker(metadata) for metadata in workers_metadata.values()]
    for worker_id in worker_ids:
        assert store_workers.get_worker_status(worker_id) == WorkerStatuses.ALIVE
    with pytest.raises(ValueError):
        store_workers.get_worker_status(0)


def test_count(store_workers, workers_metadata):
    '''Test the counting of workers with and without filter.'''
    store_workers._store.flushdb()
    worker_ids = [store_workers.add_worker(metadata) for metadata in workers_metadata.values()]
    assert store_workers.count_live_workers() == len(worker_ids)
    assert store_workers.count_live_workers(WorkerTypes.ANALYSIS) == 3
    with pytest.raises(ValueError):
        store_workers.count_live_workers(0)


def test_set_status(store_workers, workers_metadata):
    '''Test the setting of worker status.'''
    store_workers._store.flushdb()
    worker_ids = [store_workers.add_worker(metadata) for metadata in workers_metadata.values()]
    stopping = 3  # How many workers to stop
    assert len(worker_ids) >= stopping, 'Not enough workers to assess, please check test'
    for worker_id in worker_ids[:stopping]:
        if store_workers.get_worker_status(worker_id) == WorkerStatuses.ALIVE:
            store_workers.set_worker_status(worker_id, WorkerStatuses.STOPPED)
    assert store_workers.count_live_workers() == len(worker_ids) - stopping
    with pytest.raises(ValueError):
        store_workers.set_worker_status(0, WorkerStatuses.STOPPED)
    with pytest.raises(ValueError):
        store_workers.set_worker_status(worker_id, 0)


def test_worker_logs(store_workers, workers_metadata):
    '''Test the addition and retrieval of various types of worker logs.'''
    store_workers._store.flushdb()
    worker_ids = [store_workers.add_worker(metadata) for metadata in workers_metadata.values()]
    assert len(worker_ids) > 0, 'No workers to assess, please check test'
    worker_id = worker_ids[0]
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
        store_workers.add_worker_logs(worker_id, log)
    # Validate order and values of retrieved logs
    retrieved = store_workers.get_worker_logs(worker_id)
    assert len(retrieved) == len(logs)
    for log_no, log in enumerate(logs):
        retrieved[log_no] == log


def test_health(store_workers, workers_metadata):
    '''Test the workers' health management methods.'''
    store_workers._store.flushdb()
    worker_ids = [store_workers.add_worker(metadata) for metadata in workers_metadata.values()]
    assert len(worker_ids) > 2, 'Not enough workers to assess, please check test'
    # Set health
    health = {
        'memory_utilisation': {
            'min': 20.0,
            'max': 78.3
        }
    }
    store_workers.set_health(worker_ids[0], health)
    stored_health = store_workers.get_health(worker_ids[0])
    assert all(item in stored_health.items() for item in health.items())

    # Update health: new and updated fields
    health = {
        'heartbeat': {
            'timestamp': time()
        },
        'memory_utilisation': {
            'max': 33
        }
    }
    store_workers.set_health(worker_ids[0], health)
    stored_health = store_workers.get_health(worker_ids[0])
    assert stored_health['heartbeat'] == health['heartbeat']
    assert stored_health['memory_utilisation'] == {'max': 33.0, 'min': 20.0}

    # Invalid worker number
    with pytest.raises(ValueError):
        store_workers.set_health(0, health)

    # Invalid health data
    invalid_health = {
        'memory_utilisation': {
            'max': 33.3,
            'inval_param': 20.0,
            'inval_value': 'hello'
        },
        'inval_indicator': {
            'max': 50
        }
    }
    try:
        store_workers.set_health(worker_ids[0], invalid_health)
    except ValueError as error:
        error_str = str(error)
        assert 'Invalid memory_utilisation parameter: inval_param' in error_str
        assert 'Invalid memory_utilisation parameter: inval_value' in error_str
        assert 'Invalid health indicator: inval_indicator' in error_str

    # Non-live worker: check health is clean and cannot be set any more
    store_workers.set_worker_status(worker_ids[0], WorkerStatuses.STOPPED)
    with pytest.raises(ValueError):
        store_workers.get_health(worker_ids[0])
    with pytest.raises(ValueError):
        store_workers.set_health(worker_ids[0], health)
