'''This module includes store (redis) methods to manage workers health.
'''
from __future__ import absolute_import

import logging
from enum import Enum, IntEnum

from redis import WatchError

from .store_handler import StoreHandler, KeyConcurrencyError


class WorkerTypes(Enum):
    '''Valid worker types.'''
    ANALYSIS = 1
    EXECUTION = 2
    MONITOR = 3
    OTHER = 4

class WorkerStatuses(IntEnum):
    '''Valid worker statuses.

    IntEnum is used as the value gets stored in the stats hash.

    TODO: These values are placeholders for now. Actual names should be defined in view of the
    workers lifecycle design.
    '''
    PENDING = 1
    ALIVE = 2
    STOPPED = 3
    ERRORED = 4


class WorkerMetadata(object):
    '''Worker metadata.'''
    def __init__(self, name, worker_type, creation_time):
        if worker_type not in WorkerTypes:
            raise ValueError('Invalid worker type: {}'.format(worker_type))
        self.name = name
        self.worker_type = worker_type
        self.creation_time = creation_time


class StoreWorkers(StoreHandler):
    '''Interface to manage workers.'''

    K_WORKERS = 'workers'
    K_HEALTH = 'health'
    K_MEMORY = 'memory_utilisation'
    HEALTH_INDICATORS = {
        "heartbeat": ("timestamp"),
        "memory_utilisation": ("min", "max", "avg", "peak1", "peak5", "peak10"),
        "cpu_utilisation":  ("min", "max", "avg", "peak1", "peak5", "peak10"),
        "queue_length":  ("min", "max", "avg", "peak1", "peak5", "peak10"),
        "compute_time_per_task":  ("min", "max", "avg", "peak1", "peak5", "peak10"),
        "compute_throughput":  ("min", "max", "avg", "peak1", "peak5", "peak10"),
        "network_throughput":  ("min", "max", "avg", "peak1", "peak5", "peak10"),
        "total_throughput": ("compute", "network")
    }

    def add_worker(self, worker_metadata, worker_status=WorkerStatuses.ALIVE):
        '''Add a worker to the store.

        Returns:
          The worker id.
        '''
        if not isinstance(worker_metadata, WorkerMetadata):
            raise ValueError('Invalid worker metadata ({}).'.format(type(worker_metadata)))
        if not isinstance(worker_status, WorkerStatuses):
            raise ValueError('Invalid worker status ({}).'.format(type(worker_status)))
        worker_id = self._add_item(self.K_WORKERS, worker_metadata)
        # Set status in the stats
        self._store.hset(self._make_stats_key(self.K_WORKERS, str(worker_id)),
                         self.K_STATS_STATUS, worker_status.value)
        # If alive, add to list of alive workers for fast retrieval
        if worker_status == WorkerStatuses.ALIVE:
            self._store.rpush(self._make_list_key(self.K_WORKERS, worker_status), worker_id)
        return worker_id

    def get_worker(self, worker_id):
        '''Retrieve a specific WorkerMetadata.'''
        return self._get_item(self.K_WORKERS, worker_id)

    def get_worker_status(self, worker_id):
        '''Retrieve the status of a single worker.'''
        return WorkerStatuses(int(self._get_item_status(self.K_WORKERS, worker_id)))

    def set_worker_status(self, worker_id, status):
        '''Changes the status of a worker.'''
        # pylint: disable=unsupported-membership-test
        if status not in WorkerStatuses:
            raise ValueError('Invalid status: {}'.format(status))
        with self._store.pipeline() as pipe:
            stats_key = self._make_stats_key(self.K_WORKERS, str(worker_id))
            try:
                pipe.watch(stats_key)
                old_status = self._store.hget(stats_key, self.K_STATS_STATUS)
                if not old_status:
                    raise ValueError('Unknown id: {}'.format(worker_id))
                old_status = WorkerStatuses(int(old_status))
                if status == old_status:
                    return
                pipe.multi()
                # If moving to/from ALIVE, move between lists
                if old_status == WorkerStatuses.ALIVE:
                    # Remove from current status list
                    pipe.lrem(self._make_list_key(self.K_WORKERS, old_status), 0, worker_id)
                    # Clean all health records
                    self._clean_health(worker_id)
                elif status == WorkerStatuses.ALIVE:
                    # Add to alive list
                    pipe.rpush(self._make_list_key(self.K_WORKERS, status), worker_id)
                # Set status in stats
                pipe.hset(stats_key, self.K_STATS_STATUS, status.value)
                pipe.execute()
            except WatchError:
                raise KeyConcurrencyError('Status key modified by third-party while I was editing it')

    def count_live_workers(self, worker_type=None):
        '''Count live workers, optionally filtering by type.

        Filtering is costly as it needs to pull all workers to check their type.
        '''
        if worker_type is None:
            return self._store.llen(self._make_list_key(self.K_WORKERS, WorkerStatuses.ALIVE))
        if worker_type not in WorkerTypes:
            raise ValueError('Invalid worker type: {}'.format(worker_type))
        count = 0
        for worker_id in self._items_with_status(self.K_WORKERS, WorkerStatuses.ALIVE):
            worker = self.get_worker(worker_id)
            if worker.worker_type == worker_type:
                count += 1
        return count

    def add_worker_logs(self, worker_id, logs):
        '''Add logs for a specific worker.

        The new logs get pickled an appended to the current list of worker logs.
        '''
        return self._add_item_logs(self.K_WORKERS, worker_id, logs)

    def get_worker_logs(self, worker_id):
        '''Return the list of worker logs.

        Each log may itself be a list or object.
        '''
        return self._get_item_logs(self.K_WORKERS, worker_id)

    def _clean_health(self, worker_id):
        '''Remove all health records for a worker. To be used when the worker stops.'''
        for indicator in self.HEALTH_INDICATORS:
            self._store.delete(self._make_key(self.K_WORKERS, self.K_HEALTH, indicator, str(worker_id)))


    def set_health(self, worker_id, health):
        if self.get_worker_status(worker_id) != WorkerStatuses.ALIVE:
            raise ValueError('Cannot set health for non-existing or non-alive worker.')
        valid_health = {}
        errors = []
        for indicator, i_data in health.items():
            if indicator not in self.HEALTH_INDICATORS:
                errors.append('Invalid health indicator: {}'.format(indicator))
                continue
            valid_params = self.HEALTH_INDICATORS[indicator]
            items = {}
            for param, data in i_data.items():
                if param not in valid_params:
                    errors.append('Invalid {} parameter: {}'.format(indicator, param))
                    continue
                if not isinstance(data, (int, float)):
                    errors.append('Non-numeric {}:{} value: {}'.format(indicator, param, data))
                    continue
                items[param] = data
            if items:
                valid_health[indicator] = items
        if errors:
            raise ValueError('Invalid health data: \n - {}'.format('\n - '.join(errors)))
        for indicator, data in valid_health.items():
            self._store.hmset(self._make_key(self.K_WORKERS, self.K_HEALTH, indicator, str(worker_id)),
                              data)

    def get_health(self, worker_id, indicator=None):
        if self.get_worker_status(worker_id) != WorkerStatuses.ALIVE:
            raise ValueError('Cannot get health for non-existing or non-alive worker.')
        if indicator and indicator not in self.HEALTH_INDICATORS:
            raise ValueError('Invalid health indicator: {}'.format(indicator))
        indicators = [indicator] if indicator else list(self.HEALTH_INDICATORS.keys())
        health = {}
        for indic in indicators:
            health[indic] = {param.decode('utf-8'): float(val) for param, val in self._store.hgetall(
                self._make_key(self.K_WORKERS, self.K_HEALTH, indic, str(worker_id))).items()}
        return health
