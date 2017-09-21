'''This module provides manages interaction with a redis store. This module provides manages
 interaction with a redis store.
'''
from __future__ import absolute_import

import logging
from enum import Enum, IntEnum
from collections import OrderedDict

from redis import StrictRedis, WatchError
from dill import loads, dumps


class FunctionTypes(Enum):
    '''Valid function types.'''
    PICKLED = 1
    TEXT = 2
    DSL = 3


class JobStatuses(IntEnum):
    '''Valid job/result statuses.

    IntEnum is used as the value gets stored in the stats hash.'''
    QUEUED = 1
    RUNNING = 2
    COMPLETED = 3
    CANCELLED = 4
    ERRORED = 5


class ResultTypes(Enum):
    '''Valid result types.'''
    INDEXED = 1
    FILE = 2
    S3IO = 3


class FunctionMetadata(object):
    '''Function and its metadata.'''
    def __init__(self, function_type, function):
        if function_type not in FunctionTypes:
            raise ValueError('Invalid function type: {}'.format(function_type))
        self.function_type = function_type
        self.function = function


class JobMetadata(object):
    '''Job metadata.'''
    def __init__(self, function_id, data_id, result_ids, ttl, chunk, timestamp):
        self.function_id = function_id
        self.data_id = data_id
        self.result_ids = result_ids
        self.ttl = ttl
        self.chunk = chunk
        self.timestamp = timestamp


class ResultMetadata(object):
    '''Result metadata.'''
    def __init__(self, result_type, descriptor):
        if result_type not in ResultTypes:
            raise ValueError('Invalid result type: {}'.format(result_type))
        self.result_type = result_type
        self.descriptor = descriptor


class KeyConcurrencyError(Exception):
    '''Error raised when a race condition arises when acccessing a key.'''
    pass


class StoreHandler(object):
    '''Main interface to the data store.'''

    DELIMITER = ':'
    K_FUNCTIONS = 'functions'
    K_JOBS = 'jobs'
    K_DATA = 'data'
    K_RESULTS = 'results'
    K_COUNT = 'total'
    K_STATS = 'stats'
    K_STATS_STATUS = 'status'
    # pylint: disable=not-an-iterable
    K_LISTS = {status: status.name.lower() for status in JobStatuses}
    '''Job lists.'''

    def __init__(self, **redis_config):
        '''Initialise the data store interface.'''
        self.logger = logging.getLogger(self.__class__.__name__)

        self._store = StrictRedis(**redis_config)

    def _make_key(self, *parts):
        '''Build a redis key using the agreed delimiter.

        All `parts` are expected to be strings as redis only understand strings.
        '''
        return self.DELIMITER.join(parts)

    def _make_list_key(self, key, status):
        '''Build a redis key for a specific item status list.'''
        return self._make_key(key, self.K_LISTS[status])

    def _make_stats_key(self, key, item_id):
        '''Build a redis key for a specific item stats.'''
        return self._make_key(key, self.K_STATS, item_id)

    def _add_item(self, key, item):
        '''Add an item to its queue list according to  its key type.

        The operations are not performed in a transaction because the initial `incr` is atomic and
        reserves an id, after which the order of the following operations is not important nor
        required to be atomic The only benefit of a transaction would be to improve performance,
        however it would imply `watch`-ing the item_id operation and re-trying until everything is
        atomic, which could actually result in a loss of performance.
        '''
        # Get new item id as incremental integer
        item_id = self._store.incr(self._make_key(key, self.K_COUNT))
        # Add pickled item to relevant list of items
        self._store.set(self._make_key(key, str(item_id)), dumps(item, byref=True))
        # Set jobs and results status in the stats
        if key in (self.K_JOBS, self.K_RESULTS):
            self._store.hset(self._make_stats_key(key, str(item_id)),
                             self.K_STATS_STATUS, JobStatuses.QUEUED.value)
            # For a job, also add it to the queued list
            if key == self.K_JOBS:
                self._store.rpush(self._make_list_key(key, JobStatuses.QUEUED), item_id)
        return item_id

    def _items_with_status(self, key, status):
        '''Returns the list of item ids currently in the queue, as integers.'''
        return [int(item_id) for item_id
                in self._store.lrange(self._make_list_key(key, status), 0, -1)]

    def _get_item(self, key, item_id):
        '''Retrieve a specific item by its key type and id.'''
        return loads(self._store.get(self._make_key(key, str(item_id))))

    def _get_item_status(self, key, item_id):
        '''Retrieve the status of an item.'''
        status = self._store.hget(self._make_stats_key(key, str(item_id)),
                                  self.K_STATS_STATUS)
        if not status:
            raise ValueError('Unknown id: {}'.format(item_id))
        return JobStatuses(int(status))

    def _set_item_status(self, key, item_id, new_status):
        '''Set the status of an item.'''
        # pylint: disable=unsupported-membership-test
        if new_status not in JobStatuses:
            raise ValueError('Invalid status: {}'.format(new_status))
        with self._store.pipeline() as pipe:
            stats_key = self._make_stats_key(key, str(item_id))
            try:
                pipe.watch(stats_key)
                old_status = self._store.hget(stats_key, self.K_STATS_STATUS)
                if not old_status:
                    raise ValueError('Unknown id: {}'.format(item_id))
                old_status = JobStatuses(int(old_status))
                pipe.multi()
                # For a job, also move it between lists
                if key == self.K_JOBS:
                    # Remove from current status list
                    pipe.lrem(self._make_list_key(self.K_JOBS, old_status), 0, item_id)
                    # Add to new list
                    pipe.rpush(self._make_list_key(self.K_JOBS, new_status), item_id)
                # Set status in stats
                pipe.hset(stats_key, self.K_STATS_STATUS, new_status.value)
                pipe.execute()
            except WatchError:
                raise KeyConcurrencyError('Status key modified by third-party while I was editing it')

    def add_job(self, function_type, function, data, result_ids, ttl=-1, chunk=None):
        '''Add an new function and its data to the queue.

        Both get serialised for storage. `data` is optional in case a job doesn't have any explicit
        data attached to it.
        '''
        if not function:
            raise ValueError('Cannot add job without function')
        func = FunctionMetadata(function_type, function)
        function_id = self._add_item(self.K_FUNCTIONS, func)
        data_id = self._add_item(self.K_DATA, data) if data else -1
        timestamp = None  # TODO: do we use redis or worker time?
        job = JobMetadata(function_id, data_id, result_ids, ttl, chunk, timestamp)
        return self._add_item(self.K_JOBS, job)

    def add_result(self, result):
        '''Add a (list of) result(s) to the store.

        Returns:
          The result id (or the list of result ids).
        '''
        if isinstance(result, ResultMetadata):
            return self._add_item(self.K_RESULTS, result)
        if not isinstance(result, (list, tuple)):
            raise ValueError('Invalid result type ({}). It must be ResultMetadata or list thereof.'
                             .format(type(result)))
        return [self.add_result(item) for item in result]

    def queued_jobs(self):
        '''List queued jobs.'''
        return self._items_with_status(self.K_JOBS, JobStatuses.QUEUED)

    def running_jobs(self):
        '''List running jobs.'''
        return self._items_with_status(self.K_JOBS, JobStatuses.RUNNING)

    def completed_jobs(self):
        '''List completed jobs.'''
        return self._items_with_status(self.K_JOBS, JobStatuses.COMPLETED)

    def cancelled_jobs(self):
        '''List cancelled jobs.'''
        return self._items_with_status(self.K_JOBS, JobStatuses.CANCELLED)

    def errored_jobs(self):
        '''List errored jobs.'''
        return self._items_with_status(self.K_JOBS, JobStatuses.ERRORED)

    def get_job(self, job_id):
        '''Retrieve a specific JobMetadata.'''
        return self._get_item(self.K_JOBS, job_id)

    def get_function(self, function_id):
        '''Retrieve a specific function.'''
        return self._get_item(self.K_FUNCTIONS, function_id)

    def get_data(self, data_id):
        '''Retrieve a specific data.'''
        return self._get_item(self.K_DATA, data_id)

    def get_result(self, result_id):
        '''Retrieve a specific result.'''
        return self._get_item(self.K_RESULTS, result_id)

    def get_job_status(self, job_id):
        '''Retrieve the status of a job.'''
        return self._get_item_status(self.K_JOBS, job_id)

    def set_job_status(self, job_id, status):
        '''Changes the status of a job.'''
        return self._set_item_status(self.K_JOBS, job_id, status)

    def get_result_status(self, result_id):
        '''Retrieve the status of a single result.'''
        return self._get_item_status(self.K_RESULTS, result_id)

    def set_result_status(self, result_id, status):
        '''Changes the status of a single result.'''
        return self._set_item_status(self.K_RESULTS, result_id, status)

    def __str__(self):
        '''Returns information about the store. For now, all its keys.'''
        return 'Store keys: {}'.format(
            sorted([key.decode('utf-8') for key in self._store.keys()]))
