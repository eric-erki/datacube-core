'''This module provides manages interaction with a redis store. This module provides manages
 interaction with a redis store.
'''
from __future__ import absolute_import

import logging
from enum import Enum
from collections import OrderedDict

from redis import StrictRedis
from dill import loads, dumps


class FunctionTypes(Enum):
    '''Valid function types.'''
    PICKLED = 1
    TEXT = 2
    DSL = 3


class JobStatuses(Enum):
    '''Valid job statuses.'''
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
            raise ValueError('Invalid function type: %s', function_type)
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
            raise ValueError('Invalid result type: %s', result_type)
        self.result_type = result_type
        self.descriptor = descriptor


class StoreHandler(object):
    '''Main interface to the data store.'''

    DELIMITER = ':'
    K_FUNCTIONS = 'functions'
    K_JOBS = 'jobs'
    K_DATA = 'data'
    K_RESULTS = 'results'
    K_COUNT = 'total'

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

    def _add_item(self, key, item):
        '''Add an item to its queue list according to  its key type.

        TODO (talk to Peter): Decide whether to use a MULTI/EXEC block. I don't think it is required
        as the incr operation reserves an id, so it doesn't matter if the following operations are
        done in the same transaction.
        Use it for better performance: it should use a single packet.
        '''
        # Get new item id as incremental integer
        item_id = self._store.incr(self._make_key(key, self.K_COUNT))

        # Add pickled item to relevant list of items
        self._store.set(self._make_key(key, str(item_id)), dumps(item, byref=True))

        if key == self.K_JOBS:
            # Add job id to queued list of items
            self._store.rpush(self._make_list_key(key, JobStatuses.QUEUED), item_id)

        return item_id


    def _items_with_status(self, key, status):
        '''Returns the list of item ids currently in the queue, as integers.'''
        return [int(item_id) for item_id
                in self._store.lrange(self._make_list_key(key, status), 0, -1)]

    def _get_item(self, key, item_id):
        '''Retrieve a specific item by its key type and id.'''
        return loads(self._store.get(self._make_key(key, str(item_id))))

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


    def set_job_status(self, job_id, new_status):
        '''Move a job status from QUEUED to any other status.'''
        if new_status not in JobStatuses:
            raise ValueError('Invalid job status: %s', new_status)
        # Find job in all existing status lists
        old_status = None
        # Python2 Enums don't respect order, hence the following iteration may not be in optimal
        # order. Python3 will iterate in the order of the JobStatuses elements.
        for status in JobStatuses:
            if job_id in self._items_with_status(self.K_JOBS, status):
                old_status = status
                break
        if not old_status:
            raise ValueError('Unknown job id: %s', job_id)
        # Remove from current status list
        self._store.lrem(self._make_list_key(self.K_JOBS, old_status), 0, job_id)
        # Add to new list
        self._store.rpush(self._make_list_key(self.K_JOBS, new_status), job_id)

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

    def __str__(self):
        '''Returns information about the store. For now, all its keys.'''
        return 'Store keys: {}'.format(
            sorted([key.decode('utf-8') for key in self._store.keys()]))
