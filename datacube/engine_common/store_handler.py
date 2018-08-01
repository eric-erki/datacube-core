'''This module provides manages interaction with a redis store. This module provides manages
 interaction with a redis store.

Being a rather low level API, very few checks are performed on values
for better performance. For example, user data can be added to a job
ID although there is no job with that ID.
'''
from __future__ import absolute_import

import logging
from time import time
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
    WALLTIME_EXCEEDED = 6


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
    def __init__(self, function_id, data_id, ttl, chunk, timestamp):
        self.function_id = function_id
        self.data_id = data_id
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


class LogObject(object):
    '''Timestamping logs.'''
    def __init__(self, message):
        self.message = message
        self.timestamp = time()


class KeyConcurrencyError(Exception):
    '''Error raised when a race condition arises when acccessing a key.'''
    pass

# pylint: disable=too-many-public-methods
class StoreHandler(object):
    '''Main interface to the data store.'''

    DELIMITER = ':'
    K_FUNCTIONS = 'functions'
    K_JOBS = 'jobs'
    K_DATA = 'data'
    K_RESULTS = 'results'
    K_SYSTEM = 'system'
    K_COUNT = 'total'
    K_DEPENDENCIES = 'dependencies'
    K_STATS = 'stats'
    K_LOGS = 'logs'
    K_STATS_STATUS = 'status'
    K_USER_DATA = 'userdata'
    K_FUNCTION_PARAM = 'functionparam'

    def __init__(self, **redis_config):
        '''Initialise the data store interface.'''
        self.logger = logging.getLogger(self.__class__.__name__)
        self._redis_config = redis_config
        self._store = StrictRedis(**redis_config, socket_keepalive=True, retry_on_timeout=True,
                                  socket_connect_timeout=10, socket_timeout=10)

    def reconnect(self):
        self._store = StrictRedis(**self._redis_config, socket_keepalive=True, retry_on_timeout=True,
                                  socket_connect_timeout=10, socket_timeout=10)

    def _make_key(self, *parts):
        '''Build a redis key using the agreed delimiter.

        All `parts` are expected to be strings as redis only understand strings.
        '''
        return self.DELIMITER.join(parts)

    def _make_list_key(self, key, status):
        '''Build a redis key for a specific item status list.'''
        return self._make_key(key, status.name.lower())

    def _make_stats_key(self, key, item_id):
        '''Build a redis key for a specific item stats.'''
        return self._make_key(key, self.K_STATS, item_id)

    def _make_logs_key(self, key, item_id):
        '''Build a redis key for a specific item logs.'''
        return self._make_key(key, self.K_LOGS, item_id)

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

    def _set_item(self, key, item_id, item):
        '''Set (or update) an item.'''
        # TODO: Compress any data if pickled size > some threshold
        self._store.set(self._make_key(key, str(item_id)), dumps(item, byref=True))

    def _update_item(self, key, item_id, item):
        '''Update an item, basically overwriting its existing value with a new one.

        The item id must exist prior.
        '''
        self._store.set(self._make_key(key, str(item_id)), dumps(item, byref=True), xx=True)

    def _items_with_status(self, key, status):
        '''Returns the list of item ids currently in the queue, as integers.'''
        return [int(item_id) for item_id
                in self._store.lrange(self._make_list_key(key, status), 0, -1)]

    def _get_item(self, key, item_id, allow_empty=False):
        '''Retrieve a specific item by its key type and id.'''
        key = self._make_key(key, str(item_id))
        value = self._store.get(key)
        if not value:
            if allow_empty:
                return value
            raise ValueError('Invalid key "{}" or missing data in store'.format(key))
        return loads(value)

    def _get_item_status(self, key, item_id):
        '''Retrieve the status of an item.'''
        status = self._store.hget(self._make_stats_key(key, str(item_id)),
                                  self.K_STATS_STATUS)
        if not status:
            raise ValueError('Unknown id: {}'.format(item_id))
        return status

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

    def _add_item_logs(self, key, item_id, logs):
        '''Add logs for a specific item.

        The new logs get pickled an appended to the current list of item logs.
        '''
        self._store.rpush(self._make_logs_key(key, str(item_id)),
                          dumps(LogObject(logs), byref=True))

    def _get_item_logs(self, key, item_id):
        '''Return the list of item logs.

        Each log may itself be a list or object.
        '''
        return [loads(log) for log in
                self._store.lrange(self._make_logs_key(key, str(item_id)), 0, -1)]

    def add_job(self, function_type, function, data, ttl=-1, chunk=None):
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
        job = JobMetadata(function_id, data_id, ttl, chunk, timestamp)
        return self._add_item(self.K_JOBS, job)

    def add_result(self, job_id, result):
        '''Add a result or list or result ids to a given job.

        Returns:
          The result id.
        '''
        if not isinstance(result, (ResultMetadata, list, tuple)):
            raise ValueError('Invalid result type ({}). It must be ResultMetadata or list of IDs.'
                             .format(type(result)))
        result_id = self._add_item(self.K_RESULTS, result)
        self._store.rpush(self._make_key(self.K_JOBS, self.K_RESULTS, str(job_id)),
                          result_id)
        return result_id

    def update_result(self, result_id, result):
        '''Update a result id in the store.'''
        if not isinstance(result, (ResultMetadata, list, tuple)):
            raise ValueError('Invalid result type ({}). It must be ResultMetadata or list of IDs.'
                             .format(type(result)))
        return self._update_item(self.K_RESULTS, result_id, result)

    def add_job_dependencies(self, job_id, dependent_job_ids=None, dependent_result_ids=None):
        '''Add a job dependencies, a pickled tuple of a list of jobs ids and a list of result ids.

        Any existing values get overwritten.
        '''
        # Add pickled item to relevant list of items
        self._store.set(self._make_key(self.K_JOBS, self.K_DEPENDENCIES, str(job_id)),
                        dumps((dependent_job_ids or None, dependent_result_ids or None),
                              byref=True))

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

    def get_job_results(self, job_id):
        '''Retrieve the results IDs for a specific job.'''
        return [int(result_id) for result_id in
                self._store.lrange(self._make_key(self.K_JOBS, self.K_RESULTS, str(job_id)), 0, -1)]

    def get_function(self, function_id):
        '''Retrieve a specific function.'''
        return self._get_item(self.K_FUNCTIONS, function_id)

    def get_data(self, data_id):
        '''Retrieve a specific data.'''
        return self._get_item(self.K_DATA, data_id)

    def get_result(self, result_id):
        '''Retrieve a specific result.'''
        return self._get_item(self.K_RESULTS, result_id)

    def get_results_for_job(self, job_id):
        '''Get all results for a job, including results of subjobs if applicable.'''
        # First check the job exists
        self.get_job(job_id)
        results = {}
        # Results for the main job_id
        for result_id in self.get_job_results(job_id):
            results[result_id] = self.get_result(result_id)
        # Fetch results for all potential dependencies
        try:
            for sub_job_id in self.get_job_dependencies(job_id)[0]:
                results.update(self.get_results_for_job(sub_job_id))
        except ValueError:  # No dependencies
            pass
        return results

    def get_job_status(self, job_id):
        '''Retrieve the status of a job.'''
        return JobStatuses(int(self._get_item_status(self.K_JOBS, job_id)))

    def set_job_status(self, job_id, status):
        '''Changes the status of a job.'''
        return self._set_item_status(self.K_JOBS, job_id, status)

    def get_job_dependencies(self, job_id):
        '''Retrieve job dependencies, a tuple of a list of jobs ids and a list of result ids.'''
        (jobs, results) = self._get_item(self._make_key(self.K_JOBS, self.K_DEPENDENCIES),
                                         job_id)
        return (jobs or [], results or [])

    def get_result_status(self, result_id):
        '''Retrieve the status of a single result.'''
        return JobStatuses(int(self._get_item_status(self.K_RESULTS, result_id)))

    def set_result_status(self, result_id, status):
        '''Changes the status of a single result.'''
        return self._set_item_status(self.K_RESULTS, result_id, status)

    def add_job_logs(self, job_id, logs):
        '''Add logs for a specific job.

        The new logs get pickled an appended to the current list of job logs.
        '''
        return self._add_item_logs(self.K_JOBS, job_id, logs)

    def get_job_logs(self, job_id):
        '''Return the list of job logs.

        Each log may itself be a list or object.
        '''
        return self._get_item_logs(self.K_JOBS, job_id)

    def add_result_logs(self, result_id, logs):
        '''Add logs for a specific result.

        The new logs get pickled an appended to the current list of result logs.
        '''
        return self._add_item_logs(self.K_RESULTS, result_id, logs)

    def get_result_logs(self, result_id):
        '''Return the list of result logs.

        Each log may itself be a list or object.
        '''
        return self._get_item_logs(self.K_RESULTS, result_id)

    def add_system_logs(self, job_id, logs):
        '''Add system logs for a specific job.

        The new logs get pickled an appended to the current list of system logs.
        '''
        return self._add_item_logs(self.K_SYSTEM, job_id, logs)

    def get_system_logs(self, job_id):
        '''Return the list of system logs.

        Each log may itself be a list or object.
        '''
        return self._get_item_logs(self.K_SYSTEM, job_id)

    def get_job_progress(self, job_id):
        '''Return the progress of a job.

        This returns a tuple (number_of_completed_dependent_jobs,
        total_number_of_dependent_jobs).
        '''
        jids = self.get_job_dependencies(job_id)[0]
        return (
            sum([self.get_job_status(jid) == JobStatuses.COMPLETED for jid in jids]),
            len(jids)
        )

    def get_result_progress(self, result_id):
        '''Return the progress of a result.

        If the result is a list of results, then this returns a tuple
        (number_of_completed_sub_results, total_number_of_sub_results). If the result is a
        ResultMetadata, then return (1 if completed else 0, 1).
        '''
        result = self.get_result(result_id)
        if isinstance(result, ResultMetadata):
            return (int(self.get_result_status(result_id) == JobStatuses.COMPLETED), 1)
        return (
            sum([self.get_result_status(rid) == JobStatuses.COMPLETED for rid in result]),
            len(result)
        )

    def set_user_data(self, job_id, user_data=None):
        '''Set or clear a job's user data, a dictionary of user-defined content.

        Any existing values get overwritten. If user_data is empty,
        then the key is cleared. Typically, this method is called by a
        subjob, and the data stored will be retrieved for all subjobs
        related to a base job.
        '''
        if user_data is None:
            self._store.delete(self._make_key(self.K_USER_DATA, str(job_id)))
        elif not isinstance(user_data, dict):
            raise ValueError('Invalid user data type ({}). It must be a dictionary.'
                             .format(type(user_data)))
        else:
            self._set_item(self.K_USER_DATA, job_id, user_data)

    def get_user_data(self, job_id):
        '''Get a job's user data, a list of dictionaries.

        Each subjob may store its own dictionary, and the list
        collates them all.
        '''
        # First check the job exists
        self.get_job(job_id)
        user_data = []
        # User data attached to job_id
        data = self._get_item(self.K_USER_DATA, job_id, allow_empty=True)
        if data:
            user_data.append(data)
        # User data attached to potential dependencies
        try:
            for sub_job_id in self.get_job_dependencies(job_id)[0]:
                data = self._get_item(self.K_USER_DATA, sub_job_id, allow_empty=True)
                if data:
                    user_data.append(data)
        except ValueError:
            pass
        return user_data

    def set_function_params(self, param_id, function_params=None):
        '''Set or clear a job's function params, a dictionary of user-defined content.

        Any existing values get overwritten. If user_data is empty,
        then the key is cleared. Typically, this method is called by a
        subjob, and the data stored will be retrieved for all subjobs
        related to a base job.
        '''
        if function_params is None:
            self._store.delete(self._make_key(self.K_FUNCTION_PARAM, str(param_id)))
        elif not isinstance(function_params, dict):
            raise ValueError('Invalid function params type ({}). It must be a dictionary.'
                             .format(type(function_params)))
        else:
            self._set_item(self.K_FUNCTION_PARAM, param_id, function_params)

    def get_function_params(self, param_id):
        '''Get a job's function params, a list of dictionaries.
        '''
        # First check the job exists
        # self.get_job(param_id)
        # User data attached to param_id
        return self._get_item(self.K_FUNCTION_PARAM, param_id, allow_empty=True)

    def __str__(self):
        '''Returns information about the store. For now, all its keys.'''
        return 'Store keys: {}'.format(
            sorted([key.decode('utf-8') for key in self._store.keys()]))

    def _decode_string(self, value, key=None):
        '''Helper function for str_dump. Do not call directly.'''
        try:
            value = value.decode('utf-8')
        except ValueError:
            value = loads(value)
            if hasattr(value, '__dict__'):
                value = '{}: {{{}}}'.format(
                    type(value).__name__,
                    ', '.join(['{}: {}'.format(k, v.__name__ if callable(v) else v)
                               for k, v in value.__dict__.items()]))
        if key:
            return '{}: {}'.format(key.decode('utf-8'), value)
        return str(value)

    def _decode_list(self, key, value):
        '''Helper function for str_dump. Do not call directly.'''
        return '{}: [{}]'.format(key.decode('utf-8'),
                                 ', '.join([self._decode_string(val) for val in value]))

    def str_dump(self):
        '''Return a simple string dump of the store, for debugging only.'''
        output = []
        for key in sorted(self._store.keys()):
            val_type = self._store.type(key)
            if val_type == b'string':
                output.append(self._decode_string(self._store.get(key), key)[:400])
            elif val_type == b'hash':
                output.append('{}:'.format(key.decode('utf-8')))
                for skey in self._store.hkeys(key):
                    output.append(' - {}'.format(self._decode_string(
                        self._store.hget(key, skey), skey)))
            elif val_type == b'list':
                output.append(self._decode_list(key, self._store.lrange(key, 0, -1)))
            else:
                output.append('{}: <{}>'.format(key, self._store.type(key)))
        return '\n'.join(output)
