from __future__ import absolute_import, print_function

import logging
import time
from math import ceil
from pprint import pformat
from six.moves import zip
from hashlib import sha512
from dill import dumps

from .worker import Worker
from datacube.engine_common.store_handler import FunctionTypes
from datacube.engine_common.store_workers import WorkerTypes
from datacube.engine_common.file_transfer import FileTransfer
from datacube.analytics.job_result import JobResult, LoadType
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO
from datacube.drivers.s3.storage.s3aio.s3io import S3IO


class AnalyticsEngineV2(Worker):
    """This class is responsible for receiving a job (function, data) from
    the user and decomposing it into sizeable sub-jobs.

    Sub jobs are submitted to the Execution Engine worker(s) to process.

    A sub-job is a partial (function, data) job.

    A Job Result Object is returned to the user as the main interface to
    the submitted job to retrieve job/result status and results.

    State & Health tracking are tracked via redis state/health store.
    """

    DEFAULT_STORAGE = {
        'chunk': None,
        'ttl': -1
    }

    def __init__(self, name, params_url):
        super().__init__(name, WorkerTypes.ANALYSIS, params_url)

    def analyse(self):
        '''user - job submit
        AE - gets the job
           - job decomposition
             - result ids
             - job id
             - create job metadata, result(s) metadata -> redis
                - sets state to queued.
             - JRO created and returned to user
             - sleep
             - sets state to running
             - sleep
             - set results state to queued
             - save 2 sets (red, blue) results to S3
             - sets result state to complete
             - sets job state to complete
        user - use JRO to query job/result
             - result array through array
             - job/result
               - status
        '''
        function = self._input_params['function']
        function_params = self._input_params['function_params']
        data = self._input_params['data'] if 'data' in self._input_params else None
        user_tasks = self._input_params['user_tasks'] if 'user_tasks' in self._input_params else None
        args = self._input_params['args'] if 'args' in self._input_params else None
        kwargs = self._input_params['kwargs'] if 'kwargs' in self._input_params else None

        if data and user_tasks:
            raise ValueError('Only `data` or `user_tasks` can be specified at once')

        query = data['query'] if data and 'query' in data else None

        # Decompose
        storage = self.DEFAULT_STORAGE.copy()
        if data and 'storage_params' in data:
            storage.update(data['storage_params'])
        function_type = self._determine_function_type(function)
        decomposed = self._decompose(function_type, function, query, function_params, storage, user_tasks)
        if logging.getLevelName(self.logger.getEffectiveLevel()) == 'DEBUG':
            self.logger.debug('Decomposed\n%s', pformat(decomposed, indent=4))

        # Run the base job
        self.job_starts(decomposed['base'])
        return (self._get_jro_params(decomposed['base']), decomposed)

    def _determine_function_type(self, func):
        '''Determine the type of a function.'''
        # Fixed for now. This may not be a simple thing to do safely (i.e. without executing the
        # code)
        return FunctionTypes.PICKLED

    def _decompose(self, function_type, function, data, function_params, storage_params, user_tasks):
        '''Decompose the function and data.

        The decomposition of function over data creates one or more jobs. Each job has its own
        decomposed function, and will create one or more results during execution. This function
        returns a dictionary describing the base job and the list of decomposed jobs, each with
        their ids as required.
        '''
        # Prepare the sub-jobs and base job info
        function_params_id = sha512(dumps(function_params)).hexdigest()
        # todo: reserve key in redis here
        jobs = self._create_jobs(function, data, function_params_id, storage_params, user_tasks)
        base = self._create_base_job(function_type, function, data, storage_params, user_tasks, jobs)
        return {
            'base': base,
            'jobs': jobs
        }

    def _store_job(self, job, dependent_job_ids=None):
        '''Store a job, its data and dependencies in the store.

        The job metadata passed as first parameter is updated in place, basically adding all item
        ids in the store.
        '''
        job_id = self._store.add_job(job['function_type'],
                                     job['function'],
                                     job['data'])
        # Add dependencies
        self._store.add_job_dependencies(job_id, dependent_job_ids)
        job.update({'id': job_id})

    def _create_base_job(self, function_type, function, data, storage_params, user_tasks, dependent_jobs):
        '''Prepare the base job.'''
        if data:
            job = {
                'function_type': function_type,
                'function': function,
                'user_tasks': user_tasks,
                'data': data,
                'ttl': storage_params['ttl'],
                'chunk': storage_params['chunk']
            }
        else:
            descriptors = {}
            job = {
                'function_type': function_type,
                'function': function,
                'user_tasks': user_tasks,
                'data': data,
                'ttl': None,
                'chunk': None
            }
        dependent_job_ids = [dep_job['id'] for dep_job in dependent_jobs]
        # Store and modify job in place to add store ids
        self._store_job(job, dependent_job_ids)
        return job

    def _create_jobs(self, function, data, function_params, storage_params, user_tasks):
        '''Decompose data and function into a list of jobs.'''
        jobs = self._decompose_function(function, data, function_params, storage_params, user_tasks)
        for job in jobs:
            # Store and modify job in place to add store ids
            self._store_job(job)
        return jobs

    def _decompose_data(self, data, storage_params):
        '''Decompose data into a list of chunks.'''
        # == Partial implementation ==
        # TODO: Add a loop: for dataset in datasets...
        from copy import deepcopy
        decomposed_data = {}

        for name, query in data.items():
            metadata = self._datacube.metadata_for_load(**query)
            total_shape = metadata['grouped'].shape + metadata['geobox'].shape
            _, indices, chunk_ids = S3LIO.create_indices(total_shape, storage_params['chunk'], '^_^')
            decomposed_item = {}
            decomposed_item['query'] = deepcopy(query)
            decomposed_item['metadata'] = metadata
            decomposed_item['indices'] = indices
            decomposed_item['chunk_ids'] = chunk_ids
            decomposed_item['total_shape'] = total_shape
            decomposed_data[name] = decomposed_item
        return decomposed_data

    def _decompose_function(self, function, data, function_params, storage_params, user_tasks):
        '''Decompose a function and data into a list of jobs.'''
        sub_jobs = []
        if user_tasks is not None:
            for user_task in user_tasks:
                job = {
                    'function_type': FunctionTypes.PICKLED,
                    'function': function,
                    'user_task': user_task,
                    'data': None,
                    'function_params': function_params,
                    'slice': None,
                    'chunk_id': None
                }
                sub_jobs.append(job)
        elif data is not None:
            job_data = self._decompose_data(data, storage_params)
            # TODO: fix this with delayed descriptors, this works because of common decomposition.
            first_query = job_data[sorted(job_data)[0]]

            positions = tuple(tuple(int(s.start / storage_params['chunk'][i])
                                    for i, s in enumerate(index))
                              for index in first_query['indices'])
            for chunk_id, s, position  in zip(first_query['chunk_ids'], first_query['indices'], positions):
                job = {
                    'function_type': FunctionTypes.PICKLED,
                    'function': function,
                    'user_tasks': None,
                    'data': job_data,
                    'function_params': function_params,
                    'slice': s,
                    'position': position,
                    'total_cells': max(positions),
                    'chunk_id': chunk_id,
                }
                sub_jobs.append(job)
        return sub_jobs

    def _get_jro_params(self, job):
        '''Create the parameters allowing to create a JobResult.'''
        job_descriptor = {
            'id': job['id']
            }
        result_descriptor = {
            'id': None,
            'results': {}
        }
        return (job_descriptor, result_descriptor)
