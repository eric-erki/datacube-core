from __future__ import absolute_import, print_function

import logging
import time
from pprint import pformat
from six.moves import zip

from datacube import Datacube
from .base_worker import BaseWorker
from .utils.store_handler import FunctionTypes, ResultTypes, ResultMetadata, StoreHandler
from datacube.analytics.job_result import JobResult, LoadType


class AnalyticsEngineV2(object):
    DEFAULT_STORAGE = {
        'chunk': None,
        'ttl': -1
    }

    def __init__(self, store_config, driver_manager=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.store = StoreHandler(**store_config)
        self.driver_manager = driver_manager
        self.dc = Datacube(driver_manager=driver_manager)
        self.logger.debug('Ready')

    def analyse(self, function, data, storage_params=None, *args, **kwargs):
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

        storage = self.DEFAULT_STORAGE.copy()
        if storage_params:
            storage.update(storage_params)
        function_type = self._determine_function_type(function)
        decomposed = self._decompose(function_type, function, data, storage)
        self.logger.debug('Decomposed\n%s', pformat(decomposed, indent=4))

        # Run the base job
        base_worker = BaseWorker(self.store, self.driver_manager, decomposed)
        base_worker.job_starts()

        # TODO: remove this method once moved to celery
        base_worker.run_subjobs()

        # Create a thread to monitor job completion, until it gets implemented in the coming months.
        base_worker.monitor_completion()

        # TODO: Fix this !!! Adding a delay here makes things work !!!
        #time.sleep(5)

        return (decomposed['jobs'], self._create_jro(decomposed['base']))

    def _determine_function_type(self, func):
        '''Determine the type of a function.'''
        # Fixed for now. This may not be a simple thing to do safely (i.e. without executing the
        # code)
        return FunctionTypes.PICKLED

    def _decompose(self, function_type, function, data, storage_params):
        '''Decompose the function and data.

        The decomposition of function over data creates one or more jobs. Each job has its own
        decomposed function, and will create one or more results. This function returns a dictionary
        describing the base job and the list of decomposed jobs, each with their store ids as
        required.
        '''
        # == Mock implementation ==
        # Prepare the sub-jobs and base job info
        jobs = self._create_jobs(function, data, storage_params)
        base = self._create_base_job(function_type, function, data, storage_params, jobs)
        return {
            'base': base,
            'jobs': jobs
        }

    def _store_job(self, job, dependent_job_ids=None, dependent_result_ids=None):
        '''Store a job, its data, results and dependencies in the store.

        The job metadata passed as first parameter is updated in place, basically adding all item
        ids in the store.
        '''
        for field in ('function_type', 'function', 'data', 'result_descriptors'):
            if field not in job:
                raise ValueError('Missing "{}" in job description'.format(field))
        result_ids = []
        for descriptor in job['result_descriptors'].values():
            result_metadata = ResultMetadata(ResultTypes.FILE, descriptor)
            result_id = self.store.add_result(result_metadata)
            # Assign id to result descriptor dict (in place)
            descriptor['id'] = result_id
            descriptor['base_name'] = 'result_{:07d}'.format(result_id)
            result_ids.append(result_id)
        result_id = self.store.add_result(result_ids)
        job_id = self.store.add_job(job['function_type'],
                                    job['function'],
                                    job['data'],
                                    result_id)
        # Add dependencies
        self.store.add_job_dependencies(job_id, dependent_job_ids, dependent_result_ids)
        job.update({
            'id': job_id,
            'result_id': result_id,
        })

    def _create_base_job(self, function_type, function, data, storage_params, dependent_jobs):
        '''Prepare the base job.'''
        descriptors = self._create_result_descriptors(data['measurements'], storage_params['chunk'])
        job = {
            'function_type': function_type,
            'function': function,
            'data': data,
            'ttl': storage_params['ttl'],
            'chunk': storage_params['chunk'],
            'result_descriptors': descriptors
        }
        dependent_job_ids = [dep_job['id'] for dep_job in dependent_jobs]
        # Store and modify job in place to add store ids
        self._store_job(job, dependent_job_ids)
        return job

    def _create_jobs(self, function, data, storage_params):
        '''Decompose data and function into a list of jobs.'''
        job_data = self._decompose_data(data, storage_params)
        jobs = self._decompose_function(function, job_data, storage_params)
        for job in jobs:
            # Store and modify job in place to add store ids
            self._store_job(job)
        return jobs

    def _decompose_data(self, data, storage_params):
        '''Decompose data into a list of chunks.'''
        # == Partial implementation ==
        # TODO: Add a loop: for dataset in datasets...
        metadata = self.dc.metadata_for_load(**data)
        storage = self.dc.driver_manager.drivers['s3'].storage
        total_shape = metadata['grouped'].shape + metadata['geobox'].shape
        _, indices, chunk_ids = storage.create_indices(total_shape, storage_params['chunk'], '^_^')
        from copy import deepcopy
        decomposed_data = {}
        decomposed_data['query'] = deepcopy(data)
        # metadata should be part of decomposed_data so loading on the workers does not require a database connection
        # decomposed_data['metadata'] = metadata
        # fails pickling in python 2.7
        decomposed_data['indices'] = indices
        decomposed_data['chunk_ids'] = chunk_ids
        decomposed_data['total_shape'] = total_shape
        return decomposed_data

    def _decompose_function(self, function, data, storage_params):
        '''Decompose a function and data into a list of jobs.'''
        # == Mock implementation ==
        def decomposed_function(data):
            return data
        sub_jobs = []
        for chunk_id, s in zip(data['chunk_ids'], data['indices']):
            job = {
                'function_type': FunctionTypes.PICKLED,
                'function': function,
                'data': data,
                'slice': s,
                'chunk_id': chunk_id,
                'result_descriptors': self._create_result_descriptors(data['query']['measurements'],
                                                                      storage_params['chunk'])
            }
            sub_jobs.append(job)
        return sub_jobs

    def _create_result_descriptors(self, bands, chunk):
        '''Create mock result descriptors.'''
        # == Mock implementation ==
        descriptors = {}
        for band in bands:
            descriptors[band] = {
                'type': ResultTypes.FILE,
                'load_type': LoadType.EAGER,
                'base_name': None,  # Not yet known
                'bucket': 'eetest',
                'shape': None,  # Not yet known
                'chunk': chunk,
                'dtype': None  # Not yet known
            }
        return descriptors

    def _create_jro(self, job):
        '''Create the job result object for a base job.'''
        job_descriptor = {
            'id': job['id']
            }
        result_descriptor = {
            'id': job['result_id'],
            'results': job['result_descriptors']
        }
        return JobResult(job_descriptor, result_descriptor)
