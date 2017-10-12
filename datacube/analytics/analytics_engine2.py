"""
Mock Analytics/Execution Engine Class for testing

"""

from __future__ import absolute_import, print_function

import logging
import time
from threading import Thread
from uuid import uuid4
import numpy as np
from pprint import pformat, pprint
from six.moves import zip

from datacube import Datacube
from .utils.store_handler import FunctionTypes, JobStatuses, ResultTypes, ResultMetadata, StoreHandler
from datacube.analytics.job_result import JobResult, LoadType
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO
from datacube.drivers.s3.storage.s3aio.s3io import S3IO


class AnalyticsEngineV2(object):

    def __init__(self, store_config, driver_manager=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.store = StoreHandler(**store_config)
        self.dc = Datacube(driver_manager=driver_manager)
        self.logger.debug('Ready')

    def submit_python_function(self, func, data, ttl=1, chunk=None, *args, **kwargs):
        '''
        user - job submit
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
        def job_starts(ae, job, job_type='subjob'):
            '''Set job and then all its results to running status.'''
            job_id = job['id']
            ae.store.set_job_status(job_id, JobStatuses.RUNNING)
            self.logger.debug('Job {:03d} ({}) is now {}'
                              .format(job_id, job_type, ae.store.get_job_status(job_id).name))
            time.sleep(0.15)
            for result_descriptor in job['result_descriptors'].values():
                result_id = result_descriptor['id']
                ae.store.set_result_status(result_id, JobStatuses.RUNNING)
                self.logger.debug('Result {:03d}-{:03d} ({}) is now {}'
                                  .format(job_id, result_id, job_type,
                                          ae.store.get_result_status(result_id).name))

        def job_finishes(ae, job, job_type='subjob'):
            '''Set all job results then the job itself to completed status.'''
            job_id = job['id']
            for result_descriptor in job['result_descriptors'].values():
                result_id = result_descriptor['id']
                ae.store.set_result_status(result_id, JobStatuses.COMPLETED)
                self.logger.debug('Result {:03d}-{:03d} ({}) is now {}'
                                  .format(job_id, result_id, job_type,
                                          ae.store.get_result_status(result_id).name))
            time.sleep(0.15)
            ae.store.set_job_status(job_id, JobStatuses.COMPLETED)
            self.logger.debug('Job {:03d} ({}) is now {}'
                              .format(job_id, job_type, ae.store.get_job_status(job_id).name))

        def fake_worker_thread(ae, job, driver_manager=None):
            '''Start the job, save results, then set it as completed.'''
            def _get_data(query, chunk=None, driver_manager=None):
                '''Retrieves data for worker'''
                dc = Datacube(driver_manager=driver_manager)
                if chunk is None:
                    return self.dc.load(use_threads=True, **query)
                else:
                    metadata = self.dc.metadata_for_load(**query)
                    return self.dc.load_data(metadata['grouped'], metadata['geobox'][chunk],
                                             metadata['measurements_values'].values(),
                                             driver_manager=dc.driver_manager, use_threads=True)

            def _save_array_in_s3(array, base_name, bucket, chunk_id, use_s3, driver_manager=None):
                '''Saves a single xarray.DataArray object to s3/s3-file storage'''
                s3_objects = []
                import zstd
                s3_key = '_'.join([base_name, array.name, str(chunk_id)])
                s3_object = {}
                s3_object['base_name'] = array.name
                s3_object['bucket'] = bucket
                s3_object['key'] = s3_key
                s3_object['shape'] = array.shape
                s3_object['dtype'] = array.dtype
                s3_objects.append(s3_object)
                print("Persisting to ", bucket, s3_key)
                data = bytes(array.data)
                cctx = zstd.ZstdCompressor(level=9, write_content_size=True)
                data = cctx.compress(data)
                s3io = S3IO(use_s3, None)
                s3io.put_bytes(bucket, s3_key, data)
                return s3_objects

            job_id = job['id']
            job_starts(ae, job)

            # Get data for worker here
            data = _get_data(job['data']['query'], job['slice'])

            # Execute function here

            # Save results here
            # map input to output
            # todo: pass in parameters from submit_python_function
            #       - storage parameters
            #       - naming parameters
            #       - unique key name
            for array_name in data.data_vars:
                _save_array_in_s3(data[array_name], array_name, 'ae_results', job['chunk_id'], False)

            job_finishes(ae, job)

        def wait_for_workers(store, decomposed):
            '''Base job only completes once all subjobs are complete.'''
            jobs_ready = False
            for tstep in range(30):  # max 10 checks with 0.5 sec delay
                all_statuses = []  # store all job and result statuses in this list
                for job in decomposed['jobs']:
                    try:
                        all_statuses.append(store.get_job_status(job['id']))
                    except ValueError as e:
                        pass
                    for result_descriptor in job['result_descriptors'].values():
                        try:
                            all_statuses.append(store.get_result_status(result_descriptor['id']))
                        except ValueError as e:
                            pass
                if any(js != JobStatuses.COMPLETED for js in all_statuses):
                    time.sleep(0.5)
                else:
                    jobs_ready = True
                    break
            if not jobs_ready:
                raise RuntimeError('Some subjobs did not complete')

        function_type = self._determine_function_type(func)
        decomposed = self._decompose(function_type, func, data, ttl, chunk)
        self.logger.debug('Decomposed\n%s', pformat(decomposed, indent=4))

        # Base job starts
        job_starts(self, decomposed['base'], 'base')

        # All subjobs run and complete in the background
        for job in decomposed['jobs']:
            Thread(target=fake_worker_thread, args=(self, job, self.dc.driver_manager)).start()

        # Base job waits for workers then finishes
        wait_for_workers(self.store, decomposed)
        job_finishes(self, decomposed['base'], 'base')

        return self._create_jro(decomposed['base'])

    def _determine_function_type(self, func):
        '''Determine the type of a function.'''
        # Fixed for now. This may not be a simple thing to do safely (i.e. without executing the
        # code)
        return FunctionTypes.PICKLED

    def _decompose(self, function_type, function, data, ttl=1, chunk=None):
        '''Decompose the function and data.

        The decomposition of function over data creates one or more jobs. Each job has its own
        decomposed function, and will create one or more results. This function returns a dictionary
        describing the base job and the list of decomposed jobs, each with their store ids as
        required.
        '''
        # == Mock implementation ==
        # Prepare the sub-jobs and base job info
        jobs = self._create_jobs(function, data, chunk)
        base = self._create_base_job(function_type, function, data, ttl, chunk, jobs)
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
            result_metadata = ResultMetadata(ResultTypes.S3IO, descriptor)
            result_id = self.store.add_result(result_metadata)
            # Assign id to result descriptor dict (in place)
            descriptor['id'] = result_id
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

    def _create_base_job(self, function_type, function, data, ttl, chunk, dependent_jobs):
        '''Prepare the base job.'''
        descriptors = self._create_result_descriptors(data['measurements'])
        job = {
            'function_type': function_type,
            'function': function,
            'data': data,
            'ttl': ttl,
            'chunk': chunk,
            'result_descriptors': descriptors
        }
        dependent_job_ids = [dep_job['id'] for dep_job in dependent_jobs]
        # Store and modify job in place to add store ids
        self._store_job(job, dependent_job_ids)
        return job

    def _create_jobs(self, function, data, chunk=None):
        '''Decompose data and function into a list of jobs.'''
        job_data = self._decompose_data(data, chunk)
        jobs = self._decompose_function(function, job_data)
        for job in jobs:
            # Store and modify job in place to add store ids
            self._store_job(job)
        return jobs

    def _decompose_data(self, data, chunk):
        '''Decompose data into a list of chunks.'''
        # == Partial implementation ==
        metadata = self.dc.metadata_for_load(**data)
        storage = self.dc.driver_manager.drivers['s3'].storage
        _, indices, chunk_ids = storage.create_indices(metadata['geobox'].shape, chunk, '^_^')
        from copy import deepcopy
        decomposed_data = {}
        decomposed_data['query'] = deepcopy(data)
        # metadata should be part of decomposed_data so loading on the workers does not require a database connection
        # decomposed_data['metadata'] = metadata
        # fails pickling in python 2.7
        decomposed_data['indices'] = indices
        decomposed_data['chunk_ids'] = chunk_ids
        return decomposed_data

    def _decompose_function(self, function, data):
        '''Decompose a function and data into a list of jobs.'''
        # == Mock implementation ==
        def decomposed_function(data):
            return data
        results = []
        for chunk_id, s in zip(data['chunk_ids'], data['indices']):
            result = {
                'function_type': FunctionTypes.PICKLED,
                'function': decomposed_function,
                'data': data,
                'slice': s,
                'chunk_id': chunk_id,
                'result_descriptors': self._create_result_descriptors(data['query']['measurements'])
            }
            results.append(result)
        return results

    def _save_results(self):
        # == Mock implementation ==
        s3lio = S3LIO(True, True, None, 30)

        red = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        blue = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4)) + 10
        s3lio.put_array_in_s3(red, (2, 2, 2), "jro_test_red", 'eetest')
        s3lio.put_array_in_s3(blue, (2, 2, 2), "jro_test_blue", 'eetest')

    def _create_result_descriptors(self, bands):
        '''Create mock result descriptors.'''
        # == Mock implementation ==
        descriptors = {}
        for band in bands:
            descriptors[band] = {
                'type': ResultTypes.S3IO,
                'load_type': LoadType.EAGER,
                'base_name': 'jro_test_{}'.format(band),
                'bucket': 'eetest',
                'shape': (4, 4, 4),
                'chunk': (2, 2, 2),
                'dtype': np.uint8
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
