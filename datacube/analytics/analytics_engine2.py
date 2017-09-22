"""
Mock Analytics/Execution Engine Class for testing

"""

from __future__ import absolute_import

import logging
import time
from threading import Thread
from uuid import uuid4
import numpy as np

from .utils.store_handler import FunctionTypes, JobStatuses, ResultTypes, ResultMetadata, StoreHandler
from datacube.analytics.job_result import JobResult, LoadType
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO


class AnalyticsEngineV2(object):

    def __init__(self, store_config):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.store = StoreHandler(**store_config)
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
        def fake_worker_thread(ae, job):
            job_id = job['id']
            time.sleep(0.15)
            ae.store.set_job_status(job_id, JobStatuses.RUNNING)
            self.logger.debug('Job {:03d} is now {}'
                              .format(job_id, ae.store.get_job_status(job_id).name))
            time.sleep(0.15)
            ae.store.set_job_status(job_id, JobStatuses.COMPLETED)
            self.logger.debug('Job {:03d} is now {}'
                              .format(job_id, ae.store.get_job_status(job_id).name))

            for result_id in job['result_ids']:
                ae.store.set_result_status(result_id, JobStatuses.RUNNING)
                self.logger.debug('Result {:03d}-{:03d} is now {}'
                                  .format(job_id, result_id,
                                          ae.store.get_result_status(result_id).name))
                # TODO: Put this back in. For now it crashes in tests because of AWS credentials
                # ae._save_results()
                ae.store.set_result_status(result_id, JobStatuses.COMPLETED)
                self.logger.debug('Result {:03d}-{:03d} is now {}'
                                  .format(job_id, result_id,
                                          ae.store.get_result_status(result_id).name))

        function_type = self._determine_function_type(func)
        decomposed = self._decomposition(function_type, func, data, ttl=1, chunk=None)
        jros = []
        for job in decomposed['jobs']:
            Thread(target=fake_worker_thread, args=(self, job)).start()
            jros.append(self._create_jro(job))
        return jros

    def _determine_function_type(self, func):
        '''Determine the type of a function.'''
        # Fixed for now. This may not be a simple thing to do safely (i.e. without executing the
        # code)
        return FunctionTypes.PICKLED

    def _decomposition(self, function_type, function, data, ttl=1, chunk=None):
        '''Decompose the function and data into jobs and results.

        The decomposition of function over data create one or more jobs. Each job has its own
        decomposed function (it may be the same as the original function for now) and will create
        one or more results.
        '''
        # == Mock implementation ==
        decomposed = {}
        decomposed['base'] = {}
        decomposed['base']['function_type'] = function_type
        decomposed['base']['function'] = function
        decomposed['base']['data'] = data
        decomposed['base']['ttl'] = ttl
        decomposed['base']['chunk'] = chunk
        decomposed['jobs'] = []

        # Assuming actual task decomposition, we fake 2 jobs would be created from the original
        # function below (outside for loop)
        for job_no in range(2):
            decomposed_function_type = FunctionTypes.PICKLED
            def decomposed_function(job_no=job_no):
                return 'Function of job {:03d}'.format(job_no)

            decomposed_data = 'Data of job {:03d}'.format(job_no)

            results = []
            # If each job can be parallelised (or produce several sequential results?), then how
            # many results is each job going to produce. They will all share the same function and
            # data. For different function or data, create new jobs instead.
            # Here we allow: 1 job <=> 1 data <=> 1+ result
            for result_no in range(2):
                result_descriptor = 'Result {:03d} of job {:03d}'.format(result_no, job_no)
                results.append(ResultMetadata(ResultTypes.S3IO, result_descriptor))
            # Add (empty) results to store, mark as queued
            decomposed_result_ids = self.store.add_result(results)

            # Add job to store, mark as queued
            job_id = self.store.add_job(decomposed_function_type,
                                        decomposed_function,
                                        decomposed_data,
                                        decomposed_result_ids)
            # All that is needed for workers to do the job TODO: Discuss how workers will know that
            # a job+data should lead to more than 1 result id. Potentially, fix results to a single
            # result at this level, i.e. 1 job <=> 1 data <=> 1 result
            decomposed['jobs'].append({
                'id': job_id,
                'function_type': decomposed_function_type,
                'function': decomposed_function,
                'data': decomposed_data,
                'result_ids': decomposed_result_ids
            })
        return decomposed

    def _save_results(self):
        s3lio = S3LIO(True, True, None, 30)

        red = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        blue = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4)) + 10
        s3lio.put_array_in_s3(red, (2, 2, 2), "jro_test_red", 'eetest')
        s3lio.put_array_in_s3(blue, (2, 2, 2), "jro_test_blue", 'eetest')

    def _create_result_descriptor(self, result_ids):
        # == Mock implementation ==
        bands = ('blue', 'red', 'green')
        descriptors = {}
        for result_no, result_id in enumerate(result_ids):
            band = bands[result_no % len(bands)]
            descriptors[band] = {
                'id': result_id,
                'type': ResultTypes.S3IO,
                'load_type': LoadType.EAGER,
                'base_name': 'jro_test_{}'.format(band),
                'bucket': 'eetest',
                'shape': (4, 4, 4),
                'chunk': (2, 2, 2),
                'dtype': np.uint8
            }
        return {
            # TODO: there is no store id associated with the "list" of results, only one id per
            # actual result. For now I add a uuid, but I am not sure whether we need any id at all
            'id': uuid4(),
            'results': descriptors
        }

    def _create_jro(self, job):
        job_descriptor = {'id': job['id']}
        results_descriptor = self._create_result_descriptor(job['result_ids'])
        return JobResult(job_descriptor, results_descriptor)
