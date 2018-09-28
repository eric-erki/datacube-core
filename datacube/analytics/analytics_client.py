'''The analytics client module lets end-users interact with the analytics/execution engine,
submitting jobs in a cluster and receiving job result objects in return.'''

from __future__ import absolute_import

import logging
from time import monotonic
from pathlib import Path
from uuid import uuid4

from datacube.engine_common.store_handler import StoreHandler
from datacube.engine_common.file_transfer import FileTransfer
from .job_result import JobResult, Job, Results
from .update_engine2 import UpdateActions
from datacube.config import LocalConfig

from datacube.engine_common.rpc_manager import get_rpc


class AnalyticsClient(object):
    '''Analytics client allowing interaction with the back-end engine
    through a RPC.
    '''

    def __init__(self, config):
        '''Initialise the client.

        :param LocalConfig config: The local config.
        '''
        self.logger = logging.getLogger(self.__class__.__name__)
        self._store = StoreHandler(**config.redis_config)
        self._use_s3 = config.execution_engine_config['use_s3']
        self._user_bucket = config.execution_engine_config['user_bucket']
        self._system_bucket = config.execution_engine_config['result_bucket']
        # Track urls of submitted jobs, to be able to delete payloads in S3
        self._urls = {}
        self._rpc = get_rpc(config)
        self.logger.debug('Ready')

    # pylint: disable=too-many-locals
    def submit_python_function(self, function, function_params=None, data=None,
                               user_tasks=None, walltime='00:00:30', check_period=1, paths=None, env=None,
                               tmpdir=None, *args, **kwargs):
        '''Submit a python function and data to the engine via the rpc.

        :param function function: Python function to be executed by the engine.
        :param dict function_params: Shared parameters that will be passed to the function.
          This should be kept small, large entries such as large files should be in s3 and the
          url be stored here.
        :param dict data: Dataset descriptor, a dictionary with keys `{'query', 'storage_params'}`
          whereby `query` is for input description, `storage_params` is for output description.
          Storage parameters, e.g. `{'chunk': (...), 'ttl': -1}` where
          `ttl` is the life span of the results and `chunk` the preferred result chunking.
        :param dict user_tasks: Local parameters that will be passed to the function.
           A job will be created for each (function, function_params, user task)
        :param int check_period: Period to check job completion status.
          JRO.update() will be called upon completion to update result metadata.
        :param str tmpdir: temporary dir only required for s3-file
          protocol. It will be created if not already existing.
        :param list args: Optional positional arguments for the function.
        :param dict kargs: Optional keyword arguments for the funtion.
        :return: Tuple of `(jro, results_promises)` where `result_promises` are promises of the
          subjob results.

        '''
        # Replace this by the incoming request ID
        unique_id = uuid4().hex
        start_time = monotonic()
        payload = {
            'id': unique_id,
            'type': 'base_job',
            'function': function,
            'function_params': function_params,
            'data': data,
            'user_tasks': user_tasks,
            'walltime': walltime,
            'check_period': check_period,
            'paths': paths,
            'env': env,
            'args': args,
            'kwargs': kwargs
        }
        if tmpdir:
            tmpdir = Path(tmpdir) / unique_id
        file_transfer = FileTransfer(base_dir=tmpdir, use_s3=self._use_s3,
                                     bucket=self._user_bucket, ids=[unique_id])
        url = file_transfer.store_payload(payload)
        try:
            analysis = self._rpc._run_python_function_base(url)
            jro = JobResult(*analysis, client=self, paths=paths, env=env)
            jro.checkForUpdate(check_period, start_time)
            self._urls[unique_id] = url
            return jro
        except Exception:
            # Delete payload from S3 on exception
            file_transfer.delete([url])
            raise

    def get_status(self, item, paths=None, env=None):
        '''Return the status of a job or result.'''
        if isinstance(item, Job):
            action = UpdateActions.GET_JOB_STATUS
        elif isinstance(item, Results):
            action = UpdateActions.GET_RESULT_STATUS
        else:
            raise ValueError('Can only return status of Job or Results')
        return self._rpc._get_update(action, item.id, paths, env)

    def update_jro(self, jro, paths=None, env=None):
        '''Update a JRO with all available result metadata.

        The metadata is collected from the store and used to update the jro's arrays using its
        `update_arrays()` method.
        '''
        results = self._rpc._get_update(UpdateActions.GET_ALL_RESULTS, jro.job.id, paths, env)
        jro.results.update_arrays(results)

    def get_result(self, result_id, paths=None, env=None):
        '''Return the metadata of a specific result.'''
        return self._rpc._get_update(UpdateActions.GET_RESULT, result_id, paths, env)

    def get_user_data(self, job_id, paths=None, env=None):
        '''Return the user_data of a job.'''
        return self._rpc._get_update(UpdateActions.GET_JOB_USER_DATA, job_id, paths, env)

    def __repr__(self):
        return 'Analytics client: {}'.format(self._rpc)

    def cleanup(self, jro):
        '''Clean up S3 payloads based on the jro data.

        Only the request ID is used to identify the payload to
        clean. Returns the number of objects deleted from S3.
        '''
        deleted = 0
        if jro.job.request_id in self._urls:
            url = self._urls[jro.job.request_id]
            file_transfer = FileTransfer(url=url)
            file_transfer.cleanup()
            deleted = file_transfer.delete([url])
            # TODO: For now, allow to delete from the system bucket
            #deleted += file_transfer.cleanup([self._system_bucket, self._user_bucket])
        return deleted
