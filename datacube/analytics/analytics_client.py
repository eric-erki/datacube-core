'''The analytics client module lets end-users interact with the analytics/execution engine,
submitting jobs in a cluster and receiving job result objects in return.'''

from __future__ import absolute_import

import os
import logging
from time import monotonic
from celery import Celery

from datacube.engine_common.store_handler import StoreHandler
from datacube.engine_common.file_transfer import FileTransfer
from .job_result import JobResult, Job, Results
from .update_engine2 import UpdateActions
from datacube.config import LocalConfig
from datacube.compat import urlparse

def celery_app(store_config=None):
    try:
        if store_config is None:
            local_config = LocalConfig.find()
            store_config = local_config.redis_celery_config
        _app = Celery('ee_task', broker=store_config['url'], backend=store_config['url'])
    except ValueError:
        _app = Celery('ee_task')

    _app.conf.update(
        task_serializer='pickle',
        result_serializer='pickle',
        accept_content=['pickle'],
        worker_prefetch_multiplier=1)
    return _app


# pylint: disable=invalid-name
app = celery_app()


class AnalyticsClient(object):
    '''Analytics client allowing interaction with the back-end engine through celery.'''

    def __init__(self, config):
        '''Initialise the client.

        :param LocalConfig config: The local config.
        '''
        self.logger = logging.getLogger(self.__class__.__name__)
        self._store = StoreHandler(**config.redis_config)
        self._file_transfer = FileTransfer()
        # global app
        app.conf.update(result_backend=config.redis_celery_config['url'],
                        broker_url=config.redis_celery_config['url'])
        self.logger.debug('Ready')

    # pylint: disable=too-many-locals
    def submit_python_function(self, function, function_params=None, data=None,
                               user_tasks=None, walltime='00:00:30', check_period=1, paths=None, env=None,
                               output_dir=None, *args, **kwargs):
        '''Submit a python function and data to the engine via celery.

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
        :param list args: Optional positional arguments for the function.
        :param dict kargs: Optional keyword arguments for the funtion.
        :return: Tuple of `(jro, results_promises)` where `result_promises` are promises of the
          subjob results.
        '''
        func = self._file_transfer.serialise(function)
        # compress files in function_params
        if function_params:
            for key, value in function_params.items():
                if not isinstance(value, str):
                    continue
                url = urlparse(value)
                if url.scheme == 'file':
                    fname = os.path.basename(url.path)
                    with open(url.path, "rb") as _data:
                        f = _data.read()
                        _data = self._file_transfer.compress(f)
                    function_params[key] = {'fname': fname, 'data': _data, 'copy_to_input_dir': True}
        analysis = self._run_python_function_base(func, function_params, data, user_tasks,
                                                  walltime, paths, env, output_dir, **kwargs)
        jro = JobResult(*analysis, client=self, paths=paths, env=env)
        jro.checkForUpdate(check_period, monotonic())
        return jro


    def _run_python_function_base(self, *args, **kwargs):
        '''Run the function using celery.

        This is placed in a separate method so it can be overridden
        during tests.
        '''
        analysis_p = app.send_task('datacube.analytics.analytics_worker.run_python_function_base',
                                   args=args, kwargs=kwargs)
        return analysis_p.get(disable_sync_subtasks=False)

    def _get_update(self, action, item_id, paths=None, env=None):
        '''Remotely invoke the `analytics_worker.get_update()` method.'''
        # Minimal check: item ID must be an int
        if not isinstance(item_id, int):
            raise ValueError('Invalid job or result id: {}'.format(item_id))
        data_p = app.send_task('datacube.analytics.analytics_worker.get_update',
                               args=(action, item_id, paths, env))
        return data_p.get(disable_sync_subtasks=False)

    def get_status(self, item, paths=None, env=None):
        '''Return the status of a job or result.'''
        if isinstance(item, Job):
            action = UpdateActions.GET_JOB_STATUS
        elif isinstance(item, Results):
            action = UpdateActions.GET_RESULT_STATUS
        else:
            raise ValueError('Can only return status of Job or Results')
        return self._get_update(action, item.id, paths, env)

    def update_jro(self, jro, paths=None, env=None):
        '''Update a JRO with all available result metadata.

        The metadata is collected from the store and used to update the jro's arrays using its
        `update_arrays()` method.
        '''
        results = self._get_update(UpdateActions.GET_ALL_RESULTS, jro.job.id, paths, env)
        jro.results.update_arrays(results)

    def get_result(self, result_id, paths=None, env=None):
        '''Return the metadata of a specific result.'''
        return self._get_update(UpdateActions.GET_RESULT, result_id, paths, env)

    def get_user_data(self, job_id, paths=None, env=None):
        '''Return the user_data of a job.'''
        return self._get_update(UpdateActions.GET_JOB_USER_DATA, job_id, paths, env)

    def __repr__(self):
        active = app.control.inspect().active()
        workers = [task['name'].split('.')[-1] for task in active.popitem()[1]] if active else []
        return 'Analytics client: {} active workers: {}'.format(len(workers), workers)
