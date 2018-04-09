'''The analytics client module lets end-users interact with the analytics/execution engine,
submitting jobs in a cluster and receiving job result objects in return.'''

from __future__ import absolute_import

import os
import zlib
import zstd
import logging
from celery import Celery
from dill import dumps

from datacube.engine_common.store_handler import StoreHandler
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
        # global app
        app.conf.update(result_backend=config.redis_celery_config['url'],
                        broker_url=config.redis_celery_config['url'])
        self.logger.debug('Ready')

    def update_config(self, config):
        config_p = app.send_task('datacube.analytics.analytics_worker.update_config', args=(config, ))
        config_p.get(disable_sync_subtasks=False)

    # pylint: disable=too-many-locals
    def submit_python_function(self, function, data, function_params=None, storage_params=None, *args, **kwargs):
        '''Submit a python function and data to the engine via celery.

        :param function function: Python function to be executed by the engine.
        :param dict data: Dataset descriptor.
        :param dict function_params: Parameters that will be passed to the function.
          This should be kept small, large entries such as large files should be in s3 and the
          url be stored here.
        :param dict storage_params: Storage parameters, e.g. `{'chunk': (...), 'ttl': -1}` where
          `ttl` is the life span of the results and `chunk` the preferred result chunking.
        :param list args: Optional positional arguments for the function.
        :param dict kargs: Optional keyword arguments for the funtion.
        :return: Tuple of `(jro, results_promises)` where `result_promises` are promises of the
          subjob results.
        '''
        func = dumps(function)
        cctx = zstd.ZstdCompressor(level=9, write_content_size=True)
        func = cctx.compress(func)

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
                        # _data = zlib.compress(f, 1)
                        _data = cctx.compress(f)
                    function_params[key] = {'fname': fname, 'data': _data, 'copy_to_input_dir': True}

        analysis_p = app.send_task('datacube.analytics.analytics_worker.run_python_function_base',
                                   args=(func, data, function_params, storage_params), kwargs=kwargs)
        analysis = analysis_p.get(disable_sync_subtasks=False)
        jro = JobResult(*analysis[0], client=self)
        results = analysis[1]
        return (jro, results)

    def get_status(self, item):
        '''Return the status of a job or result.'''
        if isinstance(item, Job):
            action = UpdateActions.GET_JOB_STATUS
        elif isinstance(item, Results):
            action = UpdateActions.GET_RESULT_STATUS
        else:
            raise ValueError('Can only return status of Job or Results')
        status_p = app.send_task('datacube.analytics.analytics_worker.get_update',
                                 args=(action, item.id))
        status = status_p.get(disable_sync_subtasks=False)
        return status

    def update_jro(self, jro):
        for dataset in jro.results.datasets:
            jro_result = jro.results.datasets[dataset]
            result_p = app.send_task('datacube.analytics.analytics_worker.get_update',
                                     args=(UpdateActions.GET_RESULT, jro_result.id))
            result = result_p.get(disable_sync_subtasks=False)
            jro_result.update(result.descriptor)
            self.logger.debug('Redis result id=%s (%s) updated, needs to be pushed into LazyArray: '
                              'shape=%s, dtype=%s',
                              jro_result.id, dataset,
                              result.descriptor['shape'], result.descriptor['dtype'])


    def get_user_data(self, job_id):
        '''Return the user_data of a job.'''
        # Minimal check: job ID must be an int
        if not isinstance(job_id, int):
            raise ValueError('Can only return user_data for a valid job id')
        action = UpdateActions.GET_JOB_USER_DATA
        user_data_p = app.send_task('datacube.analytics.analytics_worker.get_update',
                                    args=(action, job_id))
        user_data = user_data_p.get(disable_sync_subtasks=False)
        return user_data
