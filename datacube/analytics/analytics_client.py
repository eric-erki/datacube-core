'''The analytics client module lets end-users interact with the analytics/execution engine,
submitting jobs in a cluster and receiving job result objects in return.'''

from __future__ import absolute_import

import logging
from celery import Celery
from cloudpickle import dumps

from .utils.store_handler import StoreHandler
from datacube.analytics.job_result import JobResult, Job, Results
from datacube.config import LocalConfig


def celery_app(store_config=None):
    if store_config is None:
        local_config = LocalConfig.find()
        store_config = local_config.redis_celery_config
    _app = Celery('ee_task', broker=store_config['url'], backend=store_config['url'])
    _app.conf.update(
        task_serializer='pickle',
        result_serializer='pickle',
        accept_content=['pickle'])
    return _app


# pylint: disable=invalid-name
app = celery_app()


class AnalyticsClient(object):
    '''Analytics client allowing interaction with the back-end engine through celery.

    TODO: For now, the jro updates are made through direct calls to redis rather than through
    celery., which will be implemented in the future.
    '''

    def __init__(self, config):
        '''Initialise the client.

        :param dict config: A dictionary containing the configuration parameters of `{'datacube':
          ..., 'store': ..., 'celery': ...}`
        '''
        self.logger = logging.getLogger(self.__class__.__name__)
        self._config = config
        self._store = StoreHandler(**config['store'])
        # global app
        app.conf.update(result_backend=config['celery']['url'], broker_url=config['celery']['url'])
        self.logger.debug('Ready')

    def submit_python_function(self, function, data, storage_params=None, *args, **kwargs):
        '''Submit a python function and data to the engine via celery.

        :param function function: Python function to be executed by the engine.
        :param dict data: Dataset descriptor.
        :param dict storage_params: Storage parameters, e.g. `{'chunk': (...), 'ttl': -1}` where
          `ttl` is the life span of the results and `chunk` the preferred result chunking.
        :param list args: Optional positional arguments for the function.
        :param dict kargs: Optional keyword arguments for the funtion.
        :return: Tuple of `(jro, results_promises)` where `result_promises` are promises of the
          subjob results.
        '''
        func = dumps(function)
        analysis_p = app.send_task('datacube.analytics.analytics_engine2.run_python_function_base',
                                   args=(self._config, func, data, storage_params), kwargs=kwargs)
        analysis = analysis_p.get(disable_sync_subtasks=False)
        jro = JobResult(*analysis[0], client=self)
        results = analysis[1]
        return (jro, results)

    def get_status(self, item):
        '''Return the status of a job or result.'''
        status = None
        if isinstance(item, Job):
            status = self._store.get_job_status(item.id)
        elif isinstance(item, Results):
            status = self._store.get_result_status(item.id)
        else:
            raise ValueError('Can only return status of Job or Results')
        return status

    def update_jro(self, jro):
        for dataset in jro.results.datasets:
            jro_result = jro.results.datasets[dataset]
            # pylint: disable=protected-access
            result = self._store.get_result(jro_result._id)
            jro_result.update(result.descriptor)
            # pylint: disable=protected-access
            self.logger.debug('Redis result id=%s (%s) updated, needs to be pushed into LazyArray: '
                              'shape=%s, dtype=%s',
                              jro_result._id, dataset,
                              result.descriptor['shape'], result.descriptor['dtype'])
