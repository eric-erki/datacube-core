'''The analytics client module lets end-users interact with the analytics/execution engine,
submitting jobs in a cluster and receiving job result objects in return.'''

from __future__ import absolute_import

import logging
import time
from threading import Thread
from uuid import uuid4
import numpy as np
from pprint import pformat
from celery import Celery

from .analytics_engine2 import AnalyticsEngineV2
from .utils.store_handler import FunctionTypes, JobStatuses, ResultTypes, ResultMetadata, StoreHandler
from datacube.analytics.job_result import JobResult, Job, Results, LoadType
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO
from datacube.config import LocalConfig


def celery_app(store_config=None):

    if store_config is None:
        local_config = LocalConfig.find()
        store_config = local_config.redis_celery_config

    if 'password' in store_config:
        url = 'redis://{}:{}/{}'.format(store_config['host'], store_config['port'], store_config['db'])
    else:
        url = 'redis://:{}@{}:{}/{}'.format(store_config['password'], store_config['host'],
                                            store_config['port'], store_config['db'])

    _app = Celery('ee_task', broker=url, backend=url)

    _app.conf.update(
        task_serializer='pickle',
        result_serializer='pickle',
        accept_content=['pickle'])

    return _app


# pylint: disable=invalid-name
app = celery_app()


class AnalyticsClient(object):
    '''Analytics client allowing interaction with the back-end engine.

    For now, this is a mock implementation making direct calls to the engine and store handler. In
    the future, the calls should be made over the network, e.g. using celery or other
    communication/task management framework.
    '''

    def __init__(self, store_config, driver_manager=None):
        '''Initialise the client.

        :param dict store_config: A dictionary of store parameters, for the relevant type of store,
          e.g. redis.

        .. todo:: The final implementation should NOT have a store_config but instead a
        configuration allowing to send tasks to a remote engine.
        '''
        self.logger = logging.getLogger(self.__class__.__name__)
        self._engine = AnalyticsEngineV2(store_config, driver_manager=driver_manager)
        self.logger.debug('Ready')

    def submit_python_function(self, function, data, storage_params=None, *args, **kwargs):
        '''Submit a python function and data to the engine.

        :param function function: Python function to be executed by the engine.
        :param dict data: Dataset descriptor.
        :param dict storage_params: Storage parameters, e.g. `{'chunk': (...), 'ttl': -1}` where
          `ttl` is the life span of the results and `chunk` the preferred result chunking.
        :param list args: Optional positional arguments for the function.
        :param dict kargs: Optional keyword arguments for the funtion.
        :return: A :class:`JobResult` object.

        '''
        jro = self._engine.submit_python_function(function, data, storage_params, *args, **kwargs)
        jro.client = self
        return jro

    def submit_python_function_base(self, func, data, storage_params=None, *args, **kwargs):
        from cloudpickle import dumps
        func = dumps(func)
        return app.send_task('datacube.analytics.analytics_engine2.run_python_function_base',
                             args=(func, data, storage_params), kwargs=kwargs)

    def get_status(self, item):
        '''Return the status of a job or result.'''
        status = None
        if isinstance(item, Job):
            status = self._engine.store.get_job_status(item.id)
        elif isinstance(item, Results):
            status = self._engine.store.get_result_status(item.id)
        else:
            raise ValueError('Can only return status of Job or Results')
        return status

    def update_jro(self, jro):
        for dataset in jro.results.datasets:
            jro_result = jro.results.datasets[dataset]
            # pylint: disable=protected-access
            result = self._engine.store.get_result(jro_result._id)
            jro_result.update(result.descriptor)
            # pylint: disable=protected-access
            self.logger.debug('Redis result id=%s (%s) updated, needs to be pushed into LazyArray: '
                              'shape=%s, dtype=%s',
                              jro_result._id, dataset,
                              result.descriptor['shape'], result.descriptor['dtype'])
