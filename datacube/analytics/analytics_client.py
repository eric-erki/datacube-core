'''The analytics client module lets end-users interact with the analytics/execution engine,
submitting jobs in a cluster and receiving job result objects in return.'''

from __future__ import absolute_import

import logging
import time
from threading import Thread
from uuid import uuid4
import numpy as np
from pprint import pformat

from .analytics_engine2 import AnalyticsEngineV2
from .utils.store_handler import FunctionTypes, JobStatuses, ResultTypes, ResultMetadata, StoreHandler
from datacube.analytics.job_result import JobResult, Job, Results, LoadType
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO


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

    def submit_python_function(self, function, data, ttl=1, chunk=None, *args, **kwargs):
        '''Submit a python function and data to the engine.

        :param function function: Python function to be executed by the engine.
        :param dict data: Dataset descriptor.
        :param int ttl: Life span of the results.
        :param slice chunk: Preferred result chunking.
        :param list args: Optional positional arguments for the function.
        :param dict kargs: Optional keyword arguments for the funtion.
        :return: A :class:`JobResult` object.
        '''
        jro = self._engine.submit_python_function(function, data, ttl, chunk, *args, **kwargs)
        jro.client = self
        return jro

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
