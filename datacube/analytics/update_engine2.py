'''The update engine responds to queries from client regarding the status or value of existing job
and results. It is typically used by the JRO to update itself.
'''
from __future__ import absolute_import, print_function

import logging
from enum import Enum

from datacube.engine_common.store_handler import StoreHandler
from datacube.config import LocalConfig


class UpdateActions(Enum):
    '''Valid actions for the AnalyticsUpdater.'''
    GET_JOB_STATUS = 1
    GET_RESULT_STATUS = 2
    GET_RESULT = 3
    GET_ALL_RESULTS = 4
    GET_JOB_USER_DATA = 5


class UpdateEngineV2(object):

    def __init__(self, paths, env):
        '''Initialise the update engine.'''
        # For now, this is not inheriting Worker since it would create a Datacube object and offer
        # methods that may not get used
        self.logger = logging.getLogger(self.__class__.__name__)
        config = LocalConfig.find(paths, env) if paths and env else LocalConfig.find()
        self._store = StoreHandler(**config.redis_config)
        self.logger.debug('Ready')

    def execute(self, action, item_id):
        '''Execute an action returning some update.'''
        if action not in UpdateActions:
            raise ValueError('Invalid action: {}'.format(action))
        result = None
        if action == UpdateActions.GET_JOB_STATUS:
            result = self._store.get_job_status(item_id)
        elif action == UpdateActions.GET_RESULT_STATUS:
            result = self._store.get_result_status(item_id)
        elif action == UpdateActions.GET_RESULT:
            result = self._store.get_result(item_id)
        elif action == UpdateActions.GET_ALL_RESULTS:
            result = self._store.get_results_for_job(item_id)
        elif action == UpdateActions.GET_JOB_USER_DATA:
            result = self._store.get_user_data(item_id)
        return result
