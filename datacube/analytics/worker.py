'''Base class for the analytics and execution workers. It manages basic worker data and methods to
handle job and result lifecycle in the store.'''

from __future__ import absolute_import, print_function

import logging

from .utils.store_handler import JobStatuses, ResultMetadata

class Worker(object):
    def __init__(self, store, driver_manager, job):
        self._driver_manager = driver_manager
        self._store = store
        self._job = job
        self._job_id = self._job['id']
        self.logger = logging.getLogger('{}#{}'.format(self.__class__.__name__, self._job_id))

    def update_result_descriptor(self, descriptor, shape, dtype):
        # Update memory object
        #descriptor['shape'] = shape
        #if descriptor['chunk'] is None:
        #    descriptor['chunk'] = shape
        #descriptor['dtype'] = dtype
        # Update store result descriptor
        result_id = descriptor['id']
        result = self._store.get_result(result_id)
        if not isinstance(result, ResultMetadata):
            raise ValueError('Worker is trying to update a result list instead of metadata')
        if result.descriptor['base_name'] is None:
            result.descriptor['base_name'] = descriptor['base_name']
        result.descriptor['shape'] = shape
        if result.descriptor['chunk'] is None:
            result.descriptor['chunk'] = shape
        result.descriptor['dtype'] = dtype
        self._store.update_result(result_id, result)

    def job_starts(self):
        '''Set job to running status.'''
        # Start job
        self._store.set_job_status(self._job_id, JobStatuses.RUNNING)
        self.logger.debug('Job {:03d} ({}) is now {}'
                          .format(self._job_id, self.__class__.__name__,
                                  self._store.get_job_status(self._job_id).name))
        # Start combined result
        self.result_starts(self._job['result_id'])
        # Start individual results
        for descriptor in self._job['result_descriptors'].values():
            self.result_starts(descriptor['id'])

    def job_finishes(self):
        '''Set job to completed status.'''
        # Stop individual results
        for descriptor in self._job['result_descriptors'].values():
            self.result_finishes(descriptor['id'])
        # Stop combined result
        self.result_finishes(self._job['result_id'])
        # Stop job
        self._store.set_job_status(self._job_id, JobStatuses.COMPLETED)
        self.logger.debug('Job {:03d} ({}) is now {}'
                          .format(self._job_id, self.__class__.__name__,
                                  self._store.get_job_status(self._job_id).name))

    def result_starts(self, result_id):
        '''Set result to running status.'''
        self._store.set_result_status(result_id, JobStatuses.RUNNING)
        self.logger.debug('Result {:03d}-{:03d} ({}) is now {}'
                          .format(self._job_id, result_id, self.__class__.__name__,
                                  self._store.get_result_status(result_id).name))

    def result_finishes(self, result_id):
        '''Set result to completed status.'''
        self._store.set_result_status(result_id, JobStatuses.COMPLETED)
        self.logger.debug('Result {:03d}-{:03d} ({}) is now {}'
                          .format(self._job_id, result_id, self.__class__.__name__,
                                  self._store.get_result_status(result_id).name))
