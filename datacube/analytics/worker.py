'''Base class for the analytics and execution workers. It manages basic worker data and methods to
handle job and result lifecycle in the store.'''

from __future__ import absolute_import, print_function

from time import time
import logging

from datacube import Datacube
from datacube.engine_common.store_handler import JobStatuses, ResultMetadata
from datacube.engine_common.store_workers import StoreWorkers, WorkerMetadata, WorkerStatuses
from datacube.config import LocalConfig


class Worker(object):
    def __init__(self, name, worker_type, paths=None, env=None, output_dir=None):
        self.logger = logging.getLogger(name)
        self._output_dir = output_dir  # Base dir for data storage for when s3-file is used
        config = LocalConfig.find(paths, env) if paths and env else LocalConfig.find()
        self._datacube = Datacube(config=config)
        self._store = StoreWorkers(**config.redis_config)
        self._ee_config = config.execution_engine_config
        self._id = self._store.add_worker(WorkerMetadata(name, worker_type, time()),
                                          WorkerStatuses.ALIVE)

    def job_starts(self, job):
        '''Set job to running status.'''
        job_id = job['id']
        # Start job
        self._store.set_job_status(job_id, JobStatuses.RUNNING)
        message = 'Job {:03d} ({}) is now {}'.format(job_id, self.__class__.__name__,
                                                     self._store.get_job_status(job_id).name)
        self._store.add_worker_logs(self._id, message)
        self.logger.debug(message)

    def job_finishes(self, job):
        '''Set job to completed status.'''
        job_id = job['id']
        # Stop job
        self._store.set_job_status(job_id, JobStatuses.COMPLETED)
        message = 'Job {:03d} ({}) is now {}'.format(job_id, self.__class__.__name__,
                                                     self._store.get_job_status(job_id).name)
        self._store.add_worker_logs(self._id, message)
        self.logger.debug(message)
