'''Base class for the analytics and execution workers. It manages basic worker data and methods to
handle job and result lifecycle in the store.'''

from __future__ import absolute_import, print_function

from time import time
import logging
from pathlib import Path
from urllib.parse import urlparse
from dill import loads

from datacube import Datacube
from datacube.engine_common.store_handler import JobStatuses, ResultMetadata
from datacube.engine_common.store_workers import StoreWorkers, WorkerMetadata, WorkerStatuses
from datacube.config import LocalConfig
from datacube.engine_common.file_transfer import FileTransfer
from datacube.drivers.s3.storage.s3aio.s3io import S3IO


class Worker(object):
    def __init__(self, name, worker_type, params_url):
        self.logger = logging.getLogger(name)
        parsed = urlparse(params_url)
        self._use_s3 = parsed.scheme == 's3'
        self._tmpdir = None
        path = parsed.path
        if not self._use_s3:
            # Double shash expected, else an exception will raise
            tmpdir, path = path.split('//')
            self._tmpdir = Path(tmpdir)
            # FileTransfer base dir should be one level down from the S3 subdir
            if self._tmpdir.stem == FileTransfer.S3_DIR:
                self._tmpdir = self._tmpdir.parent
            path = Path(path)
            user_bucket = path.parts[0]
            path = path.relative_to(user_bucket)
        else:
            user_bucket = parsed.netloc
            path = Path(path.lstrip('/'))
        self._request_id = path.parts[0]
        self._file_transfer = FileTransfer(self._tmpdir, self._use_s3)

        # Fetch config from S3
        s3io = S3IO(self._use_s3, str(self._file_transfer.s3_dir))
        self._input_params = loads(s3io.get_bytes(user_bucket, str(path)))
        # Initialise datacube
        if 'paths' in self._input_params and 'env' in self._input_params:
            config = LocalConfig.find(self._input_params['paths'], self._input_params['env'])
        else:
            config = LocalConfig.find()
        self._datacube = Datacube(config=config)
        self._store = StoreWorkers(**config.redis_config)
        self._ee_config = config.execution_engine_config
        self._result_bucket = self._ee_config['result_bucket']
        self._id = self._store.add_worker(WorkerMetadata(name, worker_type, time()),
                                          WorkerStatuses.ALIVE)

        message = '{}: Initialised'.format(self)
        self._store.add_worker_logs(self._id, message)
        self.logger.debug(message)

    def job_starts(self, job):
        '''Set job to running status.'''
        job_id = job['id']
        # Start job
        self._store.set_job_status(job_id, JobStatuses.RUNNING)
        message = '{}: job {:03d} is now RUNNING'.format(self, job_id)
        self._store.add_worker_logs(self._id, message)
        self.logger.debug(message)

    def job_finishes(self, job, job_status):
        '''Set job to actual status.'''
        job_id = job['id']
        # Stop job
        self._store.set_job_status(job_id, job_status)
        message = '{}: job {:03d} is now {}'.format(self, job_id,
                                                    self._store.get_job_status(job_id).name)
        self._store.add_worker_logs(self._id, message)
        self.logger.debug(message)

    def __repr__(self):
        return 'Worker {:03d} ({})'.format(self._id, self.__class__.__name__)
