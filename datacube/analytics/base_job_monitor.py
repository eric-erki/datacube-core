from __future__ import absolute_import, print_function

from time import sleep
from threading import Thread

from .worker import Worker
from datacube.engine_common.store_workers import WorkerTypes
from datacube.engine_common.store_handler import JobStatuses


class BaseJobMonitor(Worker):
    """A temporary class monitoring the completion of a job.
    This functionality will reside in the AE worker who will periodically
    perform monitoring at the proper times.
    """

    def __init__(self, name, decomposed, paths=None, env=None, output_dir=None):
        super(BaseJobMonitor, self).__init__(name, WorkerTypes.MONITOR, paths, env, output_dir)
        self._decomposed = decomposed

    def wait_for_workers(self):
        '''Base job only completes once all subjobs are complete.'''
        jobs_ready = False
        for tstep in range(50):  # Cap the number of checks
            all_statuses = []  # store all job and result statuses in this list
            for job in self._decomposed['jobs']:
                try:
                    all_statuses.append(self._store.get_job_status(job['id']))
                except ValueError as e:
                    pass
            if any(js != JobStatuses.COMPLETED for js in all_statuses):
                self.logger.info('Waiting... %s', all_statuses)
                sleep(0.5)
            else:
                self.logger.info('All subjobs completed! %s', all_statuses)
                jobs_ready = True
                break
        if not jobs_ready:
            raise RuntimeError('Some subjobs did not complete')

    def monitor_completion(self):
        '''Track the completion of subjobs.

        Wait for subjobs to complete then update result descriptors.
        '''
        self.wait_for_workers()
        self.job_finishes(self._decomposed['base'])
        self.logger.info('Base job monitor completed')
