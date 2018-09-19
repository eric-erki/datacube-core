from __future__ import absolute_import, print_function

from time import sleep, monotonic
from threading import Thread

from .worker import Worker
from datacube.engine_common.store_workers import WorkerTypes
from datacube.engine_common.store_handler import JobStatuses


class BaseJobMonitor(Worker):
    """A temporary class monitoring the completion of a job.
    This functionality will reside in the AE worker who will periodically
    perform monitoring at the proper times.
    """

    def __init__(self, name, decomposed, subjob_tasks, params_url):
        super(BaseJobMonitor, self).__init__(name, WorkerTypes.MONITOR, params_url)
        self._decomposed = decomposed
        self._walltime = self._input_params['walltime']
        self._walltime_in_secs = self._walltime_to_sec(self._walltime)
        self._subjob_tasks = subjob_tasks
        self._start_time = monotonic()

    def _walltime_to_sec(self, walltime):
        hours, minutes, seconds = walltime.split(':')
        return int(hours) * 3600 + int(minutes) * 60 + int(seconds)

    def wait_for_workers(self):
        '''Base job only completes once all subjobs are complete.'''
        jobs_completed = False
        walltime_exceeded = False

        while True:
            elapsed_time = monotonic() - self._start_time
            if elapsed_time > self._walltime_in_secs:
                walltime_exceeded = True
                break
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
                self.logger.info('Time elapsed %d seconds', elapsed_time)
                jobs_completed = True
                break

        if jobs_completed:
            return JobStatuses.COMPLETED

        if walltime_exceeded:
            self.logger.info('Walltime %s exceeded! Killing all sub-jobs', self._walltime)
            self.kill_subjobs()
            return JobStatuses.WALLTIME_EXCEEDED

        return JobStatuses.ERRORED

    def monitor_completion(self):
        '''Track the completion of subjobs.

        Wait for subjobs to complete then update result descriptors.
        '''
        job_status = self.wait_for_workers()
        self.job_finishes(self._decomposed['base'], job_status)
        self.logger.info('Base job monitor completed')

    def kill_subjobs(self):
        for job in self._subjob_tasks:
            job.revoke(terminate=True)
