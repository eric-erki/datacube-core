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

    def __init__(self, name, config, decomposed):
        super(BaseJobMonitor, self).__init__(name, WorkerTypes.MONITOR, config)
        self._decomposed = decomposed

    def wait_for_workers(self):
        '''Base job only completes once all subjobs are complete.'''
        jobs_ready = False
        for tstep in range(150):  # Cap the number of checks
            all_statuses = []  # store all job and result statuses in this list
            for job in self._decomposed['jobs']:
                try:
                    all_statuses.append(self._store.get_job_status(job['id']))
                except ValueError as e:
                    pass
                for result_descriptor in job['result_descriptors'].values():
                    try:
                        all_statuses.append(self._store.get_result_status(result_descriptor['id']))
                    except ValueError as e:
                        pass
            if any(js != JobStatuses.COMPLETED for js in all_statuses):
                sleep(0.5)
            else:
                jobs_ready = True
                break
        if not jobs_ready:
            raise RuntimeError('Some subjobs did not complete')

    def monitor_completion(self):
        '''Track the completion of subjobs.

        Wait for subjobs to complete then update result descriptors.
        '''
        self.wait_for_workers()

        # Get first worker job and copy properties from it to base job
        job0 = self._decomposed['jobs'][0]

        # TODO: Fix this with delayed descriptors, use total_shape from first query.
        if job0['data']:
            total_shape = job0['data'][sorted(job0['data'])[0]]['total_shape']
        else:
            total_shape = None

        # TODO: this way of getting base result shape will not work if job data decomposed into
        # smaller chunks
        for array_name, result_descriptor in self._decomposed['base']['result_descriptors'].items():
            # Use the dtype from the first sub-job as dtype for the base result, for that aray_name
            sub_result_id = job0['result_descriptors'][array_name]['id']
            dtype = self._store.get_result(sub_result_id).descriptor['dtype']
            self.update_result_descriptor(result_descriptor,
                                          total_shape,
                                          dtype)
        self.job_finishes(self._decomposed['base'])
