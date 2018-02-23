from __future__ import absolute_import, print_function

from sys import version_info
import numpy as np
import zstd
from cloudpickle import loads

from datacube.engine_common.store_workers import WorkerTypes
from datacube.analytics.worker import Worker
from datacube.drivers.s3.storage.s3aio.s3io import S3IO


class ExecutionEngineV2(Worker):
    """This class is responsible for receiving decomposed jobs (function, data)
    from the the AE via redis and decomposing it further into sizeable compute jobs
    for the target compute node.
        - if job from AE is too 'big'
          - decompose into smaller jobs
          - submit to EE cluster
        - if job from AE/EE is not too big
          - execute
          - save
        - merge results to match expected shape & chunk size of AE job.

    Sub jobs are submitted to the Execution Engine worker(s) to process.

    Results are persisted to either S3, S3-file or NetCDF.

    A sub-job is a partial (function, data) job.

    State & Health tracking are tracked via redis state/health store.
    """

    def __init__(self, name, config=None):
        super(ExecutionEngineV2, self).__init__(name, WorkerTypes.EXECUTION, config)

    def _analyse(self, function, data, storage_params, *args, **kwargs):
        """stub to call _decompose to perform data decomposition if required"""
        pass

    def _decompose(self, function_type, function, data, storage_params):
        """further data decomposition to suit compute node"""
        pass

    def _get_data(self, query, chunk=None):
        '''Retrieves data for the worker.'''
        if chunk is None:
            return self._datacube.load(use_threads=True, **query)
        else:
            metadata = self._datacube.metadata_for_load(**query)
            return self._datacube.load_data(metadata['grouped'][chunk[0]], metadata['geobox'][chunk[1:]],
                                            metadata['measurements_values'].values(), use_threads=True)

    def _compute_result(self, function, data):
        '''Run the function on the data.'''
        # TODO: restore function according to its type
        func = loads(function)
        return func(data)

    def _save_array_in_s3(self, array, result_descriptor, chunk_id, use_s3=False):
        '''Saves a single xarray.DataArray object to s3/s3-file storage'''
        s3_key = '_'.join([result_descriptor['base_name'], str(chunk_id)])
        self.logger.debug('Persisting computed result to %s-%s',
                          result_descriptor['bucket'], s3_key)
        cctx = zstd.ZstdCompressor(level=9, write_content_size=True)
        if version_info >= (3, 5):
            data = bytes(array.data)
        else:
            data = bytes(np.ascontiguousarray(array).data)
        data = cctx.compress(data)
        s3io = S3IO(use_s3, None)
        s3io.put_bytes(result_descriptor['bucket'], s3_key, data)

    def execute(self, job, base_results, *args, **kwargs):
        '''Start the job, save results, then set it as completed.'''
        self.job_starts(job)

        # Get data for worker here
        data = self._get_data(job['data']['query'], job['slice'])
        if not set(data.data_vars) == set(job['result_descriptors'].keys()):
            raise ValueError('Inconsistent variables in data and result descriptors:\n{} vs. {}'.format(
                set(data.data_vars), set(job['result_descriptors'].keys())))

        # Execute function here
        computed = self._compute_result(job['function'], data)

        # Save results here
        # map input to output
        # todo: pass in parameters from submit_python_function
        #       - storage parameters
        #       - naming parameters
        #       - unique key name
        for array_name, descriptor in job['result_descriptors'].items():
            base_result_descriptor = base_results[array_name]
            array = computed[array_name]
            # Update result descriptor based on processed data
            self.update_result_descriptor(descriptor, array.shape, array.dtype)
            self._save_array_in_s3(array, base_result_descriptor, job['chunk_id'], self._ee_config['use_s3'])
        self.job_finishes(job)

        return 'Job Complete'
