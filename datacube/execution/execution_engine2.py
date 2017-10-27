'''A simple execution engine.'''

from __future__ import absolute_import, print_function

from sys import version_info
import numpy as np
import zstd
from cloudpickle import loads

from datacube.analytics.worker import Worker
from datacube.drivers.s3.storage.s3aio.s3io import S3IO


class ExecutionEngineV2(Worker):
    def _get_data(self, query, chunk=None):
        '''Retrieves data for the worker.'''
        if chunk is None:
            return self._datacube.load(use_threads=True, **query)
        else:
            metadata = self._datacube.metadata_for_load(**query)
            return self._datacube.load_data(metadata['grouped'][chunk[0]], metadata['geobox'][chunk[1:]],
                                            metadata['measurements_values'].values(),
                                            driver_manager=self._driver_manager, use_threads=True)

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

    def execute(self, base_results):
        '''Start the job, save results, then set it as completed.'''
        self.job_starts()

        # Get data for worker here
        data = self._get_data(self._job['data']['query'], self._job['slice'])
        if not set(data.data_vars) == set(self._job['result_descriptors'].keys()):
            raise ValueError('Inconsistent variables in data and result descriptors:\n{} vs. {}'.format(
                set(data.data_vars), set(self._job['result_descriptors'].keys())))

        # Execute function here
        computed = self._compute_result(self.job['function'], data)

        # Save results here
        # map input to output
        # todo: pass in parameters from submit_python_function
        #       - storage parameters
        #       - naming parameters
        #       - unique key name
        for array_name, descriptor in self._job['result_descriptors'].items():
            base_result_descriptor = base_results[array_name]
            array = computed[array_name]
            # Update result descriptor based on processed data
            self.update_result_descriptor(descriptor, array.shape, array.dtype)
            self._save_array_in_s3(array, base_result_descriptor, self._job['chunk_id'])
        self.job_finishes()

        return computed
