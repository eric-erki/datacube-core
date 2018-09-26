from __future__ import absolute_import, print_function

from os import makedirs, walk
from sys import version_info
from xarray import Dataset, DataArray
import boto3
from boto3.s3.transfer import TransferConfig
from xarray.core.utils import decode_numpy_dict_values, ensure_us_time_resolution
from dill import loads

from datacube import Datacube
from datacube.engine_common.store_handler import ResultTypes, ResultMetadata, JobStatuses
from datacube.engine_common.store_workers import WorkerTypes
from datacube.engine_common.file_transfer import FileTransfer
from datacube.engine_common.xarray_utils import get_array_descriptor
from datacube.analytics.job_result import LoadType
from datacube.analytics.worker import Worker
from datacube.drivers.s3.storage.s3aio.s3io import S3IO
from datacube.drivers.s3aio_index import S3AIOIndex


class ExecutionEngineV2(Worker):
    '''This class is responsible for receiving decomposed jobs (function, data)
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
    '''

    def __init__(self, name, params_url):
        super().__init__(name, WorkerTypes.EXECUTION, params_url)
        # Unpack input and job params
        self._job_params = self._file_transfer.unpack(self._job_params)
        self._input_params = self._file_transfer.unpack(self._input_params)
        self._result_type = ResultTypes.S3IO if self._use_s3 else ResultTypes.FILE

    def _analyse(self, function, data, storage_params, *args, **kwargs):
        '''stub to call _decompose to perform data decomposition if required'''
        pass

    def _decompose(self, function_type, function, data, storage_params):
        '''further data decomposition to suit compute node'''
        pass

    def _get_data(self, metadata, chunk=None):
        '''Retrieves data for the worker.'''
        use_threads = isinstance(self._datacube.index, S3AIOIndex)
        if chunk is None:
            return Datacube.load_data(metadata['grouped'], metadata['geobox'],
                                      metadata['measurements_values'].values(), use_threads=use_threads)
        else:
            return Datacube.load_data(metadata['grouped'][chunk[0]], metadata['geobox'][chunk[1:]],
                                      metadata['measurements_values'].values(), use_threads=use_threads)

    def _compute_result(self, function, data, function_params=None, user_task=None):
        '''Run the function on the data.'''
        return function(data, self._datacube, function_params, user_task)

    def _save_array(self, base_job_id, job, output_name, band_name, array, chunk):
        '''Save a single array to s3 and its metadata in the store.

        The metadata is prepared from the base job id, and the job itself. A new result entry is
        created in the store.
        '''
        base_name = self._request_id + '/' + '_'.join(['job', str(base_job_id), output_name, str(band_name)])
        descriptor = {
            'type': self._result_type,
            'load_type': LoadType.EAGER,
            'output_dir': str(self._file_transfer.s3_dir),
            'base_name': base_name,
            'output_name': output_name,
            'band': str(band_name),
            'bucket': self._result_bucket,
            'shape': array.shape,
            'chunk': chunk,
            'dtype': array.dtype,
            'position': job['position'],
            'total_cells': job['total_cells'],
            'xarray_descriptor': get_array_descriptor(array)
        }
        result_meta = ResultMetadata(self._result_type, descriptor)
        result_id = self._store.add_result(job['id'], result_meta)
        # Add newly created ID to the descriptor and update in store
        result_meta.descriptor['id'] = result_id
        self._store.update_result(result_id, result_meta)
        s3_key = self._file_transfer.store_array(array, base_name, job['chunk_id'])
        self.logger.debug('New result (id:%d) metadata stored to s3://%s/%s: %s',
                          result_id, self._result_bucket, s3_key, result_meta.descriptor)

    def pre_process(self, job):
        '''Copy user data into job descriptor.

        Function parameters and users tasks are only saved in the
        original input params S3 payload, so we copy them across into
        the job dictionary so they can be used during exectution.
        '''
        job['function_params'] = {}
        if 'function_params' in self._input_params and self._input_params['function_params']:
            job['function_params'] = self._input_params['function_params']
        if 'user_task' not in job or job['user_task'] is None:
            job['user_task'] = {}
        job['function_params']['input_dir'] = str(self._file_transfer.input_dir)
        job['function_params']['output_dir'] = str(self._file_transfer.output_dir)

    # pylint: disable=too-many-locals
    def post_process(self, job, user_data):
        output_files = {}
        for filepath in self._file_transfer.output_dir.rglob('*'):
            if filepath.is_file():
                relpath = str(filepath.relative_to(self._file_transfer.output_dir))
                output_files[relpath] = filepath.as_uri()
        return output_files

    def execute(self):
        '''Start the job, save results, then set it as completed.'''
        job = self._job_params
        base_job_id = self._request_id

        self.logger.debug('Starting execution of subjob %d', job['id'])
        args = self._input_params['args']
        kwargs = self._input_params['kwargs']
        self.job_starts(job)
        self.pre_process(job)

        # Prepare data, if any was passed
        data_dict = {name: self._get_data(job_data['metadata'], job['slice'])
                     for name, job_data in job['data'].items()} \
                         if job['data'] else {}

        # Execute function here
        computed = self._compute_result(job['function'], data_dict,
                                        job['function_params'], job['user_task'])

        # TODO: In the following we assume that if data was passed to the user function, then the
        # output is a dict of Dataset/DataArray, e.g. one per input query or satellite source. If no
        # data was passed to the function, we assume the output is a random python object, e.g. a
        # string. This assumption needs to be removed, e.g. by letting the function specifically
        # type its output.
        user_data = {}
        if data_dict:
            # todo: pass in parameters from submit_python_function
            #       - storage parameters
            #       - naming parameters
            #       - unique key name
            for output_name, output in computed.items():
                data = output['data']
                if isinstance(data, Dataset):
                    for band_name in data.data_vars:
                        array = data[band_name]
                        self._save_array(base_job_id, job, output_name, band_name, array, output['chunk'])
                elif isinstance(data, DataArray):
                    self._save_array(base_job_id, output_name, None, data, output['chunk'])
                else:
                    raise ValueError('Invalid return type from user function: {}'.format(
                        type(data)))
        else:
            user_data['output'] = computed

        user_data['files'] = self.post_process(job, user_data)
        url = self._file_transfer.store_payload(user_data, 'OUTPUT')
        self._store.set_user_data(job['id'], url)
        # Remove payload from S3
        self._file_transfer.delete([self._params_url])

        self.job_finishes(job, JobStatuses.COMPLETED)
        self.logger.info('Completed execution of subjob %d', job['id'])
