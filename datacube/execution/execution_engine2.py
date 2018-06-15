from __future__ import absolute_import, print_function

from os import makedirs, walk
from sys import version_info
from xarray import Dataset, DataArray
import boto3
from boto3.s3.transfer import TransferConfig
from xarray.core.utils import decode_numpy_dict_values, ensure_us_time_resolution

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

    def __init__(self, name, paths, env, output_dir=None):
        super(ExecutionEngineV2, self).__init__(name, WorkerTypes.EXECUTION, paths, env, output_dir)
        self._file_transfer = FileTransfer()
        self._use_s3 = self._ee_config['use_s3']
        self._result_type = ResultTypes.S3IO if self._use_s3 else ResultTypes.FILE
        self._result_bucket = self._ee_config['result_bucket']

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
        # TODO: restore function according to its type
        func = self._file_transfer.deserialise(function)
        return func(data, self._datacube, function_params, user_task)

    def _save_array_in_s3(self, array, base_name, chunk_id):
        '''Saves a single `xarray.DataArray` to s3/s3-file storage'''
        s3_key = '_'.join([base_name, str(chunk_id)])
        data = self._file_transfer.compress_array(array)
        s3io = S3IO(self._use_s3, self._output_dir)
        s3io.put_bytes(self._result_bucket, s3_key, data, True)
        return s3_key

    def _save_array(self, base_job_id, job, output_name, band_name, array, chunk):
        '''Save a single array to s3 and its metadata in the store.

        The metadata is prepared from the base job id, and the job itself. A new result entry is
        created in the store.
        '''
        base_name = '_'.join(['job', str(base_job_id), output_name, str(band_name)])
        descriptor = {
            'type': self._result_type,
            'load_type': LoadType.EAGER,
            'output_dir': self._output_dir,
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
        s3_key = self._save_array_in_s3(array, base_name, job['chunk_id'])
        self.logger.debug('New result (id:%d) metadata stored to %s-%s: %s',
                          result_id, self._result_bucket, s3_key, result_meta.descriptor)

    def pre_process(self, job):
        if 'function_params' not in job or job['function_params'] is None:
            job['function_params'] = {}
        if 'user_task' not in job or job['user_task'] is None:
            job['user_task'] = {}
        job['function_params']['input_dir'] = str(self._file_transfer.input_dir)
        job['function_params']['output_dir'] = str(self._file_transfer.output_dir)

        # Uncompress and populate input directory input args
        if 'function_params' in job:
            for key, value in job['function_params'].items():
                if not isinstance(value, dict):
                    continue
                if 'copy_to_input_dir' not in value or not value['copy_to_input_dir']:
                    continue
                filepath = self._file_transfer.decompress_to_file(value['data'], value['fname'])
                job['function_params'][key] = filepath

    # pylint: disable=too-many-locals
    def post_process(self, job, user_data):
        job_id = job['id']
        output_files = {}
        if self._use_s3:
            s3_bucket = self._result_bucket
            s3 = boto3.client('s3')
            transfer_config = TransferConfig(multipart_chunksize=8*1024*1024,
                                             multipart_threshold=8*1024*1024,
                                             max_concurrency=10)
            for filepath in self._file_transfer.output_dir.rglob('*'):
                if filepath.is_file():
                    relpath = str(filepath.relative_to(self._file_transfer.output_dir))
                    s3_key = str(filepath).lstrip('/')
                    s3.upload_file(str(filepath), s3_bucket, s3_key, Config=transfer_config)
                    output_files[relpath] = 's3://{}/{}'.format(s3_bucket, s3_key)
        else:
            # Last resort: store the whole output folder as compressed archive in redis
            archive = self._file_transfer.get_archive()
            if archive:
                output_files[FileTransfer.ARCHIVE] = archive

        # Clean up base directory
        self._file_transfer.cleanup()

        # store & return output metadata dict
        user_data.update(output_files)
        self._store.set_user_data(job_id, user_data)
        return {'output_files': output_files}

    def execute(self, job, base_job_id, *args, **kwargs):
        '''Start the job, save results, then set it as completed.'''
        self.logger.debug('Starting execution of subjob %d', job['id'])
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

        self.post_process(job, user_data)
        self.job_finishes(job, JobStatuses.COMPLETED)
        self.logger.info('Completed execution of subjob %d', job['id'])
