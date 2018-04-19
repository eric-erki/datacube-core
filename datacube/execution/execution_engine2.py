from __future__ import absolute_import, print_function

from shutil import rmtree
from os import makedirs, walk
from os.path import expanduser, relpath
from sys import version_info
import numpy as np
import zstd
from dill import loads
import zlib
import boto3
from boto3.s3.transfer import TransferConfig

from datacube import Datacube
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
        super(ExecutionEngineV2, self).__init__(name, WorkerTypes.EXECUTION, config, dc=False)

    def _analyse(self, function, data, storage_params, *args, **kwargs):
        """stub to call _decompose to perform data decomposition if required"""
        pass

    def _decompose(self, function_type, function, data, storage_params):
        """further data decomposition to suit compute node"""
        pass

    def _get_data(self, metadata, chunk=None):
        '''Retrieves data for the worker.'''
        if chunk is None:
            return Datacube.load_data(metadata['grouped'], metadata['geobox'],
                                      metadata['measurements_values'].values(), use_threads=True)
        else:
            return Datacube.load_data(metadata['grouped'][chunk[0]], metadata['geobox'][chunk[1:]],
                                      metadata['measurements_values'].values(), use_threads=True)

    def _compute_result(self, function, data, function_params=None, user_task=None):
        '''Run the function on the data.'''
        # TODO: restore function according to its type
        cctx = zstd.ZstdDecompressor()
        function = cctx.decompress(function)
        func = loads(function)
        return func(data, function_params, user_task)

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
        s3io.put_bytes(result_descriptor['bucket'], s3_key, data, True)

    def pre_process(self, job):
        if 'function_params' not in job or job['function_params'] is None:
            job['function_params'] = {}
        if 'user_task' not in job or job['user_task'] is None:
            job['user_task'] = {}

        job_id = job['id']

        base_dir = expanduser("~") + "/EEv2/" + str(job_id)
        input_dir = base_dir + "/input"
        output_dir = base_dir + "/output"

        job['function_params']['input_dir'] = input_dir
        job['function_params']['output_dir'] = output_dir

        # Create input input_dir & output_dir
        try:
            makedirs(input_dir)
            makedirs(output_dir)
        except OSError:
            pass

        # Uncompress and populate input directory input args
        if 'function_params' in job:
            for key, value in job['function_params'].items():
                if not isinstance(value, dict):
                    continue
                if 'copy_to_input_dir' not in value and not value['copy_to_input_dir']:
                    continue
                try:
                    # data = zlib.decompress(value['data'])
                    cctx = zstd.ZstdDecompressor()
                    data = cctx.decompress(value['data'])
                except zlib.error:
                    continue

                f = open(input_dir + "/" + value['fname'], "wb")
                f.write(data)
                f.close()

                job['function_params'][key] = input_dir + "/" + value['fname']

    # pylint: disable=too-many-locals
    def post_process(self, job, user_data, use_s3=False):
        job_id = job['id']

        base_dir = expanduser("~") + "/EEv2/" + str(job_id)
        input_dir = base_dir + "/input"
        output_dir = base_dir + "/output"
        s3_bucket = 'eev2'
        s3_base = str(job_id) + "/output"

        output_files = {}
        # Upload output directory to s3, file by file
        for dirname, _, files in walk(output_dir):
            rel_path = relpath(dirname, output_dir)
            for filename in files:
                if rel_path == '.':
                    s3_key = s3_base + "/" + filename
                else:
                    s3_key = s3_base + "/" + rel_path + "/" + filename
                print(s3_bucket, s3_key, dirname + "/" + filename)
                if use_s3:
                    s3 = boto3.client('s3')
                    transfer_config = TransferConfig(multipart_chunksize=8*1024*1024, multipart_threshold=8*1024*1024,
                                                     max_concurrency=10)
                    s3.upload_file(dirname + "/" + filename, s3_bucket, s3_key, Config=transfer_config)
                else:
                    # store somewhere else.
                    pass
                if rel_path != '.':
                    output_files[rel_path + "/" + filename] = {'bucket': s3_bucket, 'key': s3_key}
                else:
                    output_files[filename] = {'bucket': s3_bucket, 'key': s3_key}

        # Clean up base directory
        try:
            rmtree(base_dir)
        except OSError:
            pass

        # store & return output metadata dict
        user_data.update(output_files)
        self._store.set_user_data(job_id, user_data)
        return {'output_files': output_files}

    def execute(self, job, base_results, *args, **kwargs):
        '''Start the job, save results, then set it as completed.'''
        self.job_starts(job)

        self.pre_process(job)

        # Get data for worker here
        if job['data']:
            data = self._get_data(job['data']['metadata'], job['slice'])
            if not set(data.data_vars) == set(job['result_descriptors'].keys()):
                raise ValueError('Inconsistent variables in data and result descriptors:\n{} vs. {}'.format(
                    set(data.data_vars), set(job['result_descriptors'].keys())))
        else:
            data = None

        # Execute function here
        computed = self._compute_result(job['function'], data,
                                        job['function_params'], job['user_task'])

        user_data = {}
        if data:
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
        else:
            user_data = {'output': computed}

        self.post_process(job, user_data, self._ee_config['use_s3'])

        self.job_finishes(job)

        return 'Job Complete'
