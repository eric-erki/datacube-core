from __future__ import absolute_import, print_function, division

from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp, gettempdir
from sys import version_info
from urllib.parse import urlparse
from uuid import uuid4
from zstd import ZstdDecompressor, ZstdCompressor
from dill import loads
from zlib import error as ZlibError
import numpy as np
from dill import dumps

from datacube.engine_common.pickle_utils import dumps as dc_dumps
from datacube.drivers.s3.storage.s3aio.s3io import S3IO

class FileTransfer(object):
    PAYLOAD = 'payload.bin'

    ARCHIVE = '__archive'

    WORKERS_DIR = 'workers'
    INPUT_DIR = 'input'
    OUTPUT_DIR = 'output'
    S3_DIR = 's3'

    def __init__(self, base_dir=None, use_s3=False):
        if use_s3:
            self._base_dir = Path('/')
        elif base_dir:
            self._base_dir = Path(base_dir)
            self._base_dir.mkdir(parents=True, exist_ok=True)
        else:
            raise ValueError('`base_dir` required when using s3-file')
        self._use_s3 = use_s3
        self._worker_dir = self.base_dir / self.WORKERS_DIR / uuid4().hex
        self._compressor = None
        self._decompressor = None
        self._s3_dir = None
        self._input_dir = None
        self._output_dir = None

    def cleanup(self):
        if self._worker_dir:
            try:
                rmtree(str(self._worker_dir), ignore_errors=True)
                self._base_dir = None
            except OSError:
                raise ValueError('Could not clean up temporary directory: {}'.format(
                    self._worker_dir))

    @property
    def base_dir(self):
        return self._base_dir

    @property
    def s3_dir(self):
        if not self._s3_dir:
            self._s3_dir = self.base_dir / self.S3_DIR
            self._s3_dir.mkdir(parents=True, exist_ok=True)
        return self._s3_dir

    @property
    def input_dir(self):
        if not self._input_dir:
            self._input_dir = self._worker_dir / self.INPUT_DIR
            self._input_dir.mkdir(parents=True, exist_ok=True)
        return self._input_dir

    @property
    def output_dir(self):
        if not self._output_dir:
            self._output_dir = self._worker_dir / self.OUTPUT_DIR
            self._output_dir.mkdir(parents=True, exist_ok=True)
        return self._output_dir

    @property
    def compressor(self):
        if not self._compressor:
            self._compressor = ZstdCompressor(level=9, write_content_size=True)
        return self._compressor

    @property
    def decompressor(self):
        if not self._decompressor:
            self._decompressor = ZstdDecompressor()
        return self._decompressor

    def store_payload(self, bucket, unique_id, payload, tmpdir=None):
        key = '{}/{}'.format(unique_id, self.PAYLOAD)
        url = '{protocol}://{base_dir}{bucket}/{key}'.format(
            protocol='s3' if self._use_s3 else 'file',
            base_dir='' if self._use_s3 else '{}//'.format(self.s3_dir),
            bucket=bucket,
            key=key
        )
        s3io = S3IO(self._use_s3, str(self.s3_dir))
        s3io.put_bytes(bucket, key, dumps(self.pack(payload)))
        return url

    def pack(self, data):
        '''Recursively pack serialise data, pulling data from local files.

        Any file URL contained in a dictionary is replaced by the
        compressed data of the corresponding file, and stored into a sub-dict of the form:
        ```{
            'fname': filename,
            'data': byte data,
            'copy_to_input_dir': True
        }```
        '''
        if callable(data):
            return self.serialise(data)
        elif isinstance(data, dict):
            return {key: self.pack(val) for key, val in data.items()}
        elif isinstance(data, str):
            parsed = urlparse(data)
            if parsed.scheme == 'file':
                filepath = Path(parsed.path)
                with filepath.open('rb') as fh:
                    contents = self.compress(fh.read())
                    return {
                        'fname': filepath.name,
                        'data': contents,
                        'copy_to_input_dir': True
                    }
        # All other cases (incl. str that is not a file URL)
        return data

    def compress(self, data):
        return self.compressor.compress(data)

    def decompress(self, data):
        try:
            return self.decompressor.decompress(data)
        except ZlibError as error:
            raise ValueError('Invalid compressed data')

    def compress_array(self, array):
        if version_info >= (3, 5):
            data = bytes(array.data)
        else:
            data = bytes(np.ascontiguousarray(array).data)
        return self.compress(data)

    def serialise(self, data):
        return self.compress(dc_dumps(data))

    def deserialise(self, data):
        return loads(self.decompress(data))

    def decompress_to_file(self, compressed_data, filename):
        filepath = self.input_dir / filename
        with filepath.open('wb') as fh:
            data = self.decompress(compressed_data)
            fh.write(data)
        return str(filepath)

    def get_archive(self):
        '''Archive the output dir contents.'''
        if not self._output_dir or \
           not len([filepath for filepath in self._output_dir.rglob('*') if filepath.is_file()]):
            return None

        import tarfile
        from io import BytesIO
        data_stream = BytesIO()
        with tarfile.open(fileobj=data_stream, mode='w') as tar:
            tar.add(str(self.output_dir), arcname='')
        data_stream.seek(0)
        # Compress the archive with the same compressor use in this class
        return self.compress(data_stream.read())


    def restore_archive(self, data):
        '''Restore an archive into the base dir.'''
        import tarfile
        from io import BytesIO
        data_stream = BytesIO(self.decompress(data))
        user_data = {'base_dir': str(self.base_dir)}
        with tarfile.open(fileobj=data_stream, mode='r') as tar:
            tar.extractall(str(self.base_dir))
            for member in tar.getmembers():
                if member.isfile():
                    filepath = self.base_dir / member.name
                    user_data[member.name] = filepath.as_uri()
        return user_data
