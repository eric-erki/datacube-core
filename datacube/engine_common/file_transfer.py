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

# TODO: Split this class
# pylint: disable=too-many-public-methods
class FileTransfer(object):
    PAYLOAD = 'payload.bin'

    ARCHIVE = '__archive'

    WORKERS_DIR = 'workers'
    INPUT_DIR = 'input'
    OUTPUT_DIR = 'output'
    S3_DIR = 's3'

    FUNCTION_NAME = '__ft_function_name__'
    FILE_NAME = '__ft_file_name__'
    FILE_COPY = '__ft_copy_to_input_dir__'
    DATA = '__ft_data__'

    def __init__(self, url=None, base_dir=None, use_s3=False, bucket=None, ids=None):
        if url:
            self._init_from_url(url)
        else:
            self._init_from_params(base_dir, use_s3, bucket, ids)
        if self.use_s3:
            self._base_dir = Path(mkdtemp())
        else:
            self._base_dir.mkdir(parents=True, exist_ok=True)
        self._worker_dir = self.base_dir / self.WORKERS_DIR / uuid4().hex
        self._compressor = None
        self._decompressor = None
        self._s3_dir = None
        self._input_dir = None
        self._output_dir = None
        self._base_key = None
        self._base_url = None

    def cleanup(self, buckets=None):
        '''Deletes the whole base directory, use with caution!.

        The file transfer object becomes unusable after calling this
        method.
        '''
        # Clean S3

        # TODO: We allow to clean other buckets for now, because the
        # workers do not know when the client is finished working with
        # the data.
        buckets = buckets or [self.bucket]
        s3io = S3IO(self.use_s3, str(self.s3_dir))
        deleted = 0
        for bucket in buckets:
            keys = s3io.list_objects(bucket, self.ids[0], max_keys=1000)
            if keys:
                deleted += len(s3io.delete_objects(bucket, keys))

        # Clean local file system
        if self.base_dir:
            try:
                rmtree(str(self.base_dir), ignore_errors=True)
            except OSError:
                raise ValueError('Could not clean up temporary directory: {}'.format(
                    self.base_dir))
        return deleted

    def delete(self, urls):
        '''Returns the number of objects deleted from S3.'''
        to_delete = {}
        for url in urls:
            use_s3, base_dir, bucket, path, ids = self._parse_url(url)
            if bucket not in to_delete:
                to_delete[bucket] = []
            to_delete[bucket].append(str(path))
        s3io = S3IO(self.use_s3, str(self.s3_dir))
        deleted = 0
        for bucket, keys in to_delete.items():
            deleted += len(s3io.delete_objects(bucket, keys))
        return deleted

    @property
    def base_key(self):
        if not self._base_key:
            self._base_key = '/'.join([str(id) for id in self.ids])
        return self._base_key

    @property
    def base_url(self):
        if not self._base_url:
            self._base_url = '{protocol}://{base_dir}{bucket}'.format(
                protocol='s3' if self.use_s3 else 'file',
                base_dir='' if self.use_s3 else '{}//'.format(self.s3_dir),
                bucket=self.bucket
            )
        return self._base_url

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

    def _parse_url(self, url):
        parsed = urlparse(url)
        use_s3 = parsed.scheme == 's3'
        path = parsed.path
        if not use_s3:
            # Double shash expected, else an exception will raise
            tmpdir, path = path.split('//')
            base_dir = Path(tmpdir)
            # FileTransfer base dir should be one level down from the S3 subdir
            if base_dir.stem == FileTransfer.S3_DIR:
                base_dir = base_dir.parent
            path = Path(path)
            bucket = path.parts[0]
            path = path.relative_to(bucket)
            base_dir.mkdir(parents=True, exist_ok=True)
        else:
            bucket = parsed.netloc
            path = Path(path.lstrip('/'))
            base_dir = None
        ids = path.parts
        if str(ids[-1]) == self.PAYLOAD:
            ids = ids[:-1]
        return use_s3, base_dir, bucket, path, ids

    def _init_from_url(self, url):
        self.use_s3, self._base_dir, self.bucket, self._path, self.ids = self._parse_url(url)

    def _init_from_params(self, base_dir, use_s3, bucket, ids):
        if not isinstance(bucket, str):
            raise ValueError('`bucket` required')
        if not isinstance(ids, (list, tuple)):
            raise ValueError('`ids` required')
        self.use_s3 = use_s3
        self.bucket = bucket
        self.ids = ids
        if not use_s3:
            if base_dir:
                self._base_dir = Path(base_dir)
            else:
                raise ValueError('`base_dir` required when using s3-file')

    def fetch_payload(self, unpack=True):
        s3io = S3IO(self.use_s3, str(self.s3_dir))
        payload_key = '/'.join([str(id) for id in self.ids] + [self.PAYLOAD])
        payload = loads(s3io.get_bytes(self.bucket, payload_key))
        return self.unpack(payload) if unpack else payload

    def store_payload(self, payload, sub_id=None):
        key = '{}{}/{}'.format(self.base_key, '/{}'.format(sub_id) if sub_id else '', self.PAYLOAD)
        s3io = S3IO(self.use_s3, str(self.s3_dir))
        s3io.put_bytes(self.bucket, key, dumps(self.pack(payload)))
        return '{}/{}'.format(self.base_url, key)

    def store_array(self, array, base_name, chunk_id):
        '''Saves a single `xarray.DataArray` to s3/s3-file storage'''
        s3_key = '_'.join([base_name, str(chunk_id)])
        data = self.compress_array(array)
        s3io = S3IO(self.use_s3, str(self.s3_dir))
        s3io.put_bytes(self.bucket, s3_key, data, True)
        return s3_key

    def pack(self, data):
        '''Recursively pack data, pulling data from local files.

        Any file URL contained in a dictionary is replaced by the
        compressed data of the corresponding file, and stored into a sub-dict of the form:
        ```{
            'fname': filename,
            'data': byte data,
            'copy_to_input_dir': True
        }```
        '''
        if callable(data):
            return {
                self.FUNCTION_NAME: data.__name__,
                self.DATA: self.serialise(data)
            }
        elif isinstance(data, (list, tuple)):
            packed = [self.pack(val) for val in data]
            return tuple(packed) if isinstance(data, tuple) else packed
        elif isinstance(data, dict):
            return {key: self.pack(val) for key, val in data.items()}
        elif isinstance(data, str):
            parsed = urlparse(data)
            if parsed.scheme == 'file':
                filepath = Path(parsed.path)
                with filepath.open('rb') as fh:
                    contents = self.compress(fh.read())
                    return {
                        self.FILE_NAME: filepath.name,
                        self.DATA: contents,
                        self.FILE_COPY: True
                    }
        # All other cases (incl. str that is not a file URL)
        return data

    def unpack(self, data):
        '''Recursively unpack data, storing to local files.

        Files are written to the local filesystem from decompressed
        data, whenever a of the form is encourntered:
        ```{
            'fname': filename,
            'data': byte data,
            'copy_to_input_dir': True
        }```
        '''
        if isinstance(data, dict):
            if self.FUNCTION_NAME in data:
                # Restore function
                return self.deserialise(data[self.DATA])
            elif self.FILE_NAME in data:
                # Restore file data to filesytem
                if self.FILE_COPY in data and data[self.FILE_COPY]:
                    return self.decompress_to_file(data[self.DATA], data[self.FILE_NAME])
            return {key: self.unpack(val) for key, val in data.items()}
        elif isinstance(data, (list, tuple)):
            packed = [self.unpack(val) for val in data]
            return tuple(packed) if isinstance(data, tuple) else packed
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
