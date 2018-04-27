from __future__ import absolute_import, print_function

from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp
from sys import version_info
from zstd import ZstdDecompressor, ZstdCompressor
from dill import loads, dumps
from zlib import error as ZlibError
import numpy as np


class FileTransfer(object):
    ARCHIVE = '__archive'

    def __init__(self):
        self._compressor = None
        self._decompressor = None
        self._base_dir = None
        self._input_dir = None
        self._output_dir = None

    def cleanup(self):
        if self._base_dir:
            try:
                rmtree(self._base_dir)
                self._base_dir = None
            except OSError:
                raise ValueError('Could not clean up temporary directory: {}'.format(
                    self._base_dir))

    @property
    def base_dir(self):
        if not self._base_dir:
            self._base_dir = Path(mkdtemp())
        return self._base_dir

    @property
    def input_dir(self):
        if not self._input_dir:
            self._input_dir = self.base_dir / 'input'
            self._input_dir.mkdir()
        return self._input_dir

    @property
    def output_dir(self):
        if not self._output_dir:
            self._output_dir = self.base_dir / 'output'
            self._output_dir.mkdir()
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
        return self.compress(dumps(data))

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
            tar.add(self.output_dir, arcname='')
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
            tar.extractall(self.base_dir)
            for member in tar.getmembers():
                if member.isfile():
                    filepath = self.base_dir / member.name
                    user_data[member.name] = filepath.as_uri()
        return user_data
