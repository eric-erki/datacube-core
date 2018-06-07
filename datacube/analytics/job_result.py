"""
Job/Result Class

Array access to Job and Result data.

Main access point for end user after job submission

Note: Interim JobResult Code for incremental testing:
    Test 1:
        - Redis state/health store
            - Redis query/insert API for AE/EE
        - Mock AE/EE
            - stores mock state/health in redis
            - no compute
            - saves mock data to s3
            - returns Job Result object
        - Job Result Object
            - Calls not routed to AE/EE but to Redis API
    Test 2:
        - Redis state/health store
        - Job Result Object
            - Calls routed to AE/EE
        - Testbed AE/EE
            - stores real state
            - stores mock health in redis
            - real compute
            - saves real data to s3
            - returns Job Result object
    Test 3:
        - Redis state/health store
        - Job Result Object
            - Calls routed to AE/EE
        - Proper AE
        - Proper EE
"""

from __future__ import absolute_import, print_function, division

import threading
import numpy as np
from six import integer_types
from six.moves import zip
from itertools import repeat
from pprint import pprint, pformat
from dask.base import tokenize
from dask.array import Array
from enum import Enum
import xarray as xr
from copy import deepcopy

import datacube
from datacube.engine_common.store_handler import ResultTypes, JobStatuses
from datacube.engine_common.file_transfer import FileTransfer
from datacube.engine_common.xarray_utils import get_array_descriptor
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO


class JobResult(object):

    """
    jro = submitjob(...)
    jro.results.red[:, 0:100, 0:100] or jro.results['red'][:, 0:100, 0:100]
    jro.results.masking.red_mask[:, 0:100, 0:100]
    """

    def __init__(self, job_info, result_info, client=None, paths=None, env=None):
        """Initialise the Job/Result object.
        """
        self._status = JobStatuses.RUNNING
        self._client = client
        self._paths = paths
        self._env = env
        self._job = Job(self, job_info)
        self._results = Results(self, result_info)
        self._user_data = UserData(self, job_info)
        self._timer = None

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, client):
        self._client = client

    def to_dict(self):
        return {
            'job': self._job.to_dict(),
            'results': self._results.to_dict(),
            'user_data': self._user_data.to_dict()
        }

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return pformat(self.to_dict(), indent=2)

    @property
    def job(self):
        return self._job

    @property
    def results(self):
        return self._results

    @property
    def user_data(self):
        return self._user_data.user_data

    @property
    def status(self):
        return self._status

    def checkForUpdate(self, period):
        """Starts a periodic timer to check and update the JRO
        """
        if self.job.status == JobStatuses.COMPLETED:
            self.client.update_jro(self, self._paths, self._env)
            self._status = JobStatuses.COMPLETED
            print("Job is Completed")
        else:
            self._timer = threading.Timer(period, self.checkForUpdate, [period]).start()


class Job(object):
    """
    job: object to query job.
        job.id - id of the job
        job.queued - checks if job has been queued
        job.complete - checks if job has completed
        job.cancel() - cancels job.
        job.stats
        job.provenance
           - function hash/id
        job.logs - function logs
    """
    _id = 0

    def __init__(self, jro, job_info):
        self._jro = jro
        self._job_info = job_info
        self._id = self._job_info['id']
        # unpack other required info

    def to_dict(self):
        return {
            'id': self._id,
            'status': self.status
        }

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return pformat(self.to_dict(), indent=2)

    @property
    def id(self):
        """id of the job
        """
        return self._id

    @property
    def status(self):
        """status of the job (queued, running, complete, cancelled, errored)
        """
        status = None
        if self._jro.client:
            status = self._jro.client.get_status(self, self._jro._paths, self._jro._env)
        return status

    def cancel(self):
        """cancels job in progress
        """
        pass

    def statistics(self):
        """statistics for the job
        """
        pass

    def provenance(self):
        """provenance for the job (function id/hash, data id/hash)
        """
        pass

    def logs(self):
        """logs for the job
        """
        pass


class Dotify(dict):
    def __getattr__(self, attr):
        val = dict.get(attr)
        if isinstance(type(val), dict):
            return Dotify(val)
        else:
            return val
        __setattr__ = dict.__setitem__
        __delattr__ = dict.__delitem__


class LoadType(Enum):
    EAGER = 1
    EAGER_CACHED = 2
    DASK = 3


class LazyArray(object):
    """Looks and feels like a numpy/array array but is mapped to S3/Disk
    """
    # TODO(csiro):
    #     - embed this in s3io library and use s3io lazy array object
    #       - array wrapper around s3lio.get_data_unlabeled

    _id = 0

    def __init__(self, array_info):
        """Initialise the array with array_info:
        """
        self._array_info = array_info

        # unpack result_info and popular internal variables
        self._id = self._array_info['id']
        self._type = self._array_info['type']
        self._load_type = self._array_info['load_type']
        self._output_dir = self._array_info['output_dir']
        self._cache = {}
        self._xarray_descriptor = None
        self._shape = None

        if self._type in [ResultTypes.S3IO, ResultTypes.FILE]:
            # Initially, the shape, dtype and chunk size are not known as they are defined as
            # results get processed.
            self._shape_components = None
            self._coords_components = None
            self._chunk = None
            self._dtype = None
            self._base_name = self._array_info['base_name']
            self._bucket = self._array_info['bucket']
            self.update(array_info)
        elif self._type == ResultTypes.INDEXED:
            self._query = self._array_info['query']

    def _update_shape(self, array_info):
        """Progressively build the shape of the full array.

        Each time a sub-array is returned, its shape and position in the full array are used to
        compute the shape of the full array. Looking at the problem in 2d:
        ```
        [[b b ... b | e]
         [b b ... b | e]
         [   ...    | .]
         [b b ... b | e]
         [e e ... e | e]
        ```

        Where `b` denotes a cell in the main `body` of the full array, and `e` a cell on the
        edge. All `b` cells have the same shape, i.e. that of the chunking used to store this
        array. The `e` cells are smaller or equal to the chunk size.

        We only need to know the shape of any `b` cell, as well as any cell in the last column and
        any cell in the last row, as well the total number of cells to determine the full
        shape. This means the LazyArray's shape can be determined before all results are returned.

        Upon completion, the shape is compared to the xarray descriptor's number of coordinates in
        each direction, if available.
        """
        if not self._shape:
            sub_shape = array_info['shape']
            position =  array_info['position']
            total_cells = array_info['total_cells']
            if self._shape_components is None:
                # Each dimension's size is determined by 1 body and 1 edge lengths. We initially
                # create an empty array of the same dimension as the total number of cells, but with
                # 2 values for each. Once the array is full, the full shape is known.
                self._shape_components = [[None, None] for i in range(len(sub_shape))]
            for dim, length in enumerate(sub_shape):
                # Work out dimensions one by one
                if position[dim] < total_cells[dim]:
                    # Body cell
                    if self._shape_components[dim][0] is None:
                        self._shape_components[dim][0] = length
                else:
                    # Edge cell
                    if self._shape_components[dim][1] is None:
                        self._shape_components[dim][1] = length
                    if total_cells[dim] == 0:
                        # Specific case of single cell along that dimension (chunk size >= full
                        # array size along that dimension)
                        self._shape_components[dim][0] = 0
            # If all cells are full, calculate full array shape
            if all([length is not None for component in self._shape_components
                    for length in component]):
                self._shape = tuple(comps[0] * cells + comps[1]
                                    for comps, cells in zip(self._shape_components, total_cells))
                self._check_array_shape()

    def _update_xarray_descriptor(self, array_info):
        """Progressively build the xarray descriptor for the full array.

        Each time a result is produced, the corresponding sub-array is returned, with its own xarray
        descriptor. It is assumed that all results related to the same LazyArray will share the same
        attributes and dimensions, but they will all cover specific coordinates. This method
        concatenates these coordinates until it has enough information to fully describe the full
        array. Looking at a 2d example:

        ```
        [[a b c]
         [d e f]
        ```

        Where each letter denotes a result (hence a sub-array) which has its own set of
        coordinates. Knowing the coordinate sets of `{a, b, c, d}` for example, is enough to inform
        the overall array. Basically, we need at least one cell for each slice in each
        dimension. Hence, `{d, b, c}` would also be sufficient for example.

        This method performs its job using the relative `position` of the result inside the overall
        set of `total_cells`. It simply concatenates series of coordinates returned by the
        sub-results, respecting the order of slices and coordinates, but not performing any data
        check (e.g. duplicates or non-ordered coordinates). The only check performed is to validate
        that the number of coordinates in each direction indeed match the shape of the LazyArray.
        """
        if not self._xarray_descriptor and array_info['xarray_descriptor']:
            sub_coords = array_info['xarray_descriptor']['coords']
            position =  array_info['position']
            total_cells = array_info['total_cells']
            if not self._coords_components:
                # Allocate empty list (dims) of list (length along each dim) of coords
                self._coords_components = [[None for i in range(l + 1)] for l in total_cells]
            for dim_no, dim in enumerate(array_info['xarray_descriptor']['dims']):
                if not self._coords_components[dim_no][position[dim_no]]:
                    self._coords_components[dim_no][position[dim_no]] = sub_coords[dim]['data']

            if not any(coord is None for dim_coords in self._coords_components
                   for coord in dim_coords):
                self._xarray_descriptor = deepcopy(array_info['xarray_descriptor'])
                self._xarray_descriptor['coords'] = {
                    dim: [c for cs in self._coords_components[dim_no] for c in cs]
                    for dim_no, dim in enumerate(array_info['xarray_descriptor']['dims'])}
                self._check_array_shape()

    def _check_array_shape(self):
        """Check that the LazyArray shape matches the number of coordinates.

        This simple check is performed once, when both the shape and xarray descriptor have been
        fully reconstructed from incoming sub-results.
        """
        if self._shape and self._xarray_descriptor:
            lengths = tuple(len(self._xarray_descriptor['coords'][dim])
                            for dim in self._xarray_descriptor['dims'])
            assert self._shape == lengths, 'LazyArray concatenated shape does not match ' \
                'concatenated xarray coords lengths: {} vs. {}'.format(self._shape, lengths)

    def update(self, array_info):
        """Update LazyArray with new information
        Todo:
            - update keys in array_info into local variables if required.
            - make it work for ResultTypes.INDEXED:
        """
        self._update_shape(array_info)
        self._update_xarray_descriptor(array_info)
        if not self._dtype:
            self._dtype = array_info['dtype']
        if not self._chunk:
            self._chunk = array_info['chunk']

    def to_dict(self):
        if self._type in [ResultTypes.S3IO, ResultTypes.FILE]:
            return {
                'id': self._id,
                'base_name': self._base_name,
                'bucket': self._bucket,
                'shape': self._shape,
                'chunk': self._chunk,
                'dtype': self._dtype
            }
        elif self._type == ResultTypes.INDEXED:
            return {'query': self._query}
        return {}

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return pformat(self.to_dict(), indent=2)

    @property
    def id(self):
        """id of the job
        """
        return self._id

    def clear_cache(self):
        self._cache = {}

    def set_mode(self, mode):
        if mode in LoadType:
            self._load_type = mode

    # pylint: disable=too-many-locals, too-many-branches
    def __getitem__(self, slices):
        """Slicing operator to retrieve data stored on S3/DataCube
        Todo:
            - chunk cache memory management in case near memory limit
                - unload least used and stream as required from S3
        """
        if not self._shape:
            return None

        def dask_array(zipped, shape, chunks, dtype, bucket, use_s3):

            dsk = {}
            name = zipped[0][0]
            for key, data_slice, local_slice, chunk_shape, offset, chunk_id in zipped:
                required_chunk = zip((key,), (data_slice,), (local_slice,), (chunk_shape,), (offset,), (chunk_id,))
                idx = (name,) + np.unravel_index(chunk_id, [int(np.ceil(s/c)) for s, c in zip(shape, chunks)])
                s3lio = S3LIO(True, use_s3, self._output_dir)
                dsk[idx] = (s3lio.get_data_single_chunks_unlabeled, required_chunk, dtype, bucket)

            return Array(dsk, name, chunks=chunks, shape=shape, dtype=dtype)

        if self._type in [ResultTypes.S3IO, ResultTypes.FILE]:
            if not isinstance(slices, tuple):
                slices = (slices,)

            bounded_slice = self._bounded_slice(slices, self._shape)
            use_s3 = True
            if self._type == ResultTypes.FILE:
                use_s3 = False
            s3lio = S3LIO(True, use_s3, self._output_dir)
            keys, data_slices, local_slices, chunk_shapes, offset, chunk_ids = \
                s3lio.build_chunk_list(self._base_name, self._shape, self._chunk, self._dtype, bounded_slice, False)

            zipped = list(zip(keys, data_slices, local_slices, chunk_shapes, repeat(offset), chunk_ids))

            if self._load_type == LoadType.EAGER:
                '''
                xarray_dict = self._slice_metadata(self._xarray_descriptor, bounded_slice)
                xarray_dict['data'] = s3lio.get_data_unlabeled_mp(self._base_name, self._shape, self._chunk, self._dtype,
                                                                  bounded_slice, self._bucket).to_list()
                return xr.from_dict(xarray_dict)
                '''
                return xr.DataArray(s3lio.get_data_unlabeled_mp(self._base_name, self._shape, self._chunk, self._dtype,
                                                                bounded_slice, self._bucket, use_geo=True))
            elif self._load_type == LoadType.EAGER_CACHED:
                missing_chunks = []
                for a in zipped:
                    if a[5] not in self._cache:
                        missing_chunks.append(a)

                if missing_chunks:
                    received_chunks = s3lio.get_data_full_chunks_unlabeled_mp(missing_chunks, self._dtype, self._bucket)

                    if received_chunks:
                        self._cache.update(received_chunks)

                # build array from cache and return
                data = np.zeros(shape=[s.stop - s.start for s in bounded_slice], dtype=self._dtype)

                for _, data_slice, local_slice, _, _, chunk_id in zipped:
                    data[data_slice] = self._cache[chunk_id][local_slice]

                if not self._xarray_descriptor:
                    self._xarray_descriptor = s3lio.get_coords(self._bucket, self._base_name)
                if self._xarray_descriptor:
                    xarray_descriptor = self._slice_metadata(self._xarray_descriptor, bounded_slice)
                    xarray_descriptor['data'] = data.tolist()
                    return xr.DataArray.from_dict(xarray_descriptor)
                return data
            elif self._load_type == LoadType.DASK:
                full_slice = self._bounded_slice((slice(None, None),), self._shape)

                keys, data_slices, local_slices, chunk_shapes, offset, chunk_ids = \
                    s3lio.build_chunk_list(self._base_name, self._shape, self._chunk, self._dtype, full_slice, False)

                zipped = list(zip(keys, data_slices, local_slices, chunk_shapes, repeat(offset), chunk_ids))
                return xr.DataArray(dask_array(zipped, self._shape, self._chunk, self._dtype, self._bucket, use_s3)
                                    [bounded_slice])
        elif self._type == ResultTypes.INDEXED:
            # Todo: Do this properly
            #       1. EAGER: Retrieve only what is asked for, code in testbed, moving over soon.
            #       2. DASK: use dask_chunks in dc.load
            #       3. EAGER_CACHED: cache chunks with integer index.
            dc = datacube.Datacube()
            data = dc.load(use_threads=True, **self._query).to_array()[0]
            if not isinstance(slices, tuple):
                slices = (slices,)

            return data[slices]
        else:
            raise Exception("Undefined storage type")

    def _bounded_slice(self, slices, shape):
        bounded_slice = ()
        for idx, val in enumerate(slices):
            if isinstance(val, integer_types):
                if val < 0 or val+1 >= self._shape[idx]:
                    raise Exception("Index: " + val + " is out of bounds of: " + str(self._shape[idx]))
                bounded_slice += (slice(val, val+1),)
            elif isinstance(val, slice):
                if val.start is None and val.stop is None:
                    bounded_slice += (slice(0, self._shape[idx]), )
                elif val.start >= 0 and val.stop <= self._shape[idx]:
                    bounded_slice += (val,)
                else:
                    raise Exception("Slice: " + str(slices) + " is not within shape: : " + str(self._shape))

        for idx in range(len(bounded_slice), len(self._shape)):
            bounded_slice += (slice(0, self._shape[idx]),)

        return bounded_slice

    def _slice_metadata(self, descriptor, array_slice):
        d = deepcopy(descriptor)
        for dim, s in zip(d['dims'], array_slice):
            d['coords'][dim]['data'] = d['coords'][dim]['data'][s]
        return d

    def __setitem__(self, slices, value):
        """Slicing operator to modify data stored on S3/DataCube
        """

        # if self._array_info['type'] is 's3io':
        #     yield s3io.save(...)
        # else if self._array_info['type'] is 'dc.load':
        #     yield dc.save(...)
        # else:
        #     raise Exception("Undefied storage type")

    @staticmethod
    def save(array, chunk_size, base_name, bucket, use_s3, output_dir=None):
        """Saves an array to s3 and returns the array descriptor.
        """
        s3lio = S3LIO(True, use_s3, output_dir)
        s3lio.put_array_in_s3_mp(array, chunk_size, base_name, bucket, False, True)
        array_info = {}
        array_info['id'] = None
        if use_s3:
            array_info['type'] = ResultTypes.S3IO
        else:
            array_info['type'] = ResultTypes.FILE
        array_info['load_type'] = LoadType.EAGER
        array_info['base_name'] = base_name
        array_info['bucket'] = bucket
        array_info['shape'] = array.shape
        array_info['chunk'] = chunk_size
        array_info['dtype'] = array.dtype
        array_info['output_dir'] = output_dir
        array_info['position'] = (0, 0, 0)
        array_info['total_cells'] = (0, 0, 0)
        array_info['xarray_descriptor'] = get_array_descriptor(array)
        return array_info

    @staticmethod
    def load(array_info):
        """Construct a LazyArray from array_info.
        """
        return LazyArray(array_info)

    def delete(self):
        """deletes the result from storage:
            - S3 (redis index + storage)
            - DataCube (postgres index + storage)
        """
        pass

    def to_netcdf(self, path=None):
        """Exports LazyArray to a NetCDF file.
        """
        self[:].to_netcdf(path=path, engine='netcdf4')


class Results(object):
    """
    results : object to query results
        results.complete - checks if results have been completed
        results.metadata - metadata about the results
        results.id - tuple of results of datasets e.g. (result.red.id, result.blue.id)
        results.delete() - delete all results from storage (interim in S3 or ingested/indexed in ODC)
        results.datasets - dict of results.
            results['red'] - retrieves result 'red' (can be lazy)
            results.red - same as above
            results.red.id - id of result 'red'
            results.masking.band1[:,0:400,0:400] - array slice data retrieval (can be lazy)
            results.user_data - returns user put in.
            results.red.delete() - delete result 'red' from storage (interim in S3 or ingested/indexed in ODC)
    """

    def __init__(self, jro, result_info):
        self._jro = jro
        self._result_info = result_info
        self._datasets = Dotify({})
        # unpack result_info and popular internal variables
        self._id = self._result_info['id']
        for k, v in self._result_info['results'].items():
            self._add_array(k, v)

    def to_dict(self):
        if not self._id:
            return None
        return {
            'id': self._id,
            'status': self.status,
            'datasets': {k: v.to_dict() for k, v in self._datasets.items()}
        }

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return pformat(self.to_dict(), indent=2)

    @property
    def id(self):
        """id of the result
        """
        return self._id

    @property
    def datasets(self):
        """List Result names.
        """
        return self._datasets

    @property
    def status(self):
        status = None
        if self._jro.client and self.id:
            status = self._jro.client.get_status(self, self._jro._paths, self._jro._env)
        return status

    def metadata(self):
        pass

    def delete(self):
        """deletes all results from storage:
            - S3 (redis index + storage)
            - DataCube (postgres index + storage)
        """
        pass

    def logs(self):
        """logs for the result
        """
        pass

    def _add_array(self, name, array_info):
        self._datasets.update({name: LazyArray(array_info)})

    def update_arrays(self, arrays):
        for array_metadata in arrays.values():
            array_info = array_metadata.descriptor
            name = '_'.join((array_info['output_name'], array_info['band']))
            if name in self._datasets:
                self._datasets[name].update(array_info)
            else:
                self._datasets[name] = LazyArray(array_info)

    def __getattr__(self, key):
        if key in self._datasets:
            return self._datasets[key]
        else:
            print("Variable not found")
            return None


class UserData(object):
    '''User data is auxillary data generated by a job and saved as
    files.'''

    _job_id = 0
    '''The ID of the job this data relates to.'''

    def __init__(self, jro, job_info):
        '''Prepare the user data for the corresponding job ID.'''
        self._jro = jro
        self._job_id = job_info['id']
        self._user_data = None

    def to_dict(self):
        '''Convenience for string representation.'''
        return {
            'job_id': self.job_id,
            'user_data': self.user_data
        }

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return pformat(self.to_dict(), indent=2)

    @property
    def job_id(self):
        '''ID of the job this user data relates to.'''
        return self._job_id

    @property
    def user_data(self):
        '''Fetch the user data from the server.'''
        if not self._user_data:
            if self._jro.job.status == JobStatuses.COMPLETED and self._jro.client:
                self._user_data = self._jro.client.get_user_data(self.job_id, self._jro._paths, self._jro._env)
                for datum in self._user_data:
                    if FileTransfer.ARCHIVE in datum:
                        # Restore archive to local temp dir and update user_data file entries
                        file_transfer = FileTransfer()
                        restored = file_transfer.restore_archive(datum[FileTransfer.ARCHIVE])
                        datum.update(restored)
                        del datum[FileTransfer.ARCHIVE]
        return self._user_data
