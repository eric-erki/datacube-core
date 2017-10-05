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

from __future__ import absolute_import, print_function

import numpy as np
from six import integer_types
from six.moves import zip
from itertools import repeat
from pprint import pprint, pformat
from dask.base import tokenize
from dask.array import Array
from enum import Enum
import xarray as xr

import datacube
from .utils.store_handler import ResultTypes
from datacube.drivers.s3.storage.s3aio.s3lio import S3LIO


class JobResult(object):

    """
    jro = submitjob(...)
    jro.results.red[:, 0:100, 0:100] or jro.results['red'][:, 0:100, 0:100]
    jro.results.masking.red_mask[:, 0:100, 0:100]
    """

    def __init__(self, job_info, result_info):
        """Initialise the Job/Result object.
        """
        self._client = None
        self._job = Job(self, job_info)
        self._results = Results(self, result_info)

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, client):
        self._client = client

    def to_dict(self):
        return {
            'job': self._job.to_dict(),
            'results': self._results.to_dict()
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
            status = self._jro.client.get_status(self)
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
    #     - make this class lazy via Dask.

    _id = 0

    # chunk cache {chunk_id: bytes}
    _cache = {}

    def __init__(self, array_info):
        """Initialise the array with array_info:
        """
        self._array_info = array_info

        # unpack result_info and popular internal variables
        self._id = self._array_info['id']
        self._type = self._array_info['type']
        self._load_type = self._array_info['load_type']
        self._query = {}

        if self._type == ResultTypes.S3IO:
            self._base_name = self._array_info['base_name']
            self._bucket = self._array_info['bucket']
            self._shape = self._array_info['shape']
            self._chunk = self._array_info['chunk']
            self._dtype = self._array_info['dtype']
        elif self._type == ResultTypes.INDEXED:
            self._query = self._array_info['query']

    def to_dict(self):
        if self._type == ResultTypes.S3IO:
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

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return pformat(self.to_dict(), indent=2)

    # pylint: disable=too-many-locals
    def __getitem__(self, slices):
        """Slicing operator to retrieve data stored on S3/DataCube
        Todo:
            - chunk cache memory management in case near memory limit
                - unload least used and stream as required from S3
        """
        def dask_array(zipped, shape, chunks, dtype, bucket):

            dsk = {}
            name = zipped[0][0]
            for key, data_slice, local_slice, chunk_shape, offset, chunk_id in zipped:
                required_chunk = zip((key,), (data_slice,), (local_slice,), (chunk_shape,), (offset,), (chunk_id,))
                idx = (name,) + np.unravel_index(chunk_id, chunk_shape)
                s3lio = S3LIO(True, True, None, 30)
                dsk[idx] = (s3lio.get_data_single_chunks_unlabeled, required_chunk, dtype, bucket)

            return Array(dsk, name, chunks=chunks, shape=shape, dtype=dtype)

        if self._type == ResultTypes.S3IO:
            if not isinstance(slices, tuple):
                slices = (slices,)

            bounded_slice = self._bounded_slice(slices, self._shape)

            s3lio = S3LIO(True, True, None, 30)
            keys, data_slices, local_slices, chunk_shapes, offset, chunk_ids = \
                s3lio.build_chunk_list(self._base_name, self._shape, self._chunk, self._dtype, bounded_slice, False)

            zipped = list(zip(keys, data_slices, local_slices, chunk_shapes, repeat(offset), chunk_ids))

            if self._load_type == LoadType.EAGER:
                return xr.DataArray(s3lio.get_data_unlabeled(self._base_name, self._shape, self._chunk, self._dtype,
                                                             bounded_slice, self._bucket))
            elif self._load_type == LoadType.EAGER_CACHED:
                missing_chunks = []
                for a in zipped:
                    if a[5] not in self._cache:
                        missing_chunks.append(a)

                received_chunks = s3lio.get_data_full_chunks_unlabeled(missing_chunks, self._dtype, self._bucket)
                # do mp version

                if received_chunks:
                    self._cache.update(received_chunks)

                # build array from cache and return
                data = np.zeros(shape=[s.stop - s.start for s in bounded_slice], dtype=self._dtype)

                for _, data_slice, local_slice, _, _, chunk_id in zipped:
                    data[data_slice] = self._cache[chunk_id][local_slice]

                return data
            elif self._load_type == LoadType.DASK:
                full_slice = self._bounded_slice((slice(None, None),), self._shape)

                s3lio = S3LIO(True, True, None, 30)
                keys, data_slices, local_slices, chunk_shapes, offset, chunk_ids = \
                    s3lio.build_chunk_list(self._base_name, self._shape, self._chunk, self._dtype, full_slice, False)

                zipped = list(zip(keys, data_slices, local_slices, chunk_shapes, repeat(offset), chunk_ids))
                return xr.DataArray(dask_array(zipped, self._shape, self._chunk, self._dtype, self._bucket)
                                    [bounded_slice])
        elif self._type == ResultTypes.INDEXED:
            # Todo: Do this properly
            #       1. EAGER: Retrieve only what is asked for, code in testbed, moving over soon.
            #       2. DASK: use dask_chunks in dc.load
            #       3. EAGER_CACHED: cache chunks with integer index.
            dc = datacube.Datacube(app='dc-example')
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

        for idx, val in enumerate(range(len(self._shape) - len(bounded_slice))):
            bounded_slice += (slice(0, self._shape[idx]),)

        return bounded_slice

    def __setitem__(self, slices, value):
        """Slicing operator to modify data stored on S3/DataCube
        """

        # if self._array_info['type'] is 's3io':
        #     yield s3io.save(...)
        # else if self._array_info['type'] is 'dc.load':
        #     yield dc.save(...)
        # else:
        #     raise Exception("Undefied storage type")

    def delete(self):
        """deletes the result from storage:
            - S3 (redis index + storage)
            - DataCube (postgres index + storage)
        """
        pass


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
        if self._jro.client:
            status = self._jro.client.get_status(self)
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

    def __getattr__(self, key):
        if key in self._datasets:
            return self._datasets[key]
        else:
            print("Variable not found")
