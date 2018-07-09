# coding=utf-8

from __future__ import absolute_import

import os
from pprint import pformat
import numpy as np
import pytest
from datacube.engine_common.store_handler import ResultTypes, JobStatuses
from datacube.analytics.job_result import JobResult, LazyArray, LoadType

sa = pytest.importorskip('SharedArray')
s3aio = pytest.importorskip('datacube.drivers.s3.storage.s3aio')


class TestJRO(object):

    def test_Result_save_load(self, tmpdir):
        x = np.arange(4 * 4 * 4, dtype=np.uint8).reshape((4, 4, 4))
        descriptor = LazyArray.save(x, (2, 2, 2), "lazy_array", "arrayio", False, str(tmpdir))
        array = LazyArray.load(descriptor)
        assert np.array_equal(x, array[:])
        array.to_netcdf(str(tmpdir) + "/test.nc")
        assert os.path.isfile(str(tmpdir) + "/test.nc")

    def test_job_result(self):
        jro = JobResult(
            {'id': 9},
            {'id': 99, 'results': {'Hello': {
                'id': 999,
                'type': ResultTypes.S3IO,
                'load_type': LoadType.EAGER,
                'bucket': 'test_bucket',
                'output_dir': '/tmp',
                'base_name': 'test',
                'shape': (1, 200, 200),
                'total_cells': (1, 1, 1),
                'position': (0, 0, 0),
                'xarray_descriptor': None,
                'dtype': 'int16',
                'chunk': (1, 200, 200)
            }}})
        assert jro.client is None
        assert jro.results.status is None
        assert jro.user_data is None
        assert jro.status == JobStatuses.RUNNING
        assert jro.__repr__() == '''{ 'job': {'id': 9, 'status': None},
  'results': { 'datasets': { 'Hello': { 'base_name': 'test',
                                        'bucket': 'test_bucket',
                                        'chunk': (1, 200, 200),
                                        'dtype': 'int16',
                                        'id': 999,
                                        'shape': None}}},
  'user_data': {'job_id': 9, 'user_data': None}}'''

        # Job
        job = jro.job
        assert job.status is None
        assert job.__repr__() == '''{'id': 9, 'status': None}'''
        assert job.id == 9
        # Empty methods
        job.cancel()
        job.statistics()
        job.provenance()
        job.logs()

        # Results
        results = jro.results
        assert results.id is 99
        assert results.status is None
        assert results.Joe is None  # Invalid dataset name
        assert type(results.Hello) == LazyArray
        assert results.__repr__() == '''{ 'datasets': { 'Hello': { 'base_name': 'test',
                           'bucket': 'test_bucket',
                           'chunk': (1, 200, 200),
                           'dtype': 'int16',
                           'id': 999,
                           'shape': None}}}'''
        assert results.Hello.id == 999

        assert results.Hello.__repr__() == '''{ 'base_name': 'test',
  'bucket': 'test_bucket',
  'chunk': (1, 200, 200),
  'dtype': 'int16',
  'id': 999,
  'shape': None}'''

        assert results.Hello._load_type == LoadType.EAGER
        results.Hello.set_mode(LoadType.DASK)
        assert results.Hello._load_type == LoadType.DASK
        results.Hello.clear_cache()

        # Empty methods
        results.delete()
        results.metadata()

        # User data
        user_data = jro._user_data
        assert user_data.user_data is None
        assert user_data.job_id == 9
