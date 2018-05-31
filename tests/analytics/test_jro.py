# coding=utf-8

from __future__ import absolute_import

import os
import numpy as np
import pytest
from datacube.analytics.job_result import LazyArray

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
