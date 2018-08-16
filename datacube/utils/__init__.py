"""
Utility functions
"""
from __future__ import absolute_import, division, print_function

import itertools
import logging
import os
import pathlib
from math import ceil

import numpy
import toolz
import xarray

from datacube.utils.dates import parse_time
from datacube.utils.documents import read_documents, validate_document, NoDatesSafeLoader, get_doc_offset, \
    get_doc_offset_safe

from .uris import is_url, uri_to_local_path, get_part_from_uri, mk_part_uri
from .dates import datetime_to_seconds_since_1970
from .serialise import jsonify_document
from .documents import InvalidDocException, SimpleDocNav, DocReader, is_supported_document_type, read_strings_from_netcdf
from .py import cached_property, ignore_exceptions_if, import_function


_LOG = logging.getLogger(__name__)


def namedtuples2dicts(namedtuples):
    """
    Convert a dict of namedtuples to a dict of dicts.

    :param namedtuples: dict of namedtuples
    :return: dict of dicts
    """
    return {k: dict(v._asdict()) for k, v in namedtuples.items()}


def sorted_items(d, key=None, reverse=False):
    """Given a dictionary `d` return items: (k1, v1), (k2, v2)... sorted in
    ascending order according to key.

    :param dict d: dictionary
    :param key: optional function remapping key
    :param bool reverse: If True return in descending order instead of default ascending

    """
    key = toolz.first if key is None else toolz.comp(key, toolz.first)
    return sorted(d.items(), key=key, reverse=reverse)


def attrs_all_equal(iterable, attr_name):
    """
    Return true if everything in the iterable has the same value for `attr_name`.

    :rtype: bool
    """
    return len({getattr(item, attr_name, float('nan')) for item in iterable}) <= 1


def unsqueeze_data_array(da, dim, pos, coord=0, attrs=None):
    """
    Add a 1-length dimension to a data array.

    :param xarray.DataArray da: array to add a 1-length dimension
    :param str dim: name of new dimension
    :param int pos: position of dim
    :param coord: label of the coordinate on the unsqueezed dimension
    :param attrs: attributes for the coordinate dimension
    :return: A new xarray with a dimension added
    :rtype: xarray.DataArray
    """
    new_dims = list(da.dims)
    new_dims.insert(pos, dim)
    new_shape = da.data.shape[:pos] + (1,) + da.data.shape[pos:]
    new_data = da.data.reshape(new_shape)
    new_coords = {k: v for k, v in da.coords.items()}
    new_coords[dim] = xarray.DataArray([coord], dims=[dim], attrs=attrs)
    return xarray.DataArray(new_data, dims=new_dims, coords=new_coords, attrs=da.attrs)


def unsqueeze_dataset(ds, dim, coord=0, pos=0):
    ds = ds.apply(unsqueeze_data_array, dim=dim, pos=pos, keep_attrs=True, coord=coord)
    return ds


def clamp(x, l, u):
    """
    clamp x to be l <= x <= u

    >>> clamp(5, 1, 10)
    5
    >>> clamp(-1, 1, 10)
    1
    >>> clamp(12, 1, 10)
    10
    """
    assert l <= u
    return l if x < l else u if x > u else x


def intersects(a, b):
    return a.intersects(b) and not a.touches(b)


def data_resolution_and_offset(data):
    """
    >>> data_resolution_and_offset(numpy.array([1.5, 2.5, 3.5]))
    (1.0, 1.0)
    >>> data_resolution_and_offset(numpy.array([5, 3, 1]))
    (-2.0, 6.0)
    """
    res = (data[data.size - 1] - data[0]) / (data.size - 1.0)
    off = data[0] - 0.5 * res
    return numpy.asscalar(res), numpy.asscalar(off)


class DatacubeException(Exception):
    """Your Data Cube has malfunctioned"""
    pass


def iter_slices(shape, chunk_size):
    """
    Generate slices for a given shape.

    E.g. ``shape=(4000, 4000), chunk_size=(500, 500)``
    Would yield 64 tuples of slices, each indexing 500x500.

    If the shape is not divisible by the chunk_size, the last chunk in each dimension will be smaller.

    :param tuple(int) shape: Shape of an array
    :param tuple(int) chunk_size: length of each slice for each dimension
    :return: Yields slices that can be used on an array of the given shape

    >>> list(iter_slices((5,), (2,)))
    [(slice(0, 2, None),), (slice(2, 4, None),), (slice(4, 5, None),)]
    """
    assert len(shape) == len(chunk_size)
    num_grid_chunks = [int(ceil(s / float(c))) for s, c in zip(shape, chunk_size)]
    for grid_index in numpy.ndindex(*num_grid_chunks):
        yield tuple(
            slice(min(d * c, stop), min((d + 1) * c, stop)) for d, c, stop in zip(grid_index, chunk_size, shape))


def schema_validated(schema):
    """
    Decorate a class to enable validating its definition against a JSON Schema file.

    Adds a self.validate() method which takes a dict used to populate the instantiated class.

    :param pathlib.Path schema: filename of the json schema, relative to `SCHEMA_PATH`
    :return: wrapped class
    """

    def validate(cls, document):
        return validate_document(document, cls.schema, schema.parent)

    def decorate(cls):
        cls.schema = next(iter(read_documents(schema)))[1]
        cls.validate = classmethod(validate)
        return cls

    return decorate


def _tuplify(keys, values, defaults):
    assert not set(values.keys()) - set(keys), 'bad keys'
    return tuple(values.get(key, default) for key, default in zip(keys, defaults))


def _slicify(step, size):
    return (slice(i, min(i + step, size)) for i in range(0, size, step))


def _block_iter(steps, shape):
    return itertools.product(*(_slicify(step, size) for step, size in zip(steps, shape)))


def tile_iter(tile, chunk_size):
    """
    Return the sequence of chunks to split a tile into computable regions.

    :param tile: a tile of `.shape` size containing `.dim` dimensions
    :param chunk_size: dict of dimension sizes
    :return: Sequence of chunks to iterate across the entire tile
    """
    steps = _tuplify(tile.dims, chunk_size, tile.shape)
    return _block_iter(steps, tile.shape)


def write_user_secret_file(text, fname, in_home_dir=False, mode='w'):
    """Write file only readable/writeable by the user"""

    if in_home_dir:
        fname = os.path.join(os.environ['HOME'], fname)

    open_flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    access = 0o600  # Make sure file is readable by current user only
    with os.fdopen(os.open(fname, open_flags, access), mode) as handle:
        handle.write(text)
        handle.close()


def slurp(fname, in_home_dir=False, mode='r'):
    """
    Read an entire file into a string

    :param fname: file path
    :param in_home_dir: if True treat fname as a path relative to $HOME folder
    :return: Content of a file or None if file doesn't exist or can not be read for any other reason
    """
    if in_home_dir:
        fname = os.path.join(os.environ['HOME'], fname)
    try:
        with open(fname, mode) as handle:
            return handle.read()
    except IOError:
        return None


def gen_password(num_random_bytes=12):
    """
    Generate random password
    """
    import base64
    return base64.urlsafe_b64encode(os.urandom(num_random_bytes)).decode('utf-8')


def _readable_offset(offset):
    return '.'.join(map(str, offset))
