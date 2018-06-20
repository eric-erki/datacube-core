"""
xarray utilities. This module adds functionality to the base xarray module.
"""

from __future__ import absolute_import

from copy import deepcopy
from xarray import DataArray
from xarray.core.utils import decode_numpy_dict_values, ensure_us_time_resolution


def get_array_descriptor(array):
    """Return a dict-like descriptor of a DataArray.

    It returns None for other types of arrays.
    """
    if not isinstance(array, DataArray):
        return None
    d = {'coords': {}, 'attrs': decode_numpy_dict_values(array.attrs),
         'dims': array.dims}
    for k in array.coords:
        data = ensure_us_time_resolution(array[k].values).tolist()
        d['coords'].update({
            k: {'data': data,
                'dims': array[k].dims,
                'attrs': decode_numpy_dict_values(array[k].attrs)}})
    d.update({'data': None,
              'name': array.name})
    return d

def slice_metadata(descriptor, array_slice):
    d = deepcopy(descriptor)
    for dim, s in zip(d['dims'], array_slice):
        d['coords'][dim]['data'] = d['coords'][dim]['data'][s]
    return d
