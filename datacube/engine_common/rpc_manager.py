'''Manager for the remote procedure calls (RPC) used by the Analytics
engine.'''

from __future__ import absolute_import, print_function

from pkg_resources import iter_entry_points, DistributionNotFound


GROUP = 'datacube.plugins.rpc'
'''RPC group definitions are in `setup.py`.'''

def _load(entry_point, config):
    '''Load a specific RPC.'''
    # pylint: disable=bare-except
    try:
        rpc_init = entry_point.load()
    except DistributionNotFound:
        # This happens when entry points were marked with extra features,
        # but extra feature were not requested for installation
        raise ValueError('Missing extra features for rpc {}::{}'.format(GROUP, entry_point.name))
    except Exception as e:
        raise ValueError('Failed to resolve rpc {}::{}: {}'.format(GROUP, entry_point.name, e))

    try:
        rpc = rpc_init(config)
    except Exception as e:
        raise ValueError('Failed to initialise rpc {}::{}: {}'.format(GROUP, entry_point.name, e))

    if rpc is None:
        raise ValueError('No RPC returned for rpc {}::{}'.format(GROUP, entry_point.name))

    return rpc

def get_rpc(config):
    '''Return a new instance of the RPC defined in config.

    A ValueError is raised if the RPC cannot be initialised.

    There is NO caching provided, hence a new call to the RPC entry
    point is called each time this method is invoked.
    '''
    rpc_name = config.execution_engine_config['rpc']
    entry_points = list(iter_entry_points(group=GROUP, name=rpc_name))
    if not entry_points:
        raise ValueError('No RPC with name: {}::{}'.format(GROUP, rpc_name))
    elif len(entry_points) > 1:
        raise ValueError('Too many RPCs to choose from: {}'.format(entry_points))
    return _load(entry_points[0], config)
