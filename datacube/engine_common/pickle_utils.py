"""
pickle helper utilities. This module.
"""
from __future__ import absolute_import

import dill


def dumps(user_function):
    """This creates a dill.dumps() of a function that works across modules.
    """
    code = dill.source.importable(user_function, source=True)
    global_dict = {}
    local_dict = {}
    # pylint: disable=exec-used
    exec(code, global_dict, local_dict)
    return dill.dumps(local_dict[user_function.__name__])
