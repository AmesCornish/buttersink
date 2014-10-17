""" Utilities for btrfs and buttersink. """

from __future__ import division

import pprint
import math


def pretty(obj):
    """ Return pretty representation of obj. """
    # if True:
    #     return pprint.pformat(dict(obj.__dict__))
    return pprint.pformat(obj)
    # try:
    #     return pprint.pformat(dict(obj))
    # except TypeError:
    #     try:
    #         return pprint.pformat(obj.__dict__)
    #     except KeyError:
    #         logger.exception("Funny error.")


def humanize(number):
    """ Return a human-readable string for number. """
    # units = ('bytes', 'KB', 'MB', 'GB', 'TB')
    # base = 1000
    units = ('bytes', 'KiB', 'MiB', 'GiB', 'TiB')
    base = 1024
    if number is None:
        return None
    pow = int(math.log(number, base)) if number > 0 else 0
    pow = min(pow, len(units) - 1)
    mantissa = number / (base ** pow)
    return "%.4g %s" % (mantissa, units[pow])
