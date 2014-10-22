""" Utilities for btrfs and buttersink. """

from __future__ import division

import math
import pprint
import traceback


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


def displayTraceBack():
    """ Display traceback useful for debugging. """
    tb = traceback.format_stack()
    return "\n" + "".join(tb[:-1])


class DefaultList(list):

    """ list that automatically inserts None for missing items. """

    def __setitem__(self, index, value):
        """ Set item. """
        if len(self) > index:
            return list.__setitem__(self, index, value)
        if len(self) < index:
            self.extend([None] * (index - len(self)))
        list.append(self, value)

    def __getitem__(self, index):
        """ Set item. """
        if index >= len(self):
            return None
        return list.__getitem__(self, index)
