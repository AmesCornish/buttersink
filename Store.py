""" Abstract and component classes for sources and sinks. """

# sink, source, src, dest: store
# volume, diff

# Classes: CamelCase
# files: CamelCase.py
# project: butter_sink

# Volume: { uuid, path, size }
# Diff: { toUUID, fromUUID, size }

from __future__ import division

import math


class Store:

    """ Abstract class for storage back-end. """

    def hasEdge(self, toUUID, fromUUID):
        """ Store already contains this edge. """
        raise NotImplementedError

    def receive(self, toUUID, fromUUID, path):
        """ Return a file-like (stream) object to store a diff. """
        raise NotImplementedError

    def send(self, toUUID, fromUUID, streamContext):
        """ Write the diff (toUUID from fromUUID) to the stream context manager. """
        raise NotImplementedError


def humanize(number):
    """ Return a human-readable string for number. """
    # units = ('bytes', 'KB', 'MB', 'GB', 'TB')
    # base = 1000
    units = ('bytes', 'KiB', 'MiB', 'GiB', 'TiB')
    base = 1024
    pow = int(math.log(number, base)) if number > 0 else 0
    pow = min(pow, len(units)-1)
    mantissa = number / (base ** pow)
    return "%.3g %s" % (mantissa, units[pow])
