""" Abstract and component classes for sources and sinks.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.
"""

# sink, source, src, dest: store
# volume, diff

# Classes: CamelCase
# files: CamelCase.py
# project: buttersink

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

    def send(self, toUUID, fromUUID, streamContext, progress=True):
        """ Write the diff (toUUID from fromUUID) to the stream context manager. """
        raise NotImplementedError


def printBytes(number):
    """ Return a human-readable string for number. """
    return humanize(number)


def humanize(number):
    """ Return a human-readable string for number. """
    # units = ('bytes', 'KB', 'MB', 'GB', 'TB')
    # base = 1000
    units = ('bytes', 'KiB', 'MiB', 'GiB', 'TiB')
    base = 1024
    if number is None:
        return None
    pow = int(math.log(number, base)) if number > 0 else 0
    pow = min(pow, len(units)-1)
    mantissa = number / (base ** pow)
    return "%.3f %s" % (mantissa, units[pow])


def printUUID(uuid):
    if uuid is None:
        return None
    return "%s...%s" % (uuid[:4], uuid[-4:])


def printVolume(vol):
    """ Return string for dict containing volume info. """
    size = "%s exclusive" % (printBytes(vol['exclusiveSize']))
    if 'totalSize' in vol:
        size = "%s, " % (printBytes(vol['totalSize'])) + size

    return "%s %s (%s)" % (
        vol['path'], vol['uuid'], size,
        )
