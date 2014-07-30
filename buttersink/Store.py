""" Abstract and component classes for sources and sinks.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

# sink, source, src, dest: store
# volume, diff

# Classes: CamelCase
# files: CamelCase.py
# project: buttersink

from __future__ import division

import collections
import math


class Store(object):

    """ Abstract class for storage back-end.

    Diffs should be indexed by "from" volume.
    Paths should be indexed by volume.

    """

    def __init__(self):
        """ Initialize. """
        # { vol: [path] }
        self.paths = collections.defaultdict((lambda: set()))

    def listVolumes(self):
        """ Return list of all volumes in this Store's selected directory. """
        for (vol, paths) in self.paths.items():
            for path in paths:
                if not path.startswith('/'):
                    yield vol
                break

    def getPaths(self, volume):
        """ Return list of all paths to this volume in this Store. """
        return self.paths[volume]

    # Abstract methods

    def _fillVolumesAndPaths(self):
        """ Fill in self.paths. """
        raise NotImplementedError

    def getEdges(self, fromVol):
        """ Return the edges available from fromVol. """
        raise NotImplementedError

    def hasEdge(self, diff):
        """ True if Store already contains this edge. """
        raise NotImplementedError

    def receive(self, diff, paths):
        """ Return Context Manager for a file-like (stream) object to store a diff. """
        raise NotImplementedError

    def send(self, diff, streamContext, progress=True):
        """ Write the diff (toVol from fromVol) to the stream context manager. """
        raise NotImplementedError

    def receiveVolumeInfo(self, volume, paths):
        """ Return Context Manager for a file-like (stream) object to store volume info. """
        return NotImplementedError


class Diff:

    """ Represents a btrfs send diff that creates toVol from fromVol. """

    def __init__(self, sink, toVol, fromVol, size, sizeIsEstimated=False):
        """ Initialize. """
        self.sink = sink
        self.toVol = Volume.make(toVol)
        self.fromVol = Volume.make(fromVol)
        self.size = size
        self.sizeIsEstimated = sizeIsEstimated

    def setDiffSize(self, size):
        """ Set a known size (in bytes) for the difference from previous to self. """
        Volume.theDiffs[(self.uuid, previousUUID)] = size

    def getDiffSize(self):
        """ Get any known size (in bytes) for the difference from previous to self. """
        return Volume.theDiffs.get((self.uuid, previousUUID), None)


class Volume:

    """ Represents a snapshot. """

    def __init__(self, uuid, size=None, exclusiveSize=None, gen=None):
        """ Initialize. """
        self._uuid = uuid  # Must never change!
        self.size = size
        self.exclusiveSize = exclusiveSize
        self.gen = gen

    def __cmp__(self, vol):
        """ Compare. """
        return cmp(self._uuid, vol._uuid)

    def __hash__(self):
        """ Hash. """
        return hash(self._uuid)

    @property
    def uuid(self):
        """ Read-only uuid. """
        return self._uuid
    
    theDiffs = {}

    def writeInfo(self, stream):
        """ Write information about diffs into a file stream for use later. """
        raise NotImplementedError

    def readInfo(self, stream):
        """ Read previously-written information about diffs. """
        raise NotImplementedError

    def __unicode__(self):
        """ Friendly string for volume. """
        size = ""
        if self.exclusiveSize is not None:
            size += "%s exclusive" % (printBytes(self.exclusiveSize))
        if self.size is not None:
            size = "%s, " % (printBytes(self.size)) + size

        if size:
            return "%s (%s)" % (self.uuid, size)
        else:
            return unicode(self.uuid)

    def __str__(self):
        """ Friendly string for volume. """
        return unicode(self).encode('utf-8')

    def printVolume(vol):
        """ Return string for dict containing volume info. """

    @classmethod
    def make(cls, vol):
        """ Convert uuid to Volume, if necessary. """
        if isinstance(vol, cls):
            return vol
        return cls(vol)


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
    """ Return friendly abbreviated string for uuid. """
    if uuid is None:
        return None
    return "%s...%s" % (uuid[:4], uuid[-4:])
