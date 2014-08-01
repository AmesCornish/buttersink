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

    def selectPath(self, paths):
        """ From a set of destination paths, select the best one to receive to. """
        return [p for p in paths if not p.startswith("/")][0]

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

    def receiveVolumeInfo(self, paths):
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
        self.theDiffs[(self.toVol, self.fromVol)] = size

    def getDiffSize(self):
        """ Get any known size (in bytes) for the difference from previous to self. """
        return self.theDiffs.get((self.toVol, self.fromVol), None)

    def __str__(self):
        """ human-readable string. """
        return u"%s from %s (%s%s) in %s" % (
            self.toVol.display(self.sink),
            self.fromVol.display(self.sink) if self.fromVol else "",
            humanize(self.size),
            "e" if self.sizeIsEstimated else "",
            self.sink,
        )

    theDiffs = {}


class Volume:

    """ Represents a snapshot. """

    def __init__(self, uuid, size=None, exclusiveSize=None, gen=None):
        """ Initialize. """
        assert uuid is not None
        self._uuid = uuid  # Must never change!
        self.size = size
        self.exclusiveSize = exclusiveSize
        self.gen = gen

    def __cmp__(self, vol):
        """ Compare. """
        return cmp(self._uuid, vol._uuid) if vol else 1

    def __hash__(self):
        """ Hash. """
        return hash(self._uuid)

    @property
    def uuid(self):
        """ Read-only uuid. """
        return self._uuid

    def writeInfo(self, stream):
        """ Write information about diffs into a file stream for use later. """
        raise NotImplementedError

    def readInfo(self, stream):
        """ Read previously-written information about diffs. """
        raise NotImplementedError

    def __unicode__(self):
        """ Friendly string for volume. """
        return self.display()

    def __str__(self):
        """ Friendly string for volume. """
        return unicode(self).encode('utf-8')

    def __repr__(self):
        """ Python expression to create self. """
        return "%s(%s)" % (
            self.__class__,
            self.__dict__,
        )

    def display(self, sink=None):
        """ Friendly string for volume, using sink paths. """
        if self.size is not None:
            size = " (%s%s)" % (
                printBytes(self.size),
                "" if self.exclusiveSize is None else (
                    " %s exclusive" % (printBytes(self.exclusiveSize))
                )
            )
        else:
            size = ""

        vol = "%s%s" % (
            printUUID(self._uuid),
            " " + ", ".join(sink.getPaths(self)) if sink else "",
        )

        return vol + size

    @staticmethod
    def getPath(node):
        """ Return printable description of node, if not None. """
        if node is None:
            return None
        uuid = node.uuid
        return node._getPath(uuid)

    def _getPath(self, uuid):
        """ Return printable description of uuid. """
        result = Store.printUUID(uuid)
        try:
            result = "%s (%s)" % (self.diffSink.getVolume(uuid)['path'], result)
        except (KeyError, AttributeError):
            pass
        return result

    @classmethod
    def make(cls, vol):
        """ Convert uuid to Volume, if necessary. """
        if isinstance(vol, cls):
            return vol
        elif vol is None:
            return None
        else:
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
    pow = min(pow, len(units) - 1)
    mantissa = number / (base ** pow)
    return "%.3g %s" % (mantissa, units[pow])


def printUUID(uuid):
    """ Return friendly abbreviated string for uuid. """
    if uuid is None:
        return None
    return "%s...%s" % (uuid[:4], uuid[-4:])
