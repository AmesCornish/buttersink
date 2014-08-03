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
import logging
import math
import os.path

logger = logging.getLogger('__name__')
# logger.setLevel('DEBUG')


class Store(object):

    """ Abstract class for storage back-end.

    Diffs should be indexed by "from" volume.
    Paths should be indexed by volume.

    """

    ignoreExtraVolumes = True

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

    def getSendPath(self, volume):
        """ Get a path appropriate for sending the volume from this Store.

        The path may be relative or absolute in this Store.

        """
        return next(iter(self.getPaths(volume)))

    def selectReceivePath(self, paths):
        """ From a set of destination paths, select the best one to receive to.

        The paths are relative or absolute, in a source Store.
        The result will be relative, suitable for this destination Store.

        """
        logger.debug("%s", paths)
        try:
            return [p for p in paths if not p.startswith("/")][0]
        except IndexError:
            return os.path.basename(list(paths)[0])

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

    def receive(self, diff, paths, dryrun=False):
        """ Return Context Manager for a file-like (stream) object to store a diff. """
        raise NotImplementedError

    def send(self, diff, streamContext, progress=True, dryrun=False):
        """ Write the diff (toVol from fromVol) to the stream context manager. """
        raise NotImplementedError

    def receiveVolumeInfo(self, paths, dryrun=False):
        """ Return Context Manager for a file-like (stream) object to store volume info. """
        return NotImplementedError


class Diff:

    """ Represents a btrfs send diff that creates toVol from fromVol. """

    def __init__(self, sink, toVol, fromVol, size, sizeIsEstimated=False):
        """ Initialize. """
        self.sink = sink
        self.toVol = Volume.make(toVol)
        self.fromVol = Volume.make(fromVol)
        self._size = size
        self._sizeIsEstimated = sizeIsEstimated

        if self.fromVol is not None and size is not None and not sizeIsEstimated:
            Diff.theKnownSizes[self.toVol][self.fromVol] = size

    # {toVolume: {fromVolume: size}}
    theKnownSizes = collections.defaultdict(lambda: collections.defaultdict(lambda: None))

    @property
    def toUUID(self):
        """ 'to' volume's UUID. """
        return self.toVol.uuid

    @property
    def fromUUID(self):
        """ 'from' volume's UUID, if any. """
        return self.fromVol.uuid if self.fromVol else None

    @property
    def size(self):
        """ Return size. """
        self._updateSize()
        return self._size

    @property
    def sizeIsEstimated(self):
        """ Return whether size is estimated. """
        self._updateSize()
        return self._sizeIsEstimated

    def _updateSize(self):
        if self._size and not self._sizeIsEstimated:
            return

        size = Diff.theKnownSizes[self.toVol][self.fromVol]

        if size is None:
            return

        self._size = size
        self._sizeIsEstimated = False

    def __str__(self):
        """ human-readable string. """
        return u"%s from %s (%s%s) in %s" % (
            self.toVol.display(self.sink),
            self.fromVol.display(self.sink) if self.fromVol else "<None>",
            humanize(self.size),
            "-e" if self.sizeIsEstimated else "",
            self.sink,
        )


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
        for (fromVol, size) in Diff.theKnownSizes[self].iteritems():
            if size is not None and fromVol is not None:
                stream.write("%s\t%s\t%d\n" % (
                    self.uuid,
                    fromVol.uuid if fromVol else "<None>",
                    size,
                    ))

    def readInfo(self, stream):
        """ Read previously-written information about diffs. """
        for line in stream:
            (toUUID, fromUUID, size) = line.split()
            if toUUID != self.uuid:
                logger.warn("Expected UUID=%s, got %s", self.uuid, toUUID)
                continue
            Diff.theKnownSizes[self][Volume(fromUUID)] = size

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

    def display(self, sink=None, detail='phrase'):
        """ Friendly string for volume, using sink paths. """
        if detail in ('line', 'paragraph') and self.size is not None:
            size = " (%s%s)" % (
                printBytes(self.size),
                "" if self.exclusiveSize is None else (
                    " %s exclusive" % (printBytes(self.exclusiveSize))
                )
            )
        else:
            size = ""

        vol = "%s %s" % (
            printUUID(self._uuid),
            sink.getSendPath(self) if sink else "",
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
    return "%.4g %s" % (mantissa, units[pow])


def printUUID(uuid):
    """ Return friendly abbreviated string for uuid. """
    if uuid is None:
        return None
    return "%s...%s" % (uuid[:4], uuid[-4:])
