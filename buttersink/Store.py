""" Abstract and component classes for sources and sinks.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

from __future__ import division

import abc
import collections
import functools
import hashlib
import io
import logging
import math
import os.path
import sys

logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')


class Store(object):

    """ Abstract class for storage back-end.

    Diffs should be indexed by "from" volume.
    Paths should be indexed by volume.

    """

    __metaclass__ = abc.ABCMeta

    ignoreExtraVolumes = False

    def __init__(self, userPath, isDest, dryrun):
        """ Initialize. """
        # { vol: [path] }
        # The order of the paths is important to work around btrfs bugs.
        # The first path is usually the root-volume mounted path,
        # which is required by btrfs 3.14.2.
        self.paths = collections.defaultdict((lambda: []))

        # Paths specify a directory containing subvolumes,
        # unless it's a source path not ending in "/",
        # then it's a single source subvolume.

        if not (userPath.endswith("/") or isDest):
            self.userVolume = os.path.basename(userPath)
            userPath = os.path.dirname(userPath)
        else:
            self.userVolume = None

        # This will not end with a "/"
        userPath = os.path.normpath(userPath)

        assert userPath.startswith("/"), userPath
        self.userPath = userPath

        logger.debug("%s('%s')", self.__class__.__name__, userPath)

        self.dryrun = dryrun

    def listContents(self):
        """ Return list of volumes or diffs in this Store's selected directory. """
        vols = list(self.listVolumes())
        vols.sort(key=lambda v: self.getSendPath(v))
        return [vol.display(self, detail="line") for vol in vols]

    def listVolumes(self):
        """ Return list of all volumes in this Store's selected directory. """
        for (vol, paths) in self.paths.items():
            for path in paths:
                if path.startswith('/'):
                    continue
                if self.userVolume is not None and os.path.basename(path) != self.userVolume:
                    continue
                yield vol
                break

    def getPaths(self, volume):
        """ Return list of all paths to this volume in this Store. """
        return self.paths[volume]

    def getSendPath(self, volume):
        """ Get a path appropriate for sending the volume from this Store.

        The path may be relative or absolute in this Store.

        """
        try:
            return self._fullPath(next(iter(self.getPaths(volume))))
        except StopIteration:
            return None

    def selectReceivePath(self, paths):
        """ From a set of destination paths, select the best one to receive to.

        The paths are relative or absolute, in a source Store.
        The result will be absolute, suitable for this destination Store.

        """
        logger.debug("%s", paths)
        if not paths:
            path = os.path.basename(self.userPath) + '/Anon'
        try:
            path = [p for p in paths if not p.startswith("/")][0]
        except IndexError:
            path = os.path.basename(list(paths)[0])

        return self._fullPath(path)

    def _fullPath(self, path):
        if path.startswith("/"):
            return path
        if path == ".":
            return self.userPath
        return os.path.normpath(os.path.join(self.userPath, path))

    def _relativePath(self, fullPath):
        if fullPath is None:
            return None

        assert fullPath.startswith("/"), fullPath

        path = os.path.relpath(fullPath, self.userPath)

        if not path.startswith("../"):
            return path
        elif self.ignoreExtraVolumes:
            return None
        else:
            return fullPath

    def _skipDryRun(self, logger, level='DEBUG', dryrun=None):
        return skipDryRun(logger, dryrun or self.dryrun, level)

    # Abstract methods

    @abc.abstractmethod
    def _fillVolumesAndPaths(self):
        """ Fill in self.paths. """
        raise NotImplementedError

    @abc.abstractmethod
    def getEdges(self, fromVol):
        """ Return the edges available from fromVol. """
        raise NotImplementedError

    @abc.abstractmethod
    def hasEdge(self, diff):
        """ True if Store already contains this edge. """
        raise NotImplementedError

    @abc.abstractmethod
    def receive(self, diff, paths):
        """ Return Context Manager for a file-like (stream) object to store a diff. """
        raise NotImplementedError

    @abc.abstractmethod
    def send(self, diff, progress=True):
        """ Return Context Manager for a file-like (stream) object to send a diff. """
        raise NotImplementedError

    @abc.abstractmethod
    def receiveVolumeInfo(self, paths):
        """ Return Context Manager for a file-like (stream) object to store volume info. """
        raise NotImplementedError

    @abc.abstractmethod
    def keep(self, diff):
        """ Mark this diff (or volume) to be kept in path. """
        raise NotImplementedError

    @abc.abstractmethod
    def deleteUnused(self):
        """ Delete any old snapshots in path, if not kept. """
        raise NotImplementedError

    @abc.abstractmethod
    def deletePartials(self):
        """ Delete any old partial uploads/downloads in path. """
        raise NotImplementedError


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
            Diff.theKnownSizes[self.toUUID][self.fromUUID] = size

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

    def sendTo(self, dest, chunkSize, progress=True):
        """ Send this difference to the dest Store. """
        logger.info("%s: %s", "Keep" if self.sink == dest else "Xfer", self)

        vol = self.toVol
        paths = self.sink.getPaths(vol)

        if self.sink == dest:
            self.sink.keep(self)
            return

        streamContext = dest.receive(self, paths)

        sendContext = self.sink.send(self, progress)

        # try:
        #     streamContext.metadata['btrfsVersion'] = self.btrfsVersion
        # except AttributeError:
        #     pass

        try:
            streamContext.progress = progress
        except AttributeError:
            pass

        try:
            chunkSize = streamContext.chunkSize
        except AttributeError:
            pass

        if sendContext is not None and streamContext is not None:
            with streamContext as writer:
                # Open reader after writer,
                # so any raised errors will abort write before writer closes.
                with sendContext as reader:
                    checkBefore = None
                    if hasattr(writer, 'skipChunk'):
                        checkBefore = hasattr(reader, 'checkSum')

                    while True:
                        if checkBefore is True:
                            (size, checkSum) = reader.checkSum(chunkSize)

                            if writer.skipChunk(size, checkSum):
                                reader.seek(size, io.SEEK_CUR)
                                continue

                        data = reader.read(chunkSize)
                        if len(data) == 0:
                            break

                        if checkBefore is False:
                            checkSum = hashlib.md5(data).hexdigest()

                            if writer.skipChunk(len(data), checkSum, data):
                                continue

                        writer.write(data)

        if vol.hasInfo():
            infoContext = dest.receiveVolumeInfo(paths)

            if infoContext is None:
                vol.writeInfo(sys.stdout)
            else:
                with infoContext as stream:
                    vol.writeInfo(stream)

    def _updateSize(self):
        if self._size and not self._sizeIsEstimated:
            return

        size = Diff.theKnownSizes[self.toUUID][self.fromUUID]

        if size is None:
            return

        self._size = size
        self._sizeIsEstimated = False

    def __str__(self):
        """ human-readable string. """
        return u"%s from %s (%s%s)" % (
            self.toVol.display(self.sink),
            self.fromVol.display(self.sink) if self.fromVol else "None",
            "~" if self.sizeIsEstimated else "",
            humanize(self.size),
            # self.sink,
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
        for (fromUUID, size) in Diff.theKnownSizes[self.uuid].iteritems():
            if size is None or fromUUID is None:
                continue
            if not isinstance(size, int):
                logger.warning("Bad size: %s", size)
                continue
            stream.write(str("%s\t%s\t%d\n" % (
                self.uuid,
                fromUUID,
                size,
            )))

    def hasInfo(self):
        """ Will have information to write. """
        count = len([None
                     for (fromUUID, size)
                     in Diff.theKnownSizes[self.uuid].iteritems()
                     if size is not None and fromUUID is not None
                     ])
        return count > 0

    @staticmethod
    def readInfo(stream):
        """ Read previously-written information about diffs. """
        for line in stream:
            (toUUID, fromUUID, size) = line.split()
            try:
                size = int(size)
            except:
                logger.warning("Bad size: %s", size)
                continue
            logger.debug("diff info: %s %s %d", toUUID, fromUUID, size)
            Diff.theKnownSizes[toUUID][fromUUID] = size

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
        if not isinstance(detail, int):
            detail = detailNum[detail]

        if detail >= detailNum['line'] and self.size is not None:
            size = " (%s%s)" % (
                printBytes(self.size),
                "" if self.exclusiveSize is None else (
                    " %s exclusive" % (printBytes(self.exclusiveSize))
                )
            )
        else:
            size = ""

        vol = "%s %s" % (
            printUUID(self._uuid, detail - 1),
            sink.getSendPath(self) if sink else "",
        )

        return vol + size

    @classmethod
    def make(cls, vol):
        """ Convert uuid to Volume, if necessary. """
        if isinstance(vol, cls):
            return vol
        elif vol is None:
            return None
        else:
            return cls(vol)

detailTypes = ('word', 'phrase', 'line', 'paragraph')
detailNum = {t: n for (n, t) in zip(xrange(len(detailTypes)), detailTypes)}


def display(obj, detail='phrase'):
    """ Friendly string for volume, using sink paths. """
    try:
        return obj.display(detail=detail)
    except AttributeError:
        return str(obj)


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


def printUUID(uuid, detail='word'):
    """ Return friendly abbreviated string for uuid. """
    if not isinstance(detail, int):
        detail = detailNum[detail]

    if detail > detailNum['word']:
        return uuid

    if uuid is None:
        return None

    return "%s...%s" % (uuid[:4], uuid[-4:])


def skipDryRun(logger, dryRun, level=logging.DEBUG):
    """ Print or log command about to be run.

    Return True if should be skipped. """
    # This is an undocumented "feature"
    # logging.log() require a numeric level
    # logging.getLevelName() also maps names to numbers
    if not isinstance(level, int):
        level = logging.getLevelName(level)
    return _skipRun if dryRun else functools.partial(logger.log, level)


def _skipRun(format, *args):
    print("WOULD: " + format % args)
    return True
