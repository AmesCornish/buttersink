""" Abstract and component classes for sources and sinks.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

from util import humanize

import abc
import collections
import functools
import hashlib
import io
import logging
import os.path

theInfoExtension = ".bs"

logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')


class Store(object):

    """ Abstract class for storage back-end.

    Diffs should be indexed by "from" volume.
    Paths should be indexed by volume.

    """

    __metaclass__ = abc.ABCMeta

    # Public methods

    def __init__(self, host, userPath, mode, dryrun):
        """ Initialize. """
        self.host = host

        # { vol: [path] }
        # The order of the paths is important to work around btrfs bugs.
        # The first path is usually the root-volume mounted path,
        # which is required by btrfs 3.14.2.
        self.paths = collections.defaultdict((lambda: []))

        if not userPath.endswith("/"):
            self.userVolume = os.path.basename(userPath)
            userPath = os.path.dirname(userPath)
        else:
            self.userVolume = None

        # This will not end with a "/"
        userPath = os.path.normpath(userPath)

        assert userPath.startswith("/"), userPath
        self.userPath = userPath

        logger.debug("%s('%s')", self.__class__.__name__, userPath)

        self.mode = mode
        self.dryrun = dryrun

        self.ignoreExtraVolumes = False
        self.isRemote = False

        # False - Never show progress
        # True - Always show progress
        # None - Show progress for one-sided actions (e.g. measuring)
        self.showProgress = None

    def __enter__(self):
        """ So we can use a 'with' statement. """
        self._open()
        self._fillVolumesAndPaths(self.paths)
        return self

    def __exit__(self, exceptionType, exceptionValue, traceback):
        """ Clean up after 'with' statement. """
        self._close()
        self.paths = None
        return False  # Don't supress exception

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
                if path == '.':
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

    # Private and subclass methods

    def _fullPath(self, path):
        if path.startswith("/"):
            return path
        if path == ".":
            return self.userPath
        return os.path.normpath(os.path.join(self.userPath, path))

    def _relativePath(self, fullPath):
        """ Return fullPath relative to Store directory.

        Return fullPath if fullPath is not inside directory.

        Return None if fullPath is outside our scope.
        """
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

    def __str__(self):
        """ English description of self.

        Subclasses should just define __unicode__.
        """
        return unicode(self).encode('utf-8')

    # Overridables

    def _open(self):
        """ Open. """
        pass

    def _close(self):
        """ Clean up. """
        pass

    # Abstract methods

    @abc.abstractmethod
    def _fillVolumesAndPaths(self, paths):
        """ Fill in paths.

        :arg paths: = { Store.Volume: ["linux path",]}
        """
        raise NotImplementedError

    # Abstract methods

    @abc.abstractmethod
    def getEdges(self, fromVol):
        """ Return the edges available from fromVol. """
        raise NotImplementedError

    @abc.abstractmethod
    def measureSize(self, diff, chunkSize):
        """ Spend some time to get an accurate size. """
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
    def send(self, diff):
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


def transfer(sendContext, receiveContext, chunkSize):
    """ Transfer (large) data from sender to receiver. """
    try:
        chunkSize = receiveContext.chunkSize
    except AttributeError:
        pass

    if sendContext is not None and receiveContext is not None:
        with receiveContext as writer:
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


class Diff:

    """ Represents a btrfs send diff that creates toVol from fromVol. """

    def __init__(self, sink, toVol, fromVol, size=None, sizeIsEstimated=False):
        """ Initialize. """
        self.sink = sink
        self.toVol = Volume.make(toVol)
        self.fromVol = Volume.make(fromVol)
        self.setSize(size, sizeIsEstimated)

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
    def toGen(self):
        """ 'to' volume's transid. """
        return self.toVol.gen

    @property
    def fromGen(self):
        """ 'from' volume's transid. """
        return self.fromVol.gen if self.fromVol else None

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

    def setSize(self, size, sizeIsEstimated):
        """ Update size. """
        self._size = size
        self._sizeIsEstimated = sizeIsEstimated

        if self.fromVol is not None and size is not None and not sizeIsEstimated:
            Diff.theKnownSizes[self.toUUID][self.fromUUID] = size

    def sendTo(self, dest, chunkSize):
        """ Send this difference to the dest Store. """
        vol = self.toVol
        paths = self.sink.getPaths(vol)

        if self.sink == dest:
            logger.info("Keep: %s", self)
            self.sink.keep(self)
        else:
            # Log, but don't skip yet, so we can log more detailed skipped actions later
            skipDryRun(logger, dest.dryrun, 'INFO')("Xfer: %s", self)

            receiveContext = dest.receive(self, paths)

            sendContext = self.sink.send(self)

            # try:
            #     receiveContext.metadata['btrfsVersion'] = self.btrfsVersion
            # except AttributeError:
            #     pass

            transfer(sendContext, receiveContext, chunkSize)

        if vol.hasInfo():
            infoContext = dest.receiveVolumeInfo(paths)

            if infoContext is None:
                # vol.writeInfo(sys.stdout)
                pass
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

    def __init__(self, uuid, gen, size=None, exclusiveSize=None):
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

    def writeInfoLine(self, stream, fromUUID, size):
        """ Write one line of diff information. """
        if size is None or fromUUID is None:
            return
        if not isinstance(size, int):
            logger.warning("Bad size: %s", size)
            return
        stream.write(str("%s\t%s\t%d\n" % (
            self.uuid,
            fromUUID,
            size,
        )))

    def writeInfo(self, stream):
        """ Write information about diffs into a file stream for use later. """
        for (fromUUID, size) in Diff.theKnownSizes[self.uuid].iteritems():
            self.writeInfoLine(stream, fromUUID, size)

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
        try:
            for line in stream:
                (toUUID, fromUUID, size) = line.split()
                try:
                    size = int(size)
                except:
                    logger.warning("Bad size: %s", size)
                    continue
                logger.debug("diff info: %s %s %d", toUUID, fromUUID, size)
                Diff.theKnownSizes[toUUID][fromUUID] = size
        except Exception as error:
            logger.warn("Can't read .bs info file (%s)", error)

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
                humanize(self.size),
                "" if self.exclusiveSize is None else (
                    " %s exclusive" % (humanize(self.exclusiveSize))
                )
            )
        else:
            size = ""

        vol = "%s %s" % (
            _printUUID(self._uuid, detail - 1),
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
            return cls(vol, None)   # FIX: We need a gen!

detailTypes = ('word', 'phrase', 'line', 'paragraph')
detailNum = {t: n for (n, t) in zip(xrange(len(detailTypes)), detailTypes)}


def display(obj, detail='phrase'):
    """ Friendly string for volume, using sink paths. """
    try:
        return obj.display(detail=detail)
    except AttributeError:
        return str(obj)


def _printUUID(uuid, detail='word'):
    """ Return friendly abbreviated string for uuid. """
    if not isinstance(detail, int):
        detail = detailNum[detail]

    if detail > detailNum['word']:
        return uuid

    if uuid is None:
        return None

    return "%s...%s" % (uuid[:4], uuid[-4:])


def skipDryRun(logger, dryRun, level=logging.DEBUG):
    """ Return logging function.

    When logging function called, will return True if action should be skipped.
    Log will indicate if skipped because of dry run.
    """
    # This is an undocumented "feature" of logging module:
    # logging.log() requires a numeric level
    # logging.getLevelName() maps names to numbers
    if not isinstance(level, int):
        level = logging.getLevelName(level)
    return (
        functools.partial(_logDryRun, logger, level) if dryRun
        else functools.partial(logger.log, level)
    )


def _logDryRun(logger, level, format, *args):
    logger.log(level, "WOULD: " + format % args)
    return True
