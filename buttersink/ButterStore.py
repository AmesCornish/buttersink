""" Manage read-only volumes in local btrfs file system.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

from __future__ import division

import btrfs
import Butter
import progress
import Store

import io
import logging
import math
import os
import os.path
import time

logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')
theMinimumChangeRate = .00001


class ButterStore(Store.Store):

    """ A local btrfs synchronization source or sink. """

    def __init__(self, host, path, mode, dryrun):
        """ Initialize.

        host is ignored.
        path is the file system location of the read-only subvolumes.

        """
        # Don't lose a trailing slash -- it's significant
        path = os.path.abspath(path) + ("/" if path.endswith("/") else "")

        super(ButterStore, self).__init__(host, path, mode, dryrun)

        if not os.path.isdir(self.userPath):
            raise Exception("'%s' is not an existing directory" % (self.userPath))

        self.butter = Butter.Butter(dryrun)  # subprocess command-line interface
        self.btrfs = btrfs.FileSystem(self.userPath)     # ioctl interface

        self.butterVolumes = {}   # Dict of {uuid: <btrfs.Volume>}
        self.extraVolumes = {}  # Will hold volumes inside store directory, but no longer in source

    def _btrfsVol2StoreVol(self, bvol):
        if bvol.received_uuid is not None:
            uuid = bvol.received_uuid
            gen = bvol.sent_gen
        else:
            uuid = bvol.uuid
            gen = bvol.current_gen

        if uuid is None:
            return None

        return Store.Volume(uuid, gen, bvol.totalSize, bvol.exclusiveSize)

    def _fillVolumesAndPaths(self, paths):
        """ Fill in paths.

        :arg paths: = { Store.Volume: ["linux path",]}
        """
        with self.btrfs as mount:
            for bv in mount.subvolumes:
                if not bv.readOnly:
                    continue

                vol = self._btrfsVol2StoreVol(bv)
                if vol is None:
                    continue

                path = bv.fullPath

                if path is None:
                    logger.info("Skipping deleted volume %s", bv.uuid)
                    continue

                relPath = None

                for path in bv.linuxPaths:
                    path = self._relativePath(path)

                    if path is None:
                        continue  # path is outside store scope

                    paths[vol].append(path)

                    infoPath = self._fullPath(path + Store.theInfoExtension)
                    if os.path.exists(infoPath):
                        logger.debug("Reading %s", infoPath)
                        with open(infoPath) as info:
                            Store.Volume.readInfo(info)

                    if not path.startswith("/"):
                        relPath = path

                if vol not in paths:
                    continue

                logger.debug("%s", vol.display(sink=self, detail='phrase'))

                if vol.uuid in self.butterVolumes:
                    logger.warn(
                        "Duplicate effective uuid %s in '%s' and '%s'",
                        vol.uuid, path, self.butterVolumes[vol.uuid].fullPath
                    )

                self.butterVolumes[vol.uuid] = bv

                if relPath is not None:
                    # vol is inside Store directory
                    self.extraVolumes[vol] = relPath

    def _fileSystemSync(self):
        with self.btrfs as mount:
            mount.SYNC()
        time.sleep(2)

    def __unicode__(self):
        """ English description of self. """
        return u"btrfs %s" % (self.userPath)

    def __str__(self):
        """ English description of self. """
        return unicode(self).encode('utf-8')

    def getEdges(self, fromVol):
        """ Return the edges available from fromVol. """
        if fromVol is None:
            for toVol in self.paths:
                yield Store.Diff(self, toVol, fromVol, toVol.size)
            return

        if fromVol not in self.paths:
            return

        fromBVol = self.butterVolumes[fromVol.uuid]
        parentUUID = fromBVol.parent_uuid
        butterDir = os.path.dirname(fromBVol.fullPath)

        vols = [vol for vol in self.butterVolumes.values()
                if vol.parent_uuid == parentUUID or
                os.path.dirname(vol.fullPath) == butterDir
                ]

        changeRate = self._calcChangeRate(vols)

        for toBVol in vols:
            if toBVol == fromBVol:
                continue

            # This gives a conservative estimate of the size of the diff

            estimatedSize = self._estimateSize(toBVol, fromBVol, changeRate)

            toVol = self._btrfsVol2StoreVol(toBVol)

            yield Store.Diff(self, toVol, fromVol, estimatedSize, sizeIsEstimated=True)

    def hasEdge(self, diff):
        """ True if Store already contains this edge. """
        return diff.toUUID in self.butterVolumes

    def receive(self, diff, paths):
        """ Return Context Manager for a file-like (stream) object to store a diff. """
        if not self.dryrun:
            self._fileSystemSync()

        path = self.selectReceivePath(paths)

        if os.path.exists(path):
            raise Exception(
                "Path %s exists, can't receive %s" % (path, diff.toUUID)
            )

        return self.butter.receive(path, diff, self.showProgress is True)

    def receiveVolumeInfo(self, paths):
        """ Return Context Manager for a file-like (stream) object to store volume info. """
        path = self.selectReceivePath(paths)
        path = path + Store.theInfoExtension

        if Store.skipDryRun(logger, self.dryrun)("receive info to %s", path):
            return None

        return open(path, "w")

    def _estimateSize(self, toBVol, fromBVol, changeRate):
        fromGen = fromBVol.current_gen
        genDiff = abs(toBVol.current_gen - fromGen)

        estimatedSize = max(0, toBVol.totalSize - fromBVol.totalSize)
        estimatedSize += toBVol.totalSize * (1 - math.exp(-changeRate * genDiff))
        estimatedSize = max(toBVol.exclusiveSize, estimatedSize)

        return estimatedSize

    def measureSize(self, diff, chunkSize):
        """ Spend some time to get an accurate size. """
        self._fileSystemSync()

        sendContext = self.butter.send(
            self.getSendPath(diff.toVol),
            self.getSendPath(diff.fromVol),
            diff,
            showProgress=self.showProgress is not False,
            allowDryRun=False,
        )

        class _Measure(io.RawIOBase):

            def __init__(self, estimatedSize, showProgress):
                self.totalSize = None
                self.progress = progress.DisplayProgress(estimatedSize) if showProgress else None

            def __enter__(self):
                self.totalSize = 0
                if self.progress:
                    self.progress.__enter__()
                return self

            def __exit__(self, exceptionType, exceptionValue, traceback):
                if self.progress:
                    self.progress.__exit__(exceptionType, exceptionValue, traceback)
                return False  # Don't supress exception

            def writable(self):
                return True

            def write(self, bytes):
                self.totalSize += len(bytes)
                if self.progress:
                    self.progress.update(self.totalSize)

        logger.info("Measuring %s", diff)

        measure = _Measure(diff.size, self.showProgress is not False)
        Store.transfer(sendContext, measure, chunkSize)

        diff.setSize(measure.totalSize, False)

        for path in self.getPaths(diff.toVol):
            path = self._fullPath(path) + Store.theInfoExtension

            with open(path, "a") as infoFile:
                diff.toVol.writeInfoLine(infoFile, diff.fromUUID, measure.totalSize)

    def _calcChangeRate(self, bvols):
        total = 0
        diffs = 0
        minGen = bvols[0].current_gen
        maxGen = minGen
        minSize = bvols[0].totalSize
        maxSize = minSize

        for vol in bvols:
            total += vol.totalSize
            diffs += vol.exclusiveSize
            minGen = min(minGen, vol.current_gen)
            maxGen = max(maxGen, vol.current_gen)
            minSize = min(minSize, vol.totalSize)
            maxSize = max(maxSize, vol.totalSize)

        try:
            # exclusiveSize is often useless,
            # because data may be shared with read-write volumes not usable for send operations
            diffs = max(diffs, maxSize - minSize)
            rate = - math.log(1 - diffs / total) * (len(bvols) - 1) / (maxGen - minGen)
            rate /= 10  # Fudge
        except (ZeroDivisionError, ValueError):
            # logger.debug("Using minimum change rate.")
            rate = theMinimumChangeRate

        # logger.debug("Change rate: %f", rate)

        return rate

    def send(self, diff):
        """ Write the diff (toVol from fromVol) to the stream context manager. """
        if not self.dryrun:
            self._fileSystemSync()

        return self.butter.send(
            self.getSendPath(diff.toVol),
            self.getSendPath(diff.fromVol),
            diff,
            self.showProgress is True,
        )

    def keep(self, diff):
        """ Mark this diff (or volume) to be kept in path. """
        self._keepVol(diff.toVol)
        self._keepVol(diff.fromVol)

    def _keepVol(self, vol):
        """ Mark this volume to be kept in path. """
        if vol is None:
            return

        if vol in self.extraVolumes:
            del self.extraVolumes[vol]
            return

        if vol not in self.paths:
            raise Exception("%s not in %s" % (vol, self))

        paths = self.paths[vol]
        newPath = self.selectReceivePath(paths)
        if self._relativePath(newPath) in paths:
            return

        if self._skipDryRun(logger, 'INFO')("Copy %s to %s", vol, newPath):
            return

        self.butterVolumes[vol.uuid].copy(newPath)

    def deleteUnused(self, dryrun=False):
        """ Delete any old snapshots in path, if not kept. """
        for (vol, path) in self.extraVolumes.items():
            if self._skipDryRun(logger, 'INFO', dryrun=dryrun)("Delete subvolume %s", path):
                continue
            self.butterVolumes[vol.uuid].destroy()

    def deletePartials(self, dryrun=False):
        """ Delete any old partial uploads/downloads in path. """
        for (vol, path) in self.extraVolumes.items():
            if not path.endswith(".part"):
                continue
            if self._skipDryRun(logger, 'INFO', dryrun=dryrun)("Delete subvolume %s", path):
                continue
            self.butterVolumes[vol.uuid].destroy()
