""" Manage read-only volumes in local btrfs file system.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

from __future__ import division

import btrfs
import Butter
import Store

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

    def __init__(self, host, path, isDest, dryrun):
        """ Initialize.

        host is ignored.
        path is the file system location of the read-only subvolumes.

        """
        # Don't lose a trailing slash -- it's significant
        path = os.path.abspath(path) + ("/" if path.endswith("/") else "")

        super(ButterStore, self).__init__(path, isDest, dryrun)

        if not os.path.isdir(self.userPath):
            raise Exception("'%s' is not an existing directory" % (self.userPath))

        self.isDest = isDest

        self.butter = Butter.Butter(dryrun)  # subprocess command-line interface
        self.btrfs = btrfs.FileSystem(self.userPath)     # ioctl interface

        self.butterVolumes = {}   # Dict of {uuid: <btrfs.Volume>}
        self.extraVolumes = {}

        self._fillVolumesAndPaths()

    def _btrfsVol2StoreVol(self, bvol):
        uuid = bvol.received_uuid if self.isDest else bvol.uuid
        if uuid is None:
            return None
        return Store.Volume(uuid, bvol.totalSize, bvol.exclusiveSize, bvol.current_gen)

    def _fillVolumesAndPaths(self):
        """ Fill in self.paths. """
        with self.btrfs as mount:
            for bv in mount.subvolumes:
                if not bv.readOnly:
                    continue

                vol = self._btrfsVol2StoreVol(bv)
                if vol is None:
                    continue

                path = bv.fullPath

                if path is None:
                    logger.warn("Skipping deleted volume %s", bv.uuid)
                    continue

                relPath = None

                # vol = Store.Volume(uuid, bv.totalSize, bv.exclusiveSize, bv.gen)
                for path in bv.linuxPaths:
                    path = self._relativePath(path)

                    if path is None:
                        continue

                    self.paths[vol].append(path)

                    infoPath = self._fullPath(path + ".bs")
                    if os.path.exists(infoPath):
                        logger.debug("Reading %s", infoPath)
                        with open(infoPath) as info:
                            Store.Volume.readInfo(info)

                    if not path.startswith("/"):
                        relPath = path

                if vol not in self.paths:
                    continue

                logger.debug("%s", vol.display(sink=self, detail='phrase'))

                if vol.uuid in self.butterVolumes:
                    logger.warn(
                        "Duplicate effective uuid %s in '%s' and '%s'",
                        vol.uuid, path, self.butterVolumes[vol.uuid].fullPath
                    )

                self.butterVolumes[vol.uuid] = bv

                if relPath is not None:
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
        return diff.toUUID in self.butterVolumes and diff.fromUUID in self.butterVolumes

    def receive(self, diff, paths):
        """ Return Context Manager for a file-like (stream) object to store a diff. """
        if not self.dryrun:
            self._fileSystemSync()

        path = self.selectReceivePath(paths)

        return self.butter.receive(path)

    def receiveVolumeInfo(self, paths):
        """ Return Context Manager for a file-like (stream) object to store volume info. """
        path = self.selectReceivePath(paths)
        path = path + ".bs"

        if Store.skipDryRun(logger, self.dryrun)("receive to %s", path):
            return None

        return open(path, "w")

    def _estimateSize(self, toBVol, fromBVol, changeRate):
        fromGen = fromBVol.current_gen
        genDiff = abs(toBVol.current_gen - fromGen)

        estimatedSize = max(0, toBVol.totalSize - fromBVol.totalSize)
        estimatedSize += toBVol.totalSize * (1 - math.exp(-changeRate * genDiff))
        estimatedSize = max(toBVol.exclusiveSize, estimatedSize)

        return 2 * estimatedSize

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

    def send(self, diff, progress=True):
        """ Write the diff (toVol from fromVol) to the stream context manager. """
        if not self.dryrun:
            self._fileSystemSync()

        return self.butter.send(
            self.getSendPath(diff.toVol),
            self.getSendPath(diff.fromVol),
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

        newPath = self.selectReceivePath(self.paths[vol])

        if self._skipDryRun(logger)("Copy %s to %s", vol, newPath):
            return

        self.butterVolumes[vol.uuid].copy(newPath)

    def deleteUnused(self):
        """ Delete any old snapshots in path, if not kept. """
        for (vol, path) in self.extraVolumes.items():
            if self._skipDryRun(logger)("Delete subvolume %s", path):
                continue
            self.butterVolumes[vol.uuid].destroy()

    def deletePartials(self):
        """ Delete any old partial uploads/downloads in path. """
        for (vol, path) in self.extraVolumes.items():
            if not path.endswith(".part"):
                continue
            if self._skipDryRun(logger)("Delete subvolume %s", path):
                continue
            self.butterVolumes[vol.uuid].destroy()
