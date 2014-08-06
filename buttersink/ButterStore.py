""" Manage read-only volumes in local btrfs file system.

Copyright (c) 2014 Ames Cornish.  All rights reserved.  Licensed under GPLv3.

"""

from __future__ import division

import btrfs
import Butter
import Store

import datetime
import logging
import math
import os
import os.path

logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')
theMinimumChangeRate = .00001


class _Writer:

    """ Context Manager to write a snapshot. """

    def __init__(self, stream, path):
        self.stream = stream
        self.path = path

    def __enter__(self):
        return self.stream.__enter__()

    def __exit__(self, exceptionType, exception, trace):
        self.stream.__exit__(exceptionType, exception, trace)

        if exception is None:
            return

        if not os.path.exists(self.path):
            return

        # This tries to mark partial (failed) transfers.

        partial = self.path + ".part"

        if os.path.exists(partial):
            partial = self.path + "_" + datetime.datetime.now().isoformat() + ".part"

        os.rename(self.path, partial)


class ButterStore(Store.Store):

    """ A local btrfs synchronization source or sink. """

    def __init__(self, host, path, isDest):
        """ Initialize.

        host is ignored.
        path is the file system location of the read-only subvolumes.

        """
        # Don't lose a trailing slash -- it's significant
        path = os.path.abspath(path) + ("/" if path.endswith("/") else "")

        super(ButterStore, self).__init__(path, isDest)

        if not os.path.isdir(self.userPath):
            raise Exception("'%s' is not an existing directory" % (self.userPath))

        self.isDest = isDest

        self.butter = Butter.Butter()  # subprocess command-line interface
        self.btrfs = btrfs.FileSystem(self.userPath)     # ioctl interface

        self.butterVolumes = {}   # Dict of {uuid: <btrfs.Volume>}
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

                # vol = Store.Volume(uuid, bv.totalSize, bv.exclusiveSize, bv.gen)
                for path in bv.linuxPaths:
                    path = self._relativePath(path)

                    if path is None:
                        continue

                    logger.debug("%s %s", vol, path)
                    self.paths[vol].append(path)

                if vol not in self.paths:
                    continue

                if vol.uuid in self.butterVolumes:
                    logger.warn(
                        "Duplicate effective uuid %s in '%s' and '%s'",
                        vol.uuid, path, self.butterVolumes[vol.uuid].fullPath
                    )

                self.butterVolumes[vol.uuid] = bv

    def _fileSystemSync(self):
        with self.btrfs as mount:
            mount.SYNC()

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

            yield Store.Diff(self, toVol, fromVol, estimatedSize, True)

    def hasEdge(self, diff):
        """ True if Store already contains this edge. """
        return diff.toUUID in self.butterVolumes and diff.fromUUID in self.butterVolumes

    def receive(self, diff, paths, dryrun=False):
        """ Return Context Manager for a file-like (stream) object to store a diff. """
        if not dryrun:
            self._fileSystemSync()

        path = self.selectReceivePath(paths)

        stream = self.butter.receive(os.path.dirname(path), dryrun)

        return _Writer(stream, path) if stream is not None else None

    def receiveVolumeInfo(self, paths, dryrun=False):
        """ Return Context Manager for a file-like (stream) object to store volume info. """
        path = self.selectReceivePath(paths)
        path = path + ".bs"

        if Store.skipDryRun(logger, dryrun)("receive to %s", path):
            return None

        return open(path, "w")

    def _estimateSize(self, toBVol, fromBVol, changeRate):
        fromGen = fromBVol.current_gen
        genDiff = abs(toBVol.current_gen - fromGen)

        estimatedSize = max(0, toBVol.totalSize - fromBVol.totalSize)
        estimatedSize += toBVol.totalSize * (1 - math.exp(-changeRate * genDiff))
        estimatedSize = max(toBVol.exclusiveSize, estimatedSize)

        return estimatedSize

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
            logger.debug("Using minimum change rate.")
            rate = theMinimumChangeRate

        logger.debug("Change rate: %f", rate)

        return rate

    def send(self, diff, streamContext, progress=True, dryrun=False):
        """ Write the diff (toVol from fromVol) to the stream context manager. """
        if not dryrun:
            self._fileSystemSync()

        self.butter.send(
            self.getSendPath(diff.toVol),
            self.getSendPath(diff.fromVol),
            streamContext,
            progress,
            dryrun
        )
